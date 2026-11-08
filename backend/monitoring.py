"""Persistent, paginated monitoring index for Book2SFT jobs.

The job.json files remain the source of truth. SQLite only stores compact summaries;
startup/periodic reconciliation indexes historical jobs and removes deleted ones.
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
import statistics
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


STAGE_GROUPS = {
    "ocr": "OCR",
    "chunker_submit": "Chunker",
    "chunker_waiting": "Chunker",
    "normalizing": "Chunker",
    "chunking": "Chunker",
    "book_plan": "Chunker",
    "atomizing": "Chunker",
    "planning": "Chunker",
    "validating": "Chunker",
    "chunker_finalizing": "Chunker",
    "chunker_artifacts": "Chunker",
    "sft_prepare": "SFT Generator",
    "sft_generation": "SFT Generator",
    "sft_book_selection": "Book Selection",
    "sft_finalize": "SFT Generator",
    "review_pdf": "Review PDF",
}


def as_float(value: Any) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else 0.0
    except (TypeError, ValueError, OverflowError):
        return 0.0


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    index = (len(values) - 1) * p / 100
    low = math.floor(index)
    high = math.ceil(index)
    return round(values[low] + (values[high] - values[low]) * (index - low), 2)


def duration_from_dates(start: Any, end: Any) -> float | None:
    if not start or not end:
        return None
    try:
        return max(0.0, (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds())
    except (ValueError, TypeError):
        return None


def extract(job: dict[str, Any], *, orphan: bool = False) -> tuple[Any, ...]:
    pipeline = job.get("metrics", {}).get("pipeline", {}) if isinstance(job.get("metrics"), dict) else {}
    chunker = pipeline.get("chunker", {}) if isinstance(pipeline.get("chunker"), dict) else {}
    sft = pipeline.get("sft", {}) if isinstance(pipeline.get("sft"), dict) else {}
    result = job.get("result") if isinstance(job.get("result"), dict) else {}
    error = job.get("error") if isinstance(job.get("error"), dict) else {}
    status = str(job.get("status") or "queued")
    if orphan and status in {"queued", "running"}:
        status = "interrupted"
    timings = job.get("timings") if isinstance(job.get("timings"), dict) else {}
    stages = {}
    for name, item in timings.items():
        if isinstance(item, dict):
            stages[name] = {
                "status": item.get("status", "waiting"),
                "elapsed_sec": round(as_float(item.get("elapsed_sec")), 2),
                "started_at": item.get("started_at"),
                "finished_at": item.get("finished_at"),
            }
    internal = sft.get("timings_sum_sec") if isinstance(sft.get("timings_sum_sec"), dict) else {}
    internal = {str(k): round(as_float(v), 2) for k, v in internal.items() if isinstance(v, (int, float))}
    duration = duration_from_dates(job.get("started_at"), job.get("completed_at"))
    if duration is None and status in {"completed", "failed"}:
        duration = as_float(job.get("total_elapsed_sec")) or None
    return (
        str(job["job_id"]), str(job.get("filename") or ""), str(job.get("book_title") or ""),
        str(job.get("category") or ""), str(job.get("domain") or ""),
        str(job.get("input_type") or "pdf"), status, str(job.get("stage") or "queued"),
        as_float(job.get("progress")), str(job.get("created_at") or ""),
        str(job.get("updated_at") or ""), job.get("started_at"), job.get("completed_at"), duration,
        int(as_float(chunker.get("chunk_count") or sft.get("total_chunks") or result.get("chunk_count"))),
        int(as_float(sft.get("completed_chunks"))), int(as_float(sft.get("failed_chunks"))),
        int(as_float(sft.get("generated_pairs"))),
        int(as_float(sft.get("selected_pairs") or result.get("selected_sft_pairs"))),
        str(error.get("code") or ("PROCESS_RESTARTED" if status == "interrupted" else "")),
        str(error.get("details") or "")[:500],
        json.dumps(stages, ensure_ascii=False, separators=(",", ":")),
        json.dumps(internal, ensure_ascii=False, separators=(",", ":")),
        1,
    )


_COLUMNS = """job_id, filename, book_title, category, domain, input_type, status,
 stage, progress, created_at, updated_at, started_at, completed_at, duration_sec,
 chunk_count, completed_chunks, failed_chunks, generated_pairs, selected_pairs,
 error_code, error_details, timings_json, internal_timings_json, artifacts_present"""


class MonitorIndex:
    def __init__(self, path: Path, jobs_root: Path) -> None:
        self.path = path
        self.jobs_root = jobs_root
        self._cache_lock = threading.Lock()
        self._cached: dict[str, tuple[float, dict[str, Any]]] = {}
        self.syncing = False
        self.last_sync_at: str | None = None
        self.last_sync_error: str | None = None

    def connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""CREATE TABLE IF NOT EXISTS jobs (
              job_id TEXT PRIMARY KEY, filename TEXT, book_title TEXT, category TEXT,
              domain TEXT, input_type TEXT, status TEXT, stage TEXT, progress REAL,
              created_at TEXT, updated_at TEXT, started_at TEXT, completed_at TEXT,
              duration_sec REAL, chunk_count INTEGER, completed_chunks INTEGER,
              failed_chunks INTEGER, generated_pairs INTEGER, selected_pairs INTEGER,
              error_code TEXT, error_details TEXT, timings_json TEXT, internal_timings_json TEXT,
              artifacts_present INTEGER NOT NULL DEFAULT 1
            )""")
            # Schema migration for an existing monitoring index.
            columns = {r[1] for r in conn.execute("PRAGMA table_info(jobs)")}
            if "artifacts_present" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN artifacts_present INTEGER NOT NULL DEFAULT 1")
            conn.execute("CREATE INDEX IF NOT EXISTS jobs_created ON jobs(created_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS jobs_status_created ON jobs(status, created_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS jobs_input_created ON jobs(input_type, created_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS jobs_stage_created ON jobs(stage, created_at DESC)")

    @staticmethod
    def upsert_many(conn: sqlite3.Connection, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        columns = [column.strip() for column in _COLUMNS.replace("\n", " ").split(",")]
        assignments = ",".join(f"{col}=excluded.{col}" for col in columns[1:])
        sql = (f"INSERT INTO jobs ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)}) "
               f"ON CONFLICT(job_id) DO UPDATE SET {assignments}")
        conn.executemany(sql, rows)

    def upsert(self, job: dict[str, Any]) -> None:
        with self.connection() as conn:
            self.upsert_many(conn, [extract(job)])
        with self._cache_lock:
            self._cached.clear()

    def reconcile(self, session_ids: set[str]) -> None:
        """Backfill from job.json and retain compact metadata after cleanup."""
        self.syncing = True
        self.last_sync_error = None
        try:
            with self.connection() as conn:
                existing = {row[0]: (row[1], row[2], row[3]) for row in conn.execute("SELECT job_id, updated_at, status, artifacts_present FROM jobs")}
            seen: set[str] = set()
            batch: list[tuple[Any, ...]] = []
            # A single scandir pass handles 20k job folders without recursive traversal.
            with os.scandir(self.jobs_root) as entries:
                for entry in entries:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    path = Path(entry.path) / "job.json"
                    if not path.is_file():
                        continue
                    try:
                        data = json.loads(path.read_text(encoding="utf-8"))
                        jid = str(data["job_id"])
                        if jid != entry.name:
                            continue
                        seen.add(jid)
                        orphan = jid not in session_ids
                        # Old running/queued jobs cannot be resumed by current Book2SFT workers.
                        expected_status = "interrupted" if orphan and data.get("status") in {"queued", "running"} else data.get("status")
                        if jid in existing and existing[jid] == (data.get("updated_at"), expected_status, 1):
                            continue
                        batch.append(extract(data, orphan=orphan))
                        if len(batch) >= 250:
                            with self.connection() as conn:
                                self.upsert_many(conn, batch)
                            batch.clear()
                    except (OSError, ValueError, KeyError, TypeError):
                        continue  # partially written/invalid jobs are ignored
            with self.connection() as conn:
                self.upsert_many(conn, batch)
                # Retain compact historical records when the external cleaner
                # removes raw PDFs/JSONL and job.json every three days.
                missing = set(existing) - seen
                for jid in missing:
                    if existing[jid][2] == 1:
                        if existing[jid][1] in {"running", "queued"}:
                            conn.execute("UPDATE jobs SET artifacts_present=0, status='interrupted', error_code='PROCESS_INTERRUPTED' WHERE job_id=?", (jid,))
                        else:
                            conn.execute("UPDATE jobs SET artifacts_present=0 WHERE job_id=?", (jid,))
            self.last_sync_at = datetime.now(timezone.utc).isoformat()
            with self._cache_lock:
                self._cached.clear()
        except (OSError, sqlite3.Error) as exc:
            self.last_sync_error = f"{type(exc).__name__}: {exc}"
        finally:
            self.syncing = False

    def list_jobs(self, *, q: str = "", status: str = "", input_type: str = "",
                  stage: str = "", since: str = "", until: str = "",
                  page: int = 1, page_size: int = 25, sort: str = "newest") -> dict[str, Any]:
        filters: list[str] = []
        params: list[Any] = []
        if q.strip():
            filters.append("(job_id LIKE ? OR filename LIKE ? OR book_title LIKE ? OR category LIKE ?)")
            text = "%" + q.strip()[:120].replace("%", "\\%").replace("_", "\\_") + "%"
            # Escape LIKE wildcard patterns by escaping the query, not the indexed data.
            filters[-1] = "(job_id LIKE ? ESCAPE '\\' OR filename LIKE ? ESCAPE '\\' OR book_title LIKE ? ESCAPE '\\' OR category LIKE ? ESCAPE '\\')"
            params.extend([text] * 4)
        if status:
            if status == "active":
                filters.append("status IN ('running','queued')")
            else:
                filters.append("status = ?")
                params.append(status)
        if input_type:
            filters.append("input_type = ?")
            params.append(input_type)
        if stage:
            filters.append("stage = ?")
            params.append(stage)
        if since:
            filters.append("created_at >= ?")
            params.append(since)
        if until:
            filters.append("created_at < ?")
            params.append(until)
        where = (" WHERE " + " AND ".join(filters)) if filters else ""
        order = {
            "newest": "created_at DESC, job_id DESC",
            "oldest": "created_at ASC, job_id ASC",
            "slowest": "duration_sec DESC, created_at DESC",
            "most_failed": "failed_chunks DESC, created_at DESC",
        }.get(sort, "created_at DESC, job_id DESC")
        cols = "job_id,filename,book_title,category,domain,input_type,status,stage,progress,created_at,updated_at,started_at,completed_at,duration_sec,chunk_count,completed_chunks,failed_chunks,generated_pairs,selected_pairs,error_code,error_details,artifacts_present"
        with self.connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM jobs" + where, params).fetchone()[0]
            rows = conn.execute(f"SELECT {cols} FROM jobs{where} ORDER BY {order} LIMIT ? OFFSET ?",
                                [*params, page_size, (page - 1) * page_size]).fetchall()
        return {"items": [dict(row) for row in rows], "total": count, "page": page,
                "page_size": page_size, "pages": math.ceil(count / page_size),
                "indexing": self.syncing, "last_sync_at": self.last_sync_at,
                "last_sync_error": self.last_sync_error}

    def get_summary(self, job_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            return None
        item = dict(row)
        item["timings"] = json.loads(item.pop("timings_json") or "{}")
        item["internal_timings_sum_sec"] = json.loads(item.pop("internal_timings_json") or "{}")
        return item

    def overview(self, days: int = 30) -> dict[str, Any]:
        key = str(days)
        with self._cache_lock:
            old = self._cached.get(key)
            if old and time.monotonic() - old[0] < 20:
                return {**old[1], "indexing": self.syncing}
        from .monitoring_queries import summary
        with self.connection() as conn:
            conn.execute("PRAGMA temp_store=FILE")
            response = summary(conn, days)
        response.update(indexing=self.syncing, last_sync_at=self.last_sync_at,
                        last_sync_error=self.last_sync_error)
        with self._cache_lock:
            self._cached[key] = (time.monotonic(), response)
        return response


def inspect_errors(job_dir: Path, *, limit: int = 15) -> dict[str, Any]:
    path = job_dir / "sft" / "sft_errors.jsonl"
    if not path.is_file():
        return {"available": False, "sampled": 0, "http_statuses": [], "examples": []}
    codes: Counter[str] = Counter()
    messages: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    count = 0
    # Stream instead of loading potentially very large SFT results into memory.
    with path.open(encoding="utf-8-sig") as stream:
        for line in stream:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            result = row.get("result") if isinstance(row.get("result"), dict) else {}
            code = str(result.get("status_code") or "unknown")
            message = str(result.get("exception") or result.get("body") or result.get("message") or "unknown")[:300]
            codes[code] += 1
            messages[message] += 1
            count += 1
            if len(examples) < limit:
                examples.append({"chunk_index": row.get("chunk_index"),
                                 "chunk_id": row.get("chunk_id"), "http_status": code,
                                 "message": message, "elapsed_sec": (result.get("_book2sft") or {}).get("elapsed_sec")})
    # Conservative, actionable diagnostics: do not assume every HTTP 500 is an LLM outage.
    signs = {code for code in codes}
    msg_text = " ".join(messages).lower()
    if "503" in signs or "503 service unavailable" in msg_text or "no server is available" in msg_text:
        diagnosis = {"code": "DOWNSTREAM_SERVICE_UNAVAILABLE", "severity": "critical",
                     "hint": "سرویس پایین‌دستی 503 می‌دهد؛ دسترسی API مدل و ظرفیت Gateway/vLLM را مستقلاً بررسی کنید."}
    elif "429" in signs or "rate limit" in msg_text:
        diagnosis = {"code": "RATE_LIMIT", "severity": "warning",
                     "hint": "احتمال محدودیت نرخ وجود دارد؛ Concurrency و سهمیه LLM را بررسی کنید."}
    elif "422" in signs:
        diagnosis = {"code": "PAYLOAD_REJECTED", "severity": "warning",
                     "hint": "درخواست از نظر ساختار رد شده؛ schema درخواست SFT و سرویس مقصد را بررسی کنید."}
    elif "timeout" in msg_text:
        diagnosis = {"code": "TIMEOUT", "severity": "warning",
                     "hint": "یک یا چند درخواست Timeout شده؛ لاگ زمان پاسخ مدل و شبکه را بررسی کنید."}
    elif "500" in signs:
        diagnosis = {"code": "INTERNAL_SERVER_ERROR", "severity": "warning",
                     "hint": "سرویس SFT خطای 500 داده؛ لاگ همان سرویس را ببینید. صرفاً از روی این پاسخ نمی‌توان علت LLM را قطعی دانست."}
    else:
        diagnosis = {"code": "UNCLASSIFIED", "severity": "info",
                     "hint": "پاسخ‌های پایین‌دستی را با لاگ Orchestrator و Assessment تطبیق دهید."}
    return {"available": True, "diagnosis": diagnosis, "sampled": count,
            "http_statuses": [{"code": k, "count": v} for k, v in codes.most_common()],
            "common_messages": [{"message": k, "count": v} for k, v in messages.most_common(5)],
            "examples": examples}
