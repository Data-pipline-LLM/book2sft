# Task 2 local/staging entry point

**DEVELOPMENT INCOMPLETE** — see TASK_2_FINAL_REPORT.md for implemented features
and concrete gaps. Server integration/browser/load validation is not claimed.

One main launch file: **compose.task2.yml**. Collector configuration and process
hardening are included; temporary Compose variants were consolidated. Original
legacy Compose files are retained for compatibility, not used for new jobs.

All local Python uses the existing base_env:

```sh
conda run -n base_env --no-capture-output python -m unittest discover -s tests -v
conda run -n base_env --no-capture-output python -m pytest
```

25 local tests pass; the native infrastructure test is skipped without explicit
test endpoints. pytest is currently absent. Do not broadly upgrade base_env;
requirements-production/workflow files describe container dependencies, not an
instruction to downgrade the shared local environment.

For server staging, review SERVER_SETUP_CHECKLIST.md. Copy .env.staging.example
to private .env.staging, populate secrets/URLs, validate the single Compose file,
run explicit migrations/storage initialization and create the admin interactively.
Do not treat successful static config validation as a running system.

Existing monitoring tools in tools/task2 still run in base_env and use temporary
synthetic records. They do not test PostgreSQL or execute real LLM calls.

The old Book2SFT API is restricted by service token and defaults to drain mode.
Terminal history can be imported with CLI import-legacy, dry-run by default.
New uploads go through the authenticated operations API and durable outbox.
No original server cleaner should be changed during staging setup.
