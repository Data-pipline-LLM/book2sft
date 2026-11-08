/* Book2SFT Operations Center: zero-dependency, server-paginated monitoring. */
(() => {
  'use strict';
  const el = id => document.getElementById(id);
  const safe = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const number = n => new Intl.NumberFormat('fa-IR').format(Number(n) || 0);
  const sec = value => {
    if (value === undefined || value === null) return '—';
    const n = Number(value);
    if (!Number.isFinite(n)) return '—';
    if (n < 60) return `${n.toFixed(1)}s`;
    if (n < 3600) return `${Math.floor(n / 60)}m ${Math.round(n % 60)}s`;
    return `${Math.floor(n / 3600)}h ${Math.floor((n % 3600) / 60)}m`;
  };
  const date = value => value ? new Intl.DateTimeFormat('fa-IR', {dateStyle:'medium',timeStyle:'short'}).format(new Date(value)) : '—';
  const stages = typeof STAGE_LABELS !== 'undefined' ? STAGE_LABELS : {};
  const STATUS = {queued:'در صف', running:'در حال اجرا', completed:'تکمیل‌شده', failed:'ناموفق', interrupted:'متوقف‌شده'};
  const colors = {completed:'#14b8a6', running:'#4976ef', queued:'#e5aa4a', failed:'#ed6e77', interrupted:'#94a3b8'};
  const groupName = {'OCR':'OCR', 'Chunker':'Chunker', 'SFT Generator':'SFT Generator', 'Book Selection':'Book Selection','Review PDF':'Review PDF'};
  let page = 1, pages = 1, activeView = 'upload', selectedId = null;
  let listRequest = 0, dashboardBusy = false, listBusy = false, listPending = false;
  let lastOverview = 0;
  let searchTimer = null;

  async function json(url) {
    const response = await fetch(url, {cache:'no-store'});
    if (!response.ok) throw new Error(`${response.status}: ${await response.text().then(s=>s.slice(0,200))}`);
    return response.json();
  }
  function empty(text) { return `<div class="chart-empty">${safe(text)}</div>`; }
  function alertText(message) {
    el('monitorAlert').textContent = message;
    el('monitorAlert').classList.toggle('hidden', !message);
  }
  function go(view) {
    activeView = view;
    el('uploadView').classList.toggle('hidden', view !== 'upload');
    el('monitorView').classList.toggle('hidden', view !== 'monitor');
    document.querySelectorAll('.nav-tab').forEach(tab => {
      const current = tab.dataset.view === view;
      tab.classList.toggle('active', current);
      tab.setAttribute('aria-selected', String(current));
    });
    if (view === 'monitor') refresh(true);
    window.scrollTo({top:0, behavior:'auto'});
  }
  document.querySelectorAll('[data-view]').forEach(tab => tab.addEventListener('click', () => { location.hash = tab.dataset.view; }));
  window.addEventListener('hashchange', () => go(location.hash === '#monitor' ? 'monitor' : 'upload'));

  function setKpis(v) {
    el('kpiTotal').textContent = number(v.total_jobs);
    el('kpiActive').textContent = number((v.statuses?.running || 0) + (v.statuses?.queued || 0));
    el('kpiSuccess').textContent = `${Number(v.job_success_rate || 0).toFixed(1)}%`;
    el('kpiFailed').textContent = number(v.failed_chunks);
    el('kpiFailedRate').textContent = `${v.chunk_failure_rate}% از کل چانک‌ها`;
    el('kpiDuration').textContent = sec(v.duration_sec?.p50);
    el('kpiP95').textContent = `P95: ${sec(v.duration_sec?.p95)}`;
    el('kpiPairs').textContent = number(v.selected_pairs);
    el('kpiGenerated').textContent = `${number(v.generated_pairs)} جفت تولید اولیه`;
    if (v.indexing) alertText('فهرست تاریخچه در حال ایندکس‌شدن است؛ آمار پس از اتمام دقیق‌تر می‌شود.');
    else if (v.last_sync_error) alertText(`خطا در به‌روزرسانی تاریخچه: ${v.last_sync_error}`);
    else alertText('');
  }

  function dailyChart(days) {
    const holder = el('dailyChart');
    const all = (days || []).slice(-32);
    if (!all.length) { holder.innerHTML = empty('هنوز داده‌ای برای نمودار روزانه وجود ندارد.'); return; }
    const w=700,h=224,l=35,r=10,t=20,b=48,base=h-b, plot=h-b-t;
    const ceiling=Math.max(1,...all.map(d=>d.total));
    const step=(w-l-r)/all.length, bar=Math.min(24,step*.63);
    const grid=[0,.25,.5,.75,1].map(n=>`<line x1="${l}" y1="${base-plot*n}" x2="${w-r}" y2="${base-plot*n}" stroke="#e9edf4"/><text x="${l-7}" y="${base-plot*n+4}" font-size="11" text-anchor="end" fill="#8c99ab">${Math.round(ceiling*n)}</text>`).join('');
    const rectangles=all.map((row,i)=>{
      const x=l+step*i+(step-bar)/2;
      const success=Math.max(0,Number(row.completed||0)), failed=Math.max(0,Number(row.failed||0)), other=Math.max(0,row.total-success-failed);
      const scale=plot/ceiling;
      const pieces=[[success,'#14b8a6'],[failed,'#ed6e77'],[other,'#91a7e9']];
      let y=base;
      const bars=pieces.map(([n,color])=> {const height=n*scale;y-=height;return height?`<rect x="${x}" y="${y}" width="${bar}" height="${height}" rx="2" fill="${color}"/>`:'';}).join('');
      const label=(i===0||i===all.length-1||i%Math.max(1,Math.ceil(all.length/7))===0)?`<text x="${x+bar/2}" y="${base+19}" font-size="11" text-anchor="middle" fill="#64748b">${safe(row.date.slice(5))}</text>`:'';
      return `<g><title>${safe(row.date)}: ${number(row.total)} Job</title>${bars}${label}</g>`;
    }).join('');
    holder.innerHTML=`<svg viewBox="0 0 ${w} ${h}" role="img" aria-label="تعداد Jobها به تفکیک روز" class="full-chart" preserveAspectRatio="xMidYMid meet" direction="ltr">${grid}${rectangles}</svg><div class="chart-legend"><span><i style="background:#14b8a6"></i>تکمیل‌شده</span><span><i style="background:#ed6e77"></i>ناموفق</span><span><i style="background:#91a7e9"></i>سایر</span></div>`;
  }
  function statusChart(statuses) {
    const holder=el('statusChart'), rows=Object.entries(statuses||{}).filter(([,v])=>Number(v)>0), total=rows.reduce((v,[,n])=>v+n,0);
    if (!total) {holder.innerHTML=empty('هنوز Jobی ثبت نشده است.');return;}
    let offset=0;
    const circles=rows.map(([status,n])=>{
      const pct=n/total*100;
      const result=`<circle cx="90" cy="90" r="67" fill="none" stroke="${colors[status]||'#a0a9bd'}" stroke-width="19" stroke-dasharray="${pct} ${100-pct}" stroke-dashoffset="${-offset}" pathLength="100" transform="rotate(-90 90 90)"/>`;
      offset+=pct;return result;
    }).join('');
    holder.innerHTML=`<div class="donut-layout"><svg viewBox="0 0 180 180" role="img" aria-label="توزیع وضعیت Jobها">${circles}<text x="90" y="85" text-anchor="middle" class="donut-number">${total}</text><text x="90" y="106" text-anchor="middle" class="donut-sub">Job</text></svg><div class="donut-legend">${rows.map(([k,n])=>`<div><span><i style="background:${colors[k]||'#a0a9bd'}"></i>${safe(STATUS[k]||k)}</span><strong>${number(n)}</strong></div>`).join('')}</div></div>`;
  }
  function serviceChart(services) {
    const holder=el('serviceChart');
    if (!services?.length) {holder.innerHTML=empty('پس از پایان مراحل، زمان اجرای سرویس‌ها نمایش داده می‌شود.');return;}
    const max=Math.max(1,...services.map(s=>Number(s.p95_sec||s.avg_sec||0)));
    holder.innerHTML=`<div class="latency-rows">${services.map(row=>{
      const avg=Number(row.avg_sec||0),p95=Number(row.p95_sec||0);
      return `<div class="latency-item"><div class="latency-label"><strong>${safe(groupName[row.name]||row.name)}</strong><span>${number(row.jobs)} Job · ${number(row.states?.failed||0)} خطا · میانگین ${sec(avg)} / P95 ${sec(p95)}</span></div><div class="latency-track" dir="ltr"><div class="latency-p95" style="width:${Math.max(1,p95/max*100)}%"></div><div class="latency-mean" style="width:${Math.max(1,avg/max*100)}%"></div></div></div>`;
    }).join('')}</div><div class="chart-legend"><span><i style="background:#4976ef"></i>میانگین</span><span><i style="background:#c8d6fe"></i>P95</span></div>`;
  }
  function errorChart(errors) {
    const holder=el('errorChart');
    if (!errors?.length) {holder.innerHTML=empty('خطای سطح Job در این بازه ثبت نشده است.');return;}
    const peak=Math.max(...errors.map(x=>x.count),1);
    holder.innerHTML=`<div class="error-chart">${errors.slice(0,6).map((item,i)=>`<div><div class="error-chart-label"><strong title="${safe(item.code)}">${safe(item.code)}</strong><span>${number(item.count)}</span></div><div class="error-chart-track"><div style="width:${item.count/peak*100}%;background:${i?'#9bafd8':'#ed6e77'}"></div></div></div>`).join('')}</div>`;
  }
  async function loadHealth() {
    const target=el('serviceHealth');
    try {
      const response=await fetch('/ready',{cache:'no-store'});
      const data=await response.json();
      const labels={chunker:'Chunker / OCR',sft_orchestrator:'SFT Orchestrator',sft_generator:'SFT Generator'};
      target.innerHTML=Object.entries(labels).map(([name,title])=>{
        const health=data.services?.[name]||{};
        return `<div class="service-health"><span class="${health.ok?'ok':'down'} indicator"></span><div><strong>${title}</strong><small>${health.ok?'API در دسترس':`API در دسترس نیست · ${safe(health.status_code||health.error||'Unknown')}`}</small></div></div>`;
      }).join('');
    } catch(error) {target.innerHTML=empty(`بررسی وضعیت سرویس‌ها ناموفق بود: ${error.message}`);}
  }
  function internalChart(timings) {
    const holder=el('internalChart');
    const rows=Object.entries(timings||{}).filter(([,v])=>Number(v)>0).sort((a,b)=>b[1]-a[1]).slice(0,9);
    if(!rows.length){holder.innerHTML=empty('هنوز داده‌ای از زمان زیرسرویس‌ها ثبت نشده است.');return;}
    const max=Math.max(...rows.map(([,n])=>n),1);
    holder.innerHTML=`<div class="internal-chart">${rows.map(([name,seconds])=>`
      <div class="internal-chart-item"><span class="internal-chart-label" title="${safe(name)}">${safe(typeof timingName==='function'?timingName(name):name)}</span><div class="internal-chart-track" dir="ltr"><div style="width:${100*seconds/max}%"></div></div><strong>${sec(seconds)}</strong></div>`).join('')}</div>`;
  }
  async function loadOverview(force=false) {
    if (dashboardBusy || (!force && Date.now()-lastOverview<30000)) return;
    dashboardBusy=true;
    try {
      const data=await json(`/v1/monitoring/overview?days=${el('monitorDays').value}`);
      setKpis(data);dailyChart(data.daily);statusChart(data.statuses);serviceChart(data.services);errorChart(data.errors);internalChart(data.internal_timings_sum_sec);
      el('monitorUpdated').textContent=`آخرین بار: ${new Date().toLocaleTimeString('fa-IR')}`;
      lastOverview=Date.now();
    } catch(error) {alertText('دریافت شاخص‌ها ناموفق بود: '+error.message);}
    finally {dashboardBusy=false;}
  }
  function historyQuery() {
    const fields={q:el('historySearch').value.trim(),status:el('historyStatus').value,
      input_type:el('historyInput').value,stage:el('historyStage').value,
      since:el('historySince').value ? el('historySince').value+'T00:00:00' : '',
      until:el('historyUntil').value ? el('historyUntil').value+'T23:59:59.999999' : '',
      sort:el('historySort').value,
      page, page_size:el('historyPageSize').value};
    return new URLSearchParams(fields).toString();
  }
  async function loadJobs() {
    if (listBusy) { listPending=true; return; }
    const current=++listRequest;
    listBusy=true;
    try {
      const data=await json(`/v1/monitoring/jobs?${historyQuery()}`);
      if (current!==listRequest) return;
      pages=Math.max(1,data.pages);
      el('historyCount').textContent=number(data.total);
      el('historyRows').innerHTML=(data.items||[]).map(row=>{
        const status=row.status||'unknown',pct=Math.max(0,Math.min(100,Number(row.progress||0)));
        return `<tr data-job-id="${safe(row.job_id)}" tabindex="0" role="button" aria-label="جزئیات ${safe(row.book_title||row.filename)}">
        <td><strong class="book-name">${safe(row.book_title||row.filename||'بدون عنوان')}${row.artifacts_present===0?' <span class="archive-flag" title="فایل‌های اصلی پاک‌سازی شده‌اند">آرشیوی</span>':''}</strong><small class="mono job-short" title="${safe(row.job_id)}">${safe(row.job_id.slice(0,12))}… · ${row.input_type==='ocr_json'?'OCR JSON':'PDF'}</small></td>
        <td><span class="status-tag status-${safe(status)}"><i></i>${safe(STATUS[status]||status)}</span></td>
        <td class="stage-cell">${safe(stages[row.stage]||row.stage||'—')}</td>
        <td><div class="row-progress" dir="ltr"><div style="width:${pct}%"></div></div><small>${pct.toFixed(1)}%</small></td>
        <td><strong>${number(row.chunk_count)}</strong><small class="${row.failed_chunks?'red-text':'muted'}">${number(row.failed_chunks)} ناموفق</small></td>
        <td dir="ltr" class="mono">${sec(row.duration_sec ?? (['running','queued'].includes(status)?null:0))}</td>
        <td class="mono time-cell">${date(row.created_at)}</td>
        <td class="open-arrow">←</td></tr>`;
      }).join('')||'<tr><td colspan="8" class="empty-state">Job مطابق فیلترهای انتخاب‌شده پیدا نشد.</td></tr>';
      const from=data.total?(page-1)*data.page_size+1:0, to=Math.min(data.total,page*data.page_size);
      el('historyRange').textContent=`نمایش ${number(from)} تا ${number(to)} از ${number(data.total)} Job`;
      el('historyPage').textContent=`${number(page)} / ${number(pages)}`;
      el('historyPrev').disabled=page<=1;
      el('historyNext').disabled=page>=pages;
    } catch(error) {el('historyRows').innerHTML=`<tr><td colspan="8" class="empty-state">خطا در دریافت تاریخچه: ${safe(error.message)}</td></tr>`;}
    finally {listBusy=false;if(listPending){listPending=false;loadJobs();}}
  }
  async function refresh(force=false) {
    if(activeView!=='monitor')return;
    await Promise.all([loadOverview(force),loadJobs(),loadHealth()]);
    if(selectedId) openJob(selectedId,true);
  }
  function bindSelect(id) {el(id).addEventListener('change',()=>{page=1;loadJobs();});}
  for(const id of ['historyStatus','historyInput','historyStage','historySince','historyUntil','historySort','historyPageSize']) bindSelect(id);
  el('historySearch').addEventListener('input',()=>{
    clearTimeout(searchTimer);
    searchTimer=setTimeout(()=>{page=1;loadJobs();},330);
  });
  el('monitorDays').addEventListener('change',()=>{lastOverview=0;loadOverview(true);});
  el('monitorRefresh').addEventListener('click',()=>{lastOverview=0;refresh(true);});
  el('historyPrev').addEventListener('click',()=>{if(page>1){page--;loadJobs();}});
  el('historyNext').addEventListener('click',()=>{if(page<pages){page++;loadJobs();}});
  el('historyRows').addEventListener('click',event=>{const row=event.target.closest('tr[data-job-id]');if(row)openJob(row.dataset.jobId);});
  el('historyRows').addEventListener('keydown',event=>{if(event.key==='Enter'){const row=event.target.closest('tr[data-job-id]');if(row)openJob(row.dataset.jobId);}});

  function phaseTimeline(timings) {
    const rows=Object.entries(timings||{}).filter(([,t])=>t&&t.status!=='waiting');
    if(!rows.length)return empty('زمان‌بندی مراحل هنوز ثبت نشده است.');
    const total=Math.max(1, rows.reduce((sum,[,t])=>sum+Number(t.elapsed_sec||0),0));
    return `<div class="detail-timing">${rows.map(([name,t])=>{
      const elapsed=Number(t.elapsed_sec||0),skip=t.status==='skipped';
      return `<div class="detail-timing-row"><div class="detail-step"><span class="phase-dot phase-${safe(t.status||'waiting')}"></span><strong>${safe(stages[name]||name)}</strong><span class="step-status">${safe(t.status)}</span></div><div class="detail-duration"><div class="detail-track"><div style="width:${skip?0:Math.max(1,100*elapsed/total)}%"></div></div><span>${skip?'SKIP':sec(elapsed)}</span></div></div>`;
    }).join('')}</div>`;
  }
  function detailMetrics(job,summary) {
    const sft=job.metrics?.pipeline?.sft||{},chunker=job.metrics?.pipeline?.chunker||{};
    const internal=summary?.internal_timings_sum_sec||sft.timings_sum_sec||{};
    const internalRows=Object.entries(internal).filter(([,n])=>Number(n)>0).sort((a,b)=>b[1]-a[1]).slice(0,18);
    return `<div class="drawer-metrics"><div><span>چانک‌ها</span><strong>${number(sft.total_chunks||chunker.chunk_count||0)}</strong></div><div><span>چانک‌های پردازش‌شده</span><strong>${number(sft.completed_chunks||0)}</strong></div><div><span>چانک ناموفق</span><strong class="red-text">${number(sft.failed_chunks||0)}</strong></div><div><span>جفت نهایی</span><strong>${number(sft.selected_pairs||0)}</strong></div></div>
    <h3>زمان زیرسرویس‌های SFT <small>(جمع زمان‌ها روی چانک‌ها؛ قابل جمع با زمان دیواری نیست)</small></h3>
    ${internalRows.length?`<div class="internal-list">${internalRows.map(([name,t])=>`<div><span title="${safe(name)}">${safe(typeof timingName==='function'?timingName(name):name)}</span><strong>${sec(t)}</strong></div>`).join('')}</div>`:empty('هنوز تله‌متری زیرسرویس‌ها ذخیره نشده است.')}`;
  }
  function detailDownloads(downloads) {
    const labels={'chunks.jsonl':'Chunks JSONL','ocr.json':'OCR JSON','chunker.log':'Chunker log',
      'sft_pairs.jsonl':'Final SFT JSONL','sft_axolotl.jsonl':'Axolotl JSONL','sft_errors.jsonl':'SFT errors',
      'sft_raw_results.jsonl':'SFT raw results','sft_metrics.json':'SFT metrics',
      'review_selected.pdf':'Review PDF','review_all_evaluated.pdf':'Audit PDF',
      'all_evaluated_pairs.jsonl':'All evaluated pairs'};
    const keys=Object.keys(downloads||{}).filter(key=>labels[key]);
    return keys.length?`<div class="drawer-downloads">${keys.map(key=>`<a class="small-download" href="${safe(downloads[key])}" download>${safe(labels[key])} ↙</a>`).join('')}</div>`:empty('هنوز فایلی برای دریافت وجود ندارد.');
  }
  async function openJob(jobId,quiet=false) {
    selectedId=jobId;
    if(!quiet){el('jobDrawer').classList.remove('hidden');document.body.classList.add('drawer-open');el('drawerBody').innerHTML=empty('در حال دریافت جزئیات Job…');}
    try {
      const [data,errors]=await Promise.all([
        json(`/v1/monitoring/jobs/${encodeURIComponent(jobId)}`),
        json(`/v1/monitoring/jobs/${encodeURIComponent(jobId)}/errors?limit=12`).catch(()=>({available:false,examples:[]}))
      ]);
      if(selectedId!==jobId)return;
      const job=data.job,summary=data.summary||{},status=summary.status||job.status;
      el('drawerTitle').textContent=job.book_title||job.filename||'Job';
      el('drawerId').textContent=jobId;
      const running=['running','queued'].includes(status), duration=running?liveElapsed(job.started_at,job.completed_at,job.total_elapsed_sec):job.total_elapsed_sec;
      const messages=(errors.common_messages||[]).slice(0,5);
      const errorHtml=errors.available?
        `<div class="error-summary"><h3>SFT Request Failures <span class="small-count">${number(errors.sampled)}</span></h3>
        ${errors.diagnosis?`<div class="diagnosis"><strong>${safe(errors.diagnosis.code)}</strong><span>${safe(errors.diagnosis.hint)}</span></div>`:''}
        <div class="error-codes">${(errors.http_statuses||[]).map(c=>`<span>${safe(c.code)} × ${number(c.count)}</span>`).join('')}</div>
        ${messages.map(m=>`<div class="issue-item"><span class="issue-count">${number(m.count)}×</span><code dir="ltr">${safe(m.message)}</code></div>`).join('')}
        ${errors.examples?.length?`<details><summary>نمونه خطاها بر اساس چانک</summary>${errors.examples.map(e=>`<div class="error-example"><strong>${safe(e.chunk_id||e.chunk_index||'?')}</strong><span>${safe(e.http_status)} · ${sec(e.elapsed_sec)}</span><code dir="ltr">${safe(e.message)}</code></div>`).join('')}</details>`:''}</div>`:
        empty('فایل SFT Errors برای این Job موجود نیست.');
      el('drawerBody').innerHTML=`<div class="drawer-top-status"><span class="status-tag status-${safe(status)}"><i></i>${safe(STATUS[status]||status)}</span><span>${date(job.created_at)}</span></div>
        <p class="muted detail-message">${safe(job.message||'—')}</p>
        ${status==='interrupted'?'<p class="monitor-note">این Job پیش از راه‌اندازی فعلی ذخیره شده و کارگر فعالی برای ادامه آن وجود ندارد.</p>':''}
        ${data.artifacts_present===false?'<p class="monitor-note">فایل‌های Job پاک‌سازی شده‌اند؛ خلاصه، زمان‌ها و آمار در تاریخچه باقی مانده‌اند. دانلود فایل‌ها و جزئیات خام خطا دیگر در دسترس نیست.</p>':''}
        <div class="drawer-progress-label"><strong>پیشرفت کل Pipeline</strong><strong>${Number(job.progress||0).toFixed(1)}%</strong></div><div class="bar"><div class="bar-fill" style="width:${Math.max(0,Math.min(100,Number(job.progress||0)))}%"></div></div>
        <div class="drawer-facts"><div><span>کل زمان</span><strong>${sec(duration)}</strong></div><div><span>ETA</span><strong>${running?sec(job.eta_sec):'—'}</strong></div><div><span>نوع فایل</span><strong>${job.input_type==='ocr_json'?'OCR JSON':'PDF'}</strong></div><div><span>مرحله فعلی</span><strong>${safe(stages[job.stage]||job.stage)}</strong></div></div>
        ${job.error?`<div class="monitor-error"><strong>${safe(job.error.code||'Pipeline Error')}</strong><p>${safe(job.error.details||job.error.message||'')}</p></div>`:''}
        <div class="detail-section"><h3>نقشه زمان‌بندی مراحل</h3>${phaseTimeline(job.timings||summary.timings)}</div>
        <div class="detail-section">${detailMetrics(job,summary)}</div>
        <div class="detail-section">${errorHtml}</div>
        <div class="detail-section"><h3>فایل‌های خروجی و عیب‌یابی</h3>${detailDownloads(data.downloads)}</div>`;
    } catch(error) {if(selectedId===jobId)el('drawerBody').innerHTML=empty('دریافت اطلاعات Job ناموفق بود: '+error.message);}
  }
  function closeDrawer() {selectedId=null;el('jobDrawer').classList.add('hidden');document.body.classList.remove('drawer-open');}
  el('drawerClose').addEventListener('click',closeDrawer);
  el('drawerBackdrop').addEventListener('click',closeDrawer);
  document.addEventListener('keydown',event=>{if(event.key==='Escape')closeDrawer();});
  window.setInterval(()=>{
    if(activeView==='monitor'&&!document.hidden)refresh(false);
  },15000);
  go(location.hash==='#monitor'?'monitor':'upload');
})();