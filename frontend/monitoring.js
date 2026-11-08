/* Book2SFT — existing dashboard, now backed by the durable operations APIs.
 * Pure browser JS; all history pages and analytical series are server-bounded.
 * No login, user-management UI or client-held service credentials.
 */
(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const fmt = new Intl.NumberFormat('fa-IR', {maximumFractionDigits: 1});
  const escapeHTML = v => String(v ?? '—').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const num = v => v == null ? '—' : fmt.format(Number(v) || 0);
  const secs = v => v == null || !Number.isFinite(Number(v)) ? '—' :
    Number(v) < 60 ? `${Number(v).toFixed(1)} ثانیه` : Number(v) < 3600 ? `${(Number(v)/60).toFixed(1)} دقیقه` : `${(Number(v)/3600).toFixed(1)} ساعت`;
  const when = v => v ? new Date(Number(v) * 1000).toLocaleString('fa-IR') : '—';
  const labels = {queued:'در صف', starting:'در حال شروع', running:'در حال اجرا', completed:'موفق', failed:'ناموفق', paused:'مکث',
    pause_requested:'درخواست مکث', resume_requested:'درخواست ادامه', cancel_requested:'درخواست لغو', cancelled:'لغوشده',
    recovering:'بازیابی', archived:'آرشیو', deleted:'حذف‌شده', interrupted:'وقفه'};
  const palette = {completed:'#14b8a6',running:'#5675e8',queued:'#f1b55c',failed:'#eb6676',paused:'#a78bfa',cancelled:'#a4adbd',other:'#91a7e9'};
  let currentView = 'upload', busy = false, cursor = null, history = [], selected = new Set(), selectedJob = null;
  let currentJobPage = [], previousPages = [], nextPageCursor = null, requestSeq = 0, refreshTimer = null;
  const alive = () => currentView === 'monitor' && !document.hidden;
  async function api(path, options={}) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 20000);
    try {
      const opts = {...options, signal: controller.signal, cache:'no-store'};
      opts.headers = {...(options.headers || {})};
      if (options.json !== undefined) {
        opts.headers['Content-Type'] = 'application/json';
        opts.body = JSON.stringify(options.json);
        delete opts.json;
      }
      const response = await fetch(path, opts);
      const kind = response.headers.get('content-type') || '';
      const payload = kind.includes('json') ? await response.json() : await response.text();
      if (!response.ok) {
        const text = typeof payload === 'object' ? payload.detail || payload.code || JSON.stringify(payload) : payload;
        throw new Error(`${response.status} — ${String(text).slice(0,180)}`);
      }
      return payload;
    } finally {clearTimeout(timeout);}
  }
  function note(message='', error=false) {
    $('monitorAlert').textContent = message;
    $('monitorAlert').classList.toggle('hidden', !message);
    $('monitorAlert').classList.toggle('monitor-alert-danger', error);
  }
  function placeholder(message) {return `<div class="chart-empty">${escapeHTML(message)}</div>`;}
  const svgNS='http://www.w3.org/2000/svg';
  const safePlot = points => points.map(p => Number.isFinite(Number(p)) ? Number(p) : 0);
  function chartSeries(holderId, points, series, label='') {
    const holder=$(holderId);
    if(!points?.length || !series.some(s => safePlot(s.values).some(v => v > 0))) {
      holder.innerHTML = placeholder('هنوز داده ثبت نشده؛ نمودار مقدار ساختگی نمایش نمی‌دهد.');return;
    }
    const W=720,H=242,left=52,right=12,top=20,bottom=52;
    const max=Math.max(1,...series.flatMap(s => safePlot(s.values)));
    const px=i=>left + (W-left-right)*(points.length<2 ? .5 : i/(points.length-1));
    const py=v=>H-bottom-(H-top-bottom)*v/max;
    const grid=[0,.25,.5,.75,1].map(v=>`<g><line x1="${left}" x2="${W-right}" y1="${py(max*v)}" y2="${py(max*v)}" stroke="#ebeff6"/><text x="${left-8}" y="${py(max*v)+4}" text-anchor="end" fill="#8390a4" font-size="12">${escapeHTML(num(max*v))}</text></g>`).join('');
    const lines=series.map(s=>{
      const path=s.values.map((v,i)=>`${i?'L':'M'} ${px(i).toFixed(1)} ${py(Number(v)||0).toFixed(1)}`).join(' ');
      const dots=s.values.map((v,i)=>`<circle cx="${px(i)}" cy="${py(Number(v)||0)}" r="4" fill="${s.color}"><title>${escapeHTML(s.name)} · ${escapeHTML(new Date(points[i].at*1000).toISOString().slice(0,16))} UTC · ${escapeHTML(num(v))}</title></circle>`).join('');
      return `<path d="${path}" stroke="${s.color}" stroke-width="2.8" fill="none" stroke-linecap="round" stroke-linejoin="round"/>${dots}`;
    }).join('');
    const ticks=points.filter((_,i)=>i===0||i===points.length-1||i%Math.max(1,Math.ceil(points.length/6))===0)
      .map(p=>{const i=points.indexOf(p);return `<text x="${px(i)}" y="${H-22}" text-anchor="middle" font-size="11" fill="#73829a">${escapeHTML(new Date(p.at*1000).toISOString().slice(5,16).replace('T',' '))}</text>`;}).join('');
    holder.innerHTML = `<svg viewBox="0 0 ${W} ${H}" class="full-chart" role="img" aria-label="${escapeHTML(label)}" direction="ltr">${grid}${lines}${ticks}</svg>
      <div class="chart-legend">${series.map(s=>`<span><i style="background:${s.color}"></i>${escapeHTML(s.name)}</span>`).join('')}</div>`;
  }
  function doughnut(statuses) {
    const holder=$('statusChart'), pairs=Object.entries(statuses||{}).filter(([,n])=>Number(n)>0);
    const total=pairs.reduce((a,[,n])=>a+Number(n),0);
    if(!total){holder.innerHTML=placeholder('هنوز هیچ Jobی ثبت نشده.');return;}
    let start=0;
    const circles=pairs.map(([state,count])=>{
      const pct=100*count/total, color=palette[state]||palette.other;
      const html=`<circle cx="85" cy="85" r="62" fill="none" stroke="${color}" stroke-width="18" stroke-dasharray="${pct} ${100-pct}" stroke-dashoffset="${-start}" pathLength="100" transform="rotate(-90 85 85)"/>`;
      start+=pct;return html;
    }).join('');
    holder.innerHTML=`<div class="donut-layout"><svg viewBox="0 0 170 170" role="img" aria-label="توزیع وضعیت‌ها">${circles}<text x="85" y="84" text-anchor="middle" class="donut-number">${escapeHTML(num(total))}</text><text x="85" y="103" text-anchor="middle" class="donut-sub">Job</text></svg>
      <div class="donut-legend">${pairs.map(([state,n])=>`<div><span><i style="background:${palette[state]||palette.other}"></i>${escapeHTML(labels[state]||state)}</span><strong>${num(n)}</strong></div>`).join('')}</div></div>`;
  }
  function rejects(rows) {
    const holder=$('rejectChart');
    if(!rows?.length){holder.innerHTML=placeholder('هنوز Reject ثبت نشده.');return;}
    const max=Math.max(1,...rows.map(r=>r.count));
    holder.innerHTML=`<div class="error-chart">${rows.slice(0,8).map((r,i)=>`<div><div class="error-chart-label"><strong title="${escapeHTML(r.reason)}">${escapeHTML(r.reason)}</strong><span>${num(r.count)}</span></div><div class="error-chart-track"><div style="width:${100*r.count/max}%;background:${i?'#6b90e7':'#e77681'}"></div></div></div>`).join('')}</div>`;
  }
  async function loadOverview(){
    const data=await api('/v1/ops/overview');
    const states=data.statuses||{}, parts=data.chunks||{};
    $('kpiBooks').textContent=num(data.books);
    $('kpiActive').textContent=num((states.queued||0)+(states.running||0)+(states.starting||0));
    $('kpiActiveSub').textContent=`در صف ${num(states.queued||0)} · مکث ${num(states.paused||0)}`;
    $('kpiSuccess').textContent=`${num(states.completed||0)} / ${num(states.failed||0)}`;
    $('kpiChunks').textContent=num(Object.values(parts).reduce((a,v)=>a+v,0));
    $('kpiChunksSub').textContent=`موفق ${num(parts.completed||0)} · ناموفق ${num(parts.failed||0)}`;
    const eta=data.eta||{};
    $('kpiEta').textContent=secs(eta.eta_seconds);
    doughnut(states);
  }
  async function loadAnalytics(){
    const until=Date.now()/1000, days=Number($('monitorDays').value);
    const data=await api(`/v1/ops/analytics?since=${until-days*86400}&until=${until}&max_points=120`);
    const points=data.points||[], books=data.book_points||[];
    chartSeries('pairsChart',points,[{name:'تولید اولیه',values:points.map(p=>p.generated),color:'#5478df'},{name:'قبول کیفیت',values:points.map(p=>p.accepted),color:'#14b8a6'}],'Generated / quality pass');
    chartSeries('bookChart',books,[{name:'تکمیل',values:books.map(p=>p.completed),color:'#14b8a6'},{name:'شکست',values:books.map(p=>p.failed),color:'#e97783'}],'Book completions');
    chartSeries('retryChart',points,[{name:'خطا',values:points.map(p=>p.errors),color:'#e97783'},{name:'تلاش مجدد',values:points.map(p=>p.retries),color:'#f0b759'}],'Errors / retries');
    chartSeries('durationChart',points,[{name:'میانگین ثانیه / تلاش',values:points.map(p=>p.calls?p.duration_sum/p.calls:0),color:'#6689e5'}],'Mean SFT attempt seconds');
    rejects(data.rejections);
    const qualityPoints=points.filter(p=>Number(p.quality_count)>0);
    chartSeries('qualityChart',qualityPoints,[{name:'میانگین امتیاز ثبت‌شده',values:qualityPoints.map(p=>p.quality_sum/p.quality_count),color:'#8a71dc'}],'Recorded average quality');
    const generated=points.reduce((a,p)=>a+(Number(p.generated)||0),0), accepted=points.reduce((a,p)=>a+(Number(p.accepted)||0),0);
    $('kpiPairs').textContent=num(accepted);
    $('kpiPairsSub').textContent=`Quality pass ${num(accepted)} / Generated ${num(generated)} · ${days} روز`;
  }
  async function loadHealth(){
    const data=await api('/v1/ops/services');
    const checks=data.services||[];
    $('serviceHealth').innerHTML=checks.map(r=>`<div class="service-health"><span class="indicator ${r.health==='healthy'?'ok':'down'}"></span><div><strong>${escapeHTML(r.name)}</strong><small>${escapeHTML(r.health)} · ${r.probe_latency_sec==null?'latency —':secs(r.probe_latency_sec)}</small></div></div>`).join('')||placeholder('اطلاعات سرویس در دسترس نیست.');
    $('workerHealth').innerHTML=`<div class="health-summary">Book slots: ${num(data.active_book_slots||0)} · Workerهای گزارش‌شده: ${num((data.workers||[]).length)} (نمایش حداکثر ۲۰۰)</div>`
      +(data.workers||[]).slice(0,12).map(w=>`<div class="service-health"><span class="indicator ${w.health==='healthy'?'ok':'down'}"></span><div><strong>${escapeHTML(w.kind)} · ${escapeHTML(w.id)}</strong><small>${escapeHTML(w.health)} · capacity ${num(w.capacity)} · ${when(w.heartbeat)}</small></div></div>`).join('');
    traceTemplate = data.trace_url_template || '';
    const trace=data.telemetry||{};
    $('observabilityStatus').textContent=`Tracing: ${trace.enabled?'فعال':'غیرفعال'}${trace.error?' · '+trace.error:''} · Langfuse برای مشاهده Trace نیاز به راه‌اندازی و تنظیم مستقل دارد.`;
  }
  const filterKey = () => `${$('historySearch').value.trim()}|${$('historyStatus').value}|${$('historyPageSize').value}`;
  function resetHistory(){cursor=null;previousPages=[];nextPageCursor=null;selected.clear();updateSelected();}
  function updateSelected(){ $('selectedCount').textContent=`${num(selected.size)} انتخاب`; }
  async function loadJobs(){
    const seq=++requestSeq;
    const query=new URLSearchParams({limit:$('historyPageSize').value});
    if($('historySearch').value.trim())query.set('q',$('historySearch').value.trim());
    if($('historyStatus').value)query.set('status',$('historyStatus').value);
    if(cursor)query.set('cursor',cursor);
    const data=await api('/v1/ops/jobs?'+query.toString());
    if(seq!==requestSeq)return;
    currentJobPage=data.items||[];nextPageCursor=data.next_cursor;
    const rows=$('historyRows');
    rows.innerHTML=currentJobPage.map(j=>`<tr>
      <td><input type="checkbox" data-pick="${escapeHTML(j.id)}" ${selected.has(j.id)?'checked':''} aria-label="انتخاب Job"></td>
      <td><strong class="book-name">${escapeHTML(j.title||'بدون عنوان')}</strong><small class="mono">${escapeHTML(j.book_id)}</small></td>
      <td><span class="status-tag status-${escapeHTML(j.status)}"><i></i>${escapeHTML(labels[j.status]||j.status)}</span></td>
      <td><small>${when(j.updated)}</small></td><td>${num(j.attempt)}</td>
      <td><code class="mono">${escapeHTML(j.id.slice(0,8))}…</code></td>
      <td><button type="button" class="ghost-btn table-action" data-open="${escapeHTML(j.id)}">جزئیات ←</button></td></tr>`).join('')||'<tr><td colspan="7" class="empty-state">کتابی با این فیلتر پیدا نشد.</td></tr>';
    $('historyPrev').disabled=previousPages.length===0;
    $('historyNext').disabled=!nextPageCursor;
    $('historyPage').textContent=`صفحه ${num(previousPages.length+1)} · ${num(currentJobPage.length)} ردیف`;
  }
  const readableJSON = text => {if (typeof text === 'string') {try{return JSON.parse(text);}catch{return text;}}return text;};
  const facts = data => Object.entries(data||{}).map(([key,v])=>`<div class="field-row"><span>${escapeHTML(key)}</span><strong>${escapeHTML(v==null?'—':typeof v==='object'?JSON.stringify(v).slice(0,200):v)}</strong></div>`).join('');
  async function pagedDetail(path, cursor=null){
    const qs = new URLSearchParams({limit:'50'});
    if(cursor)qs.set('cursor',cursor);
    return api(path+'?'+qs.toString());
  }
  function detailsRows(rows, type){
    if(!rows?.length)return placeholder('موردی ثبت نشده است.');
    return `<div class="detail-records">${rows.map(row=>{
      const label = type==='chunks'?`Chunk #${row.ordinal} · ${row.id}`:type==='events'?`رویداد ${row.kind}`:type==='attempts'?`تلاش #${row.number} · ${row.chunk_id}`:`Artifact ${row.category}`;
      const sub=type==='events'?when(row.at):type==='attempts'?when(row.started):type==='artifacts'?`${num(row.size)} B`:`${row.status||'—'}`;
      return `<details class="detail-record"><summary>${escapeHTML(label)} <small>${escapeHTML(sub)}</small></summary><div class="record-grid">${facts(Object.fromEntries(Object.entries(row).filter(([k])=>!['detail','config','error','input_ref','output_ref'].includes(k))))}</div>
        ${row.trace_id?traceLink(row.trace_id):''}
        ${row.input_ref?`<p class="mono detail-ref">Input ref: ${escapeHTML(row.input_ref)}</p>`:''}
        ${row.output_ref?`<p class="mono detail-ref">Output ref: ${escapeHTML(row.output_ref)}</p>`:''}
        ${row.error?`<pre class="monitor-error">${escapeHTML(readableJSON(row.error) instanceof Object?JSON.stringify(readableJSON(row.error),null,2):row.error)}</pre>`:''}
        ${row.detail?`<pre class="detail-json">${escapeHTML(JSON.stringify(readableJSON(row.detail),null,2))}</pre>`:''}
        ${type==='artifacts'&&row.status==='available'?`<a class="small-download" href="/v1/ops/artifacts/${encodeURIComponent(row.id)}">دریافت Artifact ↓</a>`:''}</details>`;
    }).join('')}</div>`;
  }
  let traceTemplate='';
  function traceLink(id){
    if(!traceTemplate)return `<small class="muted">Trace ID: ${escapeHTML(id)} · لینک Langfuse هنوز تنظیم نشده</small>`;
    const href=traceTemplate.replace('{trace_id}',encodeURIComponent(id));
    if(!/^https:\/\//i.test(href))return '';
    return `<a class="small-download" href="${escapeHTML(href)}" target="_blank" rel="noopener noreferrer">مشاهده در Langfuse ↗</a>`;
  }
  async function openJob(id){
    selectedJob=id;
    $('jobDrawer').classList.remove('hidden');document.body.classList.add('drawer-open');
    $('drawerBody').innerHTML=placeholder('در حال بارگذاری جزئیات…');
    try {
      const [job,chunks,attempts,events,artifacts] = await Promise.all([
        api('/v1/ops/jobs/'+encodeURIComponent(id)),
        pagedDetail(`/v1/ops/jobs/${encodeURIComponent(id)}/chunks`),
        pagedDetail(`/v1/ops/jobs/${encodeURIComponent(id)}/attempts`),
        pagedDetail(`/v1/ops/jobs/${encodeURIComponent(id)}/events`),
        pagedDetail(`/v1/ops/jobs/${encodeURIComponent(id)}/artifacts`)]);
      if(selectedJob!==id)return;
      $('drawerTitle').textContent=job.title||'کتاب';$('drawerId').textContent=id;
      const commandOptions=availableActions(job.status);
      const stageHtml=(job.stages||[]).map(s=>`<div class="detail-timing-row"><strong>${escapeHTML(s.name)}</strong><span>${escapeHTML(labels[s.status]||s.status)}</span><small>${s.started&&s.ended?secs(s.ended-s.started):'—'}</small></div>`).join('');
      $('drawerBody').innerHTML=`<div class="drawer-top-status"><span class="status-tag status-${escapeHTML(job.status)}">${escapeHTML(labels[job.status]||job.status)}</span><span>${when(job.updated)}</span></div>
        <div class="drawer-metrics"><div><span>کتاب</span><strong>${escapeHTML(job.book_id)}</strong></div><div><span>تلاش</span><strong>${num(job.attempt)}</strong></div><div><span>قابل Resume</span><strong>${job.resumable?'بله':'خیر'}</strong></div><div><span>Chunkها</span><strong>${num(Object.values(job.chunks||{}).reduce((a,v)=>a+v,0))}</strong></div></div>
        <div class="ops-actions">${commandOptions.map(([a,title])=>`<button type="button" class="ghost-btn ${a==='delete'?'danger-btn':''}" data-command="${a}">${title}</button>`).join('')||'<span class="muted">برای این وضعیت عملیاتی وجود ندارد.</span>'}</div>
        <div id="commandNotice" class="muted mini-explain">عملیات روی صف پایدار ثبت می‌شود؛ تکمیل واقعی با بروزرسانی وضعیت مشخص می‌شود.</div>
        <h3>مراحل پردازش</h3>${stageHtml||placeholder('هنوز مرحله‌ای ثبت نشده.')}<h3>Chunkها <small>۵۰ رکورد اول</small></h3>
        <div id="chunkDetails">${detailsRows(chunks.items,'chunks')}</div>${chunks.next_cursor?`<button class="ghost-btn load-detail" data-next="chunks" data-cursor="${escapeHTML(chunks.next_cursor)}">چانک‌های بیشتر</button>`:''}
        <h3>تلاش‌ها و خطاها <small>۵۰ رکورد اول</small></h3><div id="attemptDetails">${detailsRows(attempts.items,'attempts')}</div>
        ${attempts.next_cursor?`<button class="ghost-btn load-detail" data-next="attempts" data-cursor="${escapeHTML(attempts.next_cursor)}">تلاش‌های بیشتر</button>`:''}
        <h3>رویدادها <small>۵۰ رکورد اول</small></h3><div id="eventDetails">${detailsRows(events.items,'events')}</div>
        ${events.next_cursor?`<button class="ghost-btn load-detail" data-next="events" data-cursor="${escapeHTML(events.next_cursor)}">رویدادهای بیشتر</button>`:''}
        <h3>فایل‌های خروجی</h3><div id="artifactDetails">${detailsRows(artifacts.items,'artifacts')}</div>
        ${artifacts.next_cursor?`<button class="ghost-btn load-detail" data-next="artifacts" data-cursor="${escapeHTML(artifacts.next_cursor)}">خروجی‌های بیشتر</button>`:''}`;
    } catch(e){if(selectedJob===id)$('drawerBody').innerHTML=placeholder('دریافت اطلاعات ناموفق بود: '+e.message);}
  }
  const availableActions=status=>{
    const options=[];
    if(['queued','running','starting','recovering'].includes(status))options.push(['pause','مکث در نقطه امن']);
    if(status==='paused')options.push(['resume','ادامه پردازش']);
    if(['queued','running','paused','recovering','pause_requested','resume_requested'].includes(status))options.push(['cancel','لغو Job']);
    if(['failed','completed','cancelled'].includes(status))options.push(['retry','تلاش مجدد']);
    if(['failed','completed','cancelled'].includes(status))options.push(['archive','آرشیو']);
    if(['failed','completed','cancelled','archived'].includes(status))options.push(['delete','حذف نرم (بدون حذف فایل واقعی)']);
    return options;
  };
  async function command(id, action, payload={}) {
    if(!confirm(`عملیات «${action}» برای ${id.slice(0,8)} ثبت شود؟\nدرخواست لغو یا مکث ممکن است تا رسیدن به نقطه امن زمان ببرد.`))return;
    const options={method:action==='delete'?'DELETE':'POST',headers:{'Idempotency-Key':crypto.randomUUID()}};
    if(action!=='delete')options.json=payload;
    const result=await api(`/v1/ops/jobs/${encodeURIComponent(id)}${action==='delete'?'':'/'+action}`,options);
    note(`درخواست ${action} ثبت شد؛ Command ID: ${result.id||result.command_id||'—'}`);
    await Promise.all([loadJobs(),openJob(id)]);
  }
  async function loadMore(button){
    const key=button.dataset.next, id=selectedJob, cursor=button.dataset.cursor;
    if(!id)return;
    button.disabled=true;
    try{
      const data=await pagedDetail(`/v1/ops/jobs/${encodeURIComponent(id)}/${key}`,cursor);
      $(`${key==='chunks'?'chunk':key==='attempts'?'attempt':key==='events'?'event':'artifact'}Details`).insertAdjacentHTML('beforeend',detailsRows(data.items,key));
      if(data.next_cursor){button.dataset.cursor=data.next_cursor;button.disabled=false;} else button.remove();
    } catch(e){button.disabled=false;note(e.message,true);}
  }
  const retentionLabels={temporary_files:'فایل‌های موقت',job_metadata:'متادیتای Job',chunk_inputs:'ورودی Chunk',chunk_outputs:'خروجی Chunk',task_details:'جزئیات Task',error_logs:'لاگ خطا',monitoring_events:'رویدادهای مانیتورینگ',generated_artifacts:'فایل‌های تولیدشده',completed_datasets:'دیتاست نهایی'};
  async function loadRetention(){
    const [p,r]=await Promise.all([api('/v1/admin/retention/policies'),api('/v1/admin/retention/runs?limit=10')]);
    const known=Object.fromEntries((p.items||[]).map(v=>[v.category,v]));
    $('retentionRows').innerHTML=Object.entries(retentionLabels).map(([category,title])=>{
      const row=known[category]||{days:30,enabled:false};
      return `<div class="retention-row"><span>${escapeHTML(title)}</span><input type="number" min="1" max="36500" value="${row.days}" data-days="${category}" aria-label="روز نگهداری ${escapeHTML(title)}"><label><input type="checkbox" data-enabled="${category}" ${row.enabled?'checked':''}> فعال</label><button class="ghost-btn" data-save="${category}" type="button">ذخیره</button></div>`;
    }).join('');
    $('retentionStatus').textContent=`آخرین اجرا: ${(r.items||[]).length?escapeHTML(r.items[0].status)+' · '+when(r.items[0].created):'بدون سابقه'} · پاک‌سازی زمان‌بندی‌شده فقط با فعال‌سازی صریح انجام می‌شود.`;
  }
  async function retentionSave(category){
    const day=document.querySelector(`[data-days="${category}"]`),enabled=document.querySelector(`[data-enabled="${category}"]`);
    await api('/v1/admin/retention/policies',{method:'PUT',json:{category,days:Number(day.value),enabled:enabled.checked}});
    note('سیاست ذخیره شد؛ برای Artifactهای جدید اعمال می‌شود.');
  }
  async function refresh(force=false){
    if(!alive()||busy)return;
    busy=true;
    try {
      const tasks=[loadOverview(),loadHealth(),loadJobs()];
      if(force || !window.__lastAnalytics || Date.now()-window.__lastAnalytics > 60000){tasks.push(loadAnalytics().then(()=>{window.__lastAnalytics=Date.now();}));}
      const results=await Promise.allSettled(tasks);
      const failures=results.filter(x=>x.status==='rejected');
      if(failures.length)note('برخی اطلاعات قابل دریافت نیست: '+failures.map(x=>x.reason.message).join(' | '),true);
      else note('');
      $('monitorUpdated').textContent='بروزرسانی '+new Date().toLocaleTimeString('fa-IR');
    } finally {busy=false;}
  }
  function switchView(name){
    currentView=name;
    $('uploadView').classList.toggle('hidden',name!=='upload');
    $('monitorView').classList.toggle('hidden',name!=='monitor');
    document.querySelectorAll('.nav-tab').forEach(b=>b.classList.toggle('active',b.dataset.view===name));
    if(name==='monitor'){refresh(true);loadRetention().catch(e=>note('Retention: '+e.message,true));}
  }
  document.querySelectorAll('.nav-tab').forEach(b=>b.addEventListener('click',()=>location.hash=b.dataset.view));
  addEventListener('hashchange',()=>switchView(location.hash==='#monitor'?'monitor':'upload'));
  $('monitorRefresh').onclick=()=>refresh(true);
  $('serviceRefresh').onclick=()=>loadHealth().catch(e=>note(e.message,true));
  $('monitorDays').onchange=()=>{window.__lastAnalytics=0;refresh(true);};
  $('historySearch').oninput=()=>{clearTimeout(refreshTimer);refreshTimer=setTimeout(()=>{resetHistory();loadJobs().catch(e=>note(e.message,true));},400);};
  $('historyStatus').onchange=$('historyPageSize').onchange=()=>{resetHistory();loadJobs().catch(e=>note(e.message,true));};
  $('historyReload').onclick=()=>{resetHistory();loadJobs().catch(e=>note(e.message,true));};
  $('historyPrev').onclick=()=>{if(!previousPages.length)return;cursor=previousPages.pop();loadJobs().catch(e=>note(e.message,true));};
  $('historyNext').onclick=()=>{if(!nextPageCursor)return;previousPages.push(cursor);cursor=nextPageCursor;loadJobs().catch(e=>note(e.message,true));};
  $('historyRows').onclick=e=>{
    const row=e.target.closest('[data-open]');if(row)openJob(row.dataset.open);
    const checkbox=e.target.closest('[data-pick]');if(checkbox){checkbox.checked?selected.add(checkbox.dataset.pick):selected.delete(checkbox.dataset.pick);updateSelected();}
  };
  $('bulkApply').onclick=async()=>{
    if(!selected.size){note('ابتدا Jobها را انتخاب کن.');return;}
    const ids=[...selected],action=$('bulkAction').value;
    if(!confirm(`${num(ids.length)} Job برای «${action}» انتخاب شد. ادامه؟`))return;
    try{
      const result=await api('/v1/ops/bulk',{method:'POST',headers:{'Idempotency-Key':crypto.randomUUID()},json:{job_ids:ids,action}});
      const counts={};(result.items||[]).forEach(r=>counts[r.status]=(counts[r.status]||0)+1);
      note('نتیجه: '+Object.entries(counts).map(([s,n])=>`${s} ${num(n)}`).join(' · '));
      selected.clear();updateSelected();await loadJobs();
    }catch(e){note(e.message,true);}
  };
  $('drawerClose').onclick=$('drawerBackdrop').onclick=()=>{selectedJob=null;$('jobDrawer').classList.add('hidden');document.body.classList.remove('drawer-open');};
  $('drawerBody').onclick=async e=>{
    const c=e.target.closest('[data-command]');
    if(c){c.disabled=true;try{await command(selectedJob,c.dataset.command);}catch(e){note(e.message,true);}finally{c.disabled=false;}return;}
    const more=e.target.closest('.load-detail');if(more)await loadMore(more);
  };
  $('retentionRows').onclick=async e=>{const b=e.target.closest('[data-save]');if(!b)return;b.disabled=true;try{await retentionSave(b.dataset.save);}catch(error){note(error.message,true);}finally{b.disabled=false;}};
  $('retentionPreview').onclick=async()=>{try{const result=await api('/v1/admin/retention/preview',{method:'POST'});note(`پیش‌نمایش: ${num(result.count)} فایل (${num(result.bytes)} بایت)، حذف انجام نشد.`);}catch(e){note(e.message,true);}};
  $('retentionDryRun').onclick=async()=>{try{const result=await api('/v1/admin/retention/runs',{method:'POST',json:{dry_run:true}});note(`Dry-run ثبت شد: ${result.run_id}`);}catch(e){note(e.message,true);}};
  setInterval(()=>{if(alive()&&$('monitorAuto').checked&&!selectedJob)refresh();},15000);
  switchView(location.hash==='#monitor'?'monitor':'upload');
})();
