const $ = id => document.getElementById(id);
const show = (id, value) => $(id).textContent = JSON.stringify(value, null, 2);
// Strip any URL userinfo before constructing Fetch requests. Browser-managed
// HTTP authentication still supplies credentials for this same origin.
const apiURL = path => new URL(path, window.location.origin).href;
let refreshTimer = null;
let refreshDelay = 30000;
async function command(text) {
  const response = await fetch(apiURL('/api/command'), {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({command:text})});
  const value = await response.json();
  $('message').textContent = value.message || value.detail || 'Request failed';
  await refresh();
}
async function refresh() {
  clearTimeout(refreshTimer);
  try {
    const response = await fetch(apiURL('/api/overview'));
    if (!response.ok) throw new Error('Authentication or service unavailable');
    const data = await response.json();
    // Saver mode must not keep Neon awake via an open dashboard tab.
    refreshDelay = data.status.neon_batch_seconds > 0 ? 0 : 30000;
    $('mode').textContent = data.status.paper_mode ? 'PAPER MODE' : data.status.live_adapter_enabled ? 'LIVE ENABLED' : 'MANUAL EXPORT';
    show('status', data.status); show('signals', data.signals); show('campaigns', data.campaigns); show('report', data.report); show('logs', data.logs);
    $('movers').replaceChildren();
    for (const row of [...data.market].sort((a,b)=>(b.changes['24h']||0)-(a.changes['24h']||0))) {
      const tr = document.createElement('tr');
      for (const text of [row.symbol, row.metrics.quote, row.changes['1h'] == null ? 'Unavailable' : row.changes['1h'].toFixed(2)+'%', row.changes['24h'] == null ? 'Unavailable' : row.changes['24h'].toFixed(2)+'%', row.metrics.source]) {
        const td = document.createElement('td'); td.textContent=text;tr.append(td);
      }
      $('movers').append(tr);
    }
    $('drafts').replaceChildren();
    for (const row of data.drafts) {
      const article=document.createElement('article'), title=document.createElement('h3'), meta=document.createElement('small'), body=document.createElement('p');
      title.textContent=row.payload.title; meta.textContent=`${row.status} · ${row.payload.risk} risk · ${row.id}`;body.textContent=row.payload.body;
      article.append(title,meta,body);
      if(row.payload.image){const a=document.createElement('a');a.href='/artifacts/'+encodeURIComponent(row.payload.image);a.textContent='Preview chart';a.target='_blank';article.append(a);}
      if(['review','approved','queued'].includes(row.status)) {
        for(const action of ['approve','reject']) {const button=document.createElement('button');button.textContent=action;button.addEventListener('click',()=>command('/'+action+' '+row.id));article.append(button);}
      }
      if(row.payload.receipt?.file){const a=document.createElement('a');a.href='/artifacts/'+encodeURIComponent(row.payload.receipt.file);a.textContent='Download publication';article.append(a);}
      $('drafts').append(article);
    }
  } catch(error) { $('message').textContent=error.message; }
  finally { if(refreshDelay && !document.hidden) refreshTimer = setTimeout(refresh,refreshDelay); }
}
document.querySelectorAll('[data-command]').forEach(button=>button.addEventListener('click',()=>command(button.dataset.command)));
$('refresh').addEventListener('click',refresh);
$('command-form').addEventListener('submit',event=>{event.preventDefault();command($('command').value);});
fetch(apiURL('/api/command'),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({command:'/help'})}).then(r=>r.json()).then(v=>$('help').textContent=v.message);
refresh();
