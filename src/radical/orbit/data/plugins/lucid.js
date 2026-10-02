/**
 * LUCID Plugin Module for ORBIT Explorer
 *
 * Mock-up Cell Painting portal: pick input data (Globus collection + plate
 * wells), a target HPC endpoint and an analysis; submit; follow the run
 * (stage-in -> compute -> package -> stage-out -> fetch) and see the
 * resulting cell images inline.  The run itself is driven by the lucid
 * plugin (radical.orbit.lucid_flow) on the endpoint serving this page.
 */

export const name = 'lucid';

let escHtml = s => String(s || '');   // replaced by api.escHtml in init()

const pollers   = {};                  // endpointName -> interval id

const PHASES = [
  ['preflight', 'Preflight'],
  ['stage_in',  'Stage-in (Globus)'],
  ['compute',   'Compute (rhapsody)'],
  ['package',   'Package'],
  ['stage_out', 'Stage-out (Globus)'],
  ['fetch',     'Fetch previews'],
];

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">🔬</div>
      <h2>LUCID Cell Painting — <span class="endpoint-label"></span></h2>
      <button class="btn btn-secondary btn-sm" style="margin-left:auto" data-action="refresh">↺ Refresh</button>
    </div>

    <div class="card">
      <div class="card-title">📥 Input data</div>
      <div class="form-group">
        <label>Globus collection</label>
        <input class="lc-collection" type="text" />
      </div>
      <div class="lc-row">
        <div class="form-group">
          <label>Images path</label>
          <input class="lc-images" type="text" />
        </div>
        <div class="form-group">
          <label>Pipeline path</label>
          <input class="lc-pipeline" type="text" />
        </div>
      </div>
      <div class="form-group">
        <label>Wells <span class="lc-well-count"></span>
          <a href="#" data-action="wells-all">all</a> ·
          <a href="#" data-action="wells-none">none</a>
          <span class="lc-hint">(click a well, a row or a column label)</span></label>
        <div class="lc-plate"></div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">⚙️ Target and analysis</div>
      <div class="lc-row">
        <div class="form-group">
          <label>Target endpoint</label>
          <select class="lc-endpoint"></select>
        </div>
        <div class="form-group">
          <label>Analysis</label>
          <select class="lc-analysis"></select>
        </div>
      </div>
      <button class="btn btn-primary lc-submit" data-action="submit">▶ Submit</button>
      <span class="lc-submit-msg"></span>
    </div>

    <div class="card lc-progress-card">
      <div class="card-title">📊 Progress <span class="lc-run-id"></span></div>
      <div class="lc-idle" style="color:var(--muted)">No run yet.</div>
      <div class="lc-progress" style="display:none">
        <ul class="lc-phases"></ul>
        <div class="lc-bar">
          <div class="lc-seg lc-seg-done"></div>
          <div class="lc-seg lc-seg-failed"></div>
          <div class="lc-seg lc-seg-exec"></div>
          <div class="lc-seg lc-seg-sub"></div>
        </div>
        <div class="lc-legend"></div>
        <div class="lc-stats"></div>
        <div class="lc-error"></div>
      </div>
    </div>

    <div class="card lc-results-card" style="display:none">
      <div class="card-title">🖼️ Results</div>
      <div class="lc-target"></div>
      <div class="lc-images-out"></div>
    </div>
  `;
}

export function css() {
  return `
    .lc-row { display:grid; grid-template-columns: 1fr 1fr; gap:12px; }
    .lc-hint { color:var(--muted); font-weight:normal; margin-left:6px; }
    .lc-progress-card a, .card a[data-action^="wells-"] { color:var(--accent); }
    .lc-plate { display:grid; grid-template-columns: 28px repeat(12, 1fr);
                gap:3px; max-width:560px; margin-top:6px; }
    .lc-plate .lc-lbl { font-size:11px; color:var(--muted); text-align:center;
                        cursor:pointer; align-self:center; }
    .lc-plate .lc-well { aspect-ratio:1; border-radius:50%; cursor:pointer;
                         border:1px solid var(--border); background:transparent; }
    .lc-plate .lc-well.sel { background:var(--accent); border-color:var(--accent); }
    .lc-phases { list-style:none; padding:0; margin:0 0 12px 0; }
    .lc-phases li { padding:2px 0; }
    .lc-phases .lc-secs { color:var(--muted); margin-left:6px; }
    .lc-bar { display:flex; height:18px; border-radius:9px; overflow:hidden;
              background:var(--border); }
    .lc-seg { height:100%; width:0; transition:width .4s; }
    .lc-seg-done   { background:#34d399; }
    .lc-seg-failed { background:var(--danger); }
    .lc-seg-exec   { background:var(--accent); }
    .lc-seg-sub    { background:var(--muted); }
    .lc-legend { font-size:12px; color:var(--muted); margin:6px 0 10px 0; }
    .lc-legend b { color:var(--text); }
    .lc-stats table { border-collapse:collapse; font-size:13px; }
    .lc-stats td { padding:2px 12px 2px 0; }
    .lc-error { color:var(--danger); margin-top:8px; white-space:pre-wrap; }
    .lc-target { color:var(--muted); margin-bottom:10px; font-size:13px; }
    .lc-images-out { display:flex; flex-wrap:wrap; gap:10px; align-items:flex-start; }
    .lc-images-out img { border-radius:6px; border:1px solid var(--border);
                         background:#000; }
    .lc-images-out img.lc-montage { width:480px; max-width:100%; }
    .lc-images-out img.lc-cell    { width:112px; height:112px; object-fit:contain; }
    .lc-submit-msg { margin-left:10px; color:var(--muted); }
  `;
}

export function init(page, api) {
  escHtml = api.escHtml;
  page.querySelector('[data-action="refresh"]')
      ?.addEventListener('click', () => load(page, api));
  page.querySelector('[data-action="submit"]')
      ?.addEventListener('click', () => submit(page, api));
  page.querySelector('[data-action="wells-all"]')
      ?.addEventListener('click', e => { e.preventDefault(); setAll(page, true); });
  page.querySelector('[data-action="wells-none"]')
      ?.addEventListener('click', e => { e.preventDefault(); setAll(page, false); });
  load(page, api);
}

export function onNotification(data, page, api) {
  if (data.topic === 'run_status') render(page, api, data.data);
}


// ─────────────────────────────────────────────────────────────
//  Internals
// ─────────────────────────────────────────────────────────────

// The Explorer caches sessions and heals stale ones (expiry, endpoint
// restart) -- always go through it.
async function getSession(api) {
  return await api.getSession('lucid');
}

async function load(page, api) {
  try {
    const sid = await getSession(api);
    const cfg = await api.fetch(`config/${sid}`);
    fillForm(page, cfg);
    const st  = await api.fetch(`status/${sid}`);
    render(page, api, st);
    if (st.status === 'running') startPolling(page, api);
  } catch (e) {
    api.flash(`LUCID: ${e.message}`, false);
  }
}

function fillForm(page, cfg) {
  const d = cfg.defaults || {};
  const setIfEmpty = (sel, val) => {
    const el = page.querySelector(sel);
    if (el && !el.value) el.value = val || '';
  };
  setIfEmpty('.lc-collection', d.collection);
  setIfEmpty('.lc-images',     d.images_path);
  setIfEmpty('.lc-pipeline',   d.pipeline_path);

  // endpoints: keep the current choice if it is still offered
  const ep  = page.querySelector('.lc-endpoint');
  const cur = ep.value;
  const eps = cfg.endpoints || [];
  ep.innerHTML = eps.length
    ? eps.map(n => `<option value="${escHtml(n)}">${escHtml(n)}</option>`).join('')
    : '<option value="">— no endpoint serving globus + rhapsody + staging —</option>';
  ep.value = eps.includes(cur) ? cur : (cfg.endpoint || '');

  // analyses: disabled entries are shown greyed out
  const an = page.querySelector('.lc-analysis');
  if (!an.options.length) {
    an.innerHTML = (cfg.analyses || []).map(a =>
      `<option value="${escHtml(a.id)}" ${a.enabled ? '' : 'disabled'}
               ${a.id === d.analysis ? 'selected' : ''}>${escHtml(a.label)}</option>`
    ).join('');
  }

  const plate = page.querySelector('.lc-plate');
  if (!plate.children.length) buildPlate(page, cfg.rows || 8, cfg.cols || 12);
}

function buildPlate(page, rows, cols) {
  const plate = page.querySelector('.lc-plate');
  const cells = ['<div></div>'];
  for (let c = 1; c <= cols; c++) {
    cells.push(`<div class="lc-lbl" data-col="${c}">${c}</div>`);
  }
  for (let r = 1; r <= rows; r++) {
    const row = String.fromCharCode(64 + r);          // A..H
    cells.push(`<div class="lc-lbl" data-row="${r}">${row}</div>`);
    for (let c = 1; c <= cols; c++) {
      const id = `r${String(r).padStart(2, '0')}c${String(c).padStart(2, '0')}`;
      cells.push(`<div class="lc-well sel" data-well="${id}" title="${row}${c} (${id})"></div>`);
    }
  }
  plate.innerHTML = cells.join('');
  plate.addEventListener('click', e => {
    const t = e.target;
    if (t.dataset.well) {
      t.classList.toggle('sel');
    } else if (t.dataset.row || t.dataset.col) {
      const key   = t.dataset.row ? `r${String(t.dataset.row).padStart(2, '0')}`
                                  : `c${String(t.dataset.col).padStart(2, '0')}`;
      const wells = [...plate.querySelectorAll('.lc-well')]
                      .filter(w => w.dataset.well.includes(key));
      const on    = !wells.every(w => w.classList.contains('sel'));
      wells.forEach(w => w.classList.toggle('sel', on));
    }
    updateWellCount(page);
  });
  updateWellCount(page);
}

function setAll(page, on) {
  page.querySelectorAll('.lc-well').forEach(w => w.classList.toggle('sel', on));
  updateWellCount(page);
}

function selectedWells(page) {
  return [...page.querySelectorAll('.lc-well.sel')].map(w => w.dataset.well);
}

function updateWellCount(page) {
  page.querySelector('.lc-well-count').textContent =
      `(${selectedWells(page).length} selected)`;
}

async function submit(page, api) {
  const btn   = page.querySelector('.lc-submit');
  const msg   = page.querySelector('.lc-submit-msg');
  const wells = selectedWells(page);
  const body  = {
    collection   : page.querySelector('.lc-collection').value.trim(),
    images_path  : page.querySelector('.lc-images').value.trim(),
    pipeline_path: page.querySelector('.lc-pipeline').value.trim(),
    endpoint     : page.querySelector('.lc-endpoint').value,
    analysis     : page.querySelector('.lc-analysis').value,
    wells,
  };
  if (!body.endpoint)  { api.flash('Select a target endpoint', false); return; }
  if (!wells.length)   { api.flash('Select at least one well', false); return; }

  btn.disabled    = true;
  msg.textContent = 'submitting…';
  try {
    const sid = await getSession(api);
    const res = await api.fetch(`submit/${sid}`, {
      method: 'POST', body: JSON.stringify(body) });
    msg.textContent = `run ${res.run_id} started`;
    page.querySelector('.lc-results-card').style.display = 'none';
    startPolling(page, api);
  } catch (e) {
    msg.textContent = '';
    btn.disabled    = false;
    api.flash(`Submit failed: ${e.message}`, false);
  }
}

// Notifications drive the page; polling is the safety net (e.g. an SSE gap).
function startPolling(page, api) {
  const ep = api.endpointName;
  if (pollers[ep]) return;
  pollers[ep] = setInterval(async () => {
    if (!page.isConnected) { stopPolling(ep); return; }
    try {
      const sid = await getSession(api);
      const st  = await api.fetch(`status/${sid}`);
      render(page, api, st);
      if (st.status !== 'running') stopPolling(ep);
    } catch (e) { /* transient: keep polling */ }
  }, 2000);
}

function stopPolling(ep) {
  clearInterval(pollers[ep]);
  delete pollers[ep];
}

function gb(n) { return `${((n || 0) / 1e9).toFixed(2)} GB`; }

function render(page, api, st) {
  if (!st || st.status === 'idle' || !st.run_id) return;

  const running = st.status === 'running';
  page.querySelector('.lc-submit').disabled = running;
  if (!running) page.querySelector('.lc-submit-msg').textContent = '';
  page.querySelector('.lc-idle').style.display     = 'none';
  page.querySelector('.lc-progress').style.display = '';
  page.querySelector('.lc-run-id').textContent =
      `— ${st.run_id} on ${st.endpoint} (${st.status})`;

  // phases
  const icon = { pending: '○', running: '⏳', done: '✅', failed: '❌' };
  page.querySelector('.lc-phases').innerHTML = PHASES.map(([k, label]) => {
    const p = (st.phases || {})[k] || {};
    const s = p.secs != null ? `<span class="lc-secs">${p.secs} s</span>` : '';
    return `<li>${icon[p.state] || '○'} ${escHtml(label)}${s}</li>`;
  }).join('');

  // task bar: done | failed | executing | submitted
  const t     = st.tasks || {};
  const total = t.total || 1;
  const pct   = n => `${(100 * (n || 0) / total).toFixed(1)}%`;
  const phase = (st.phases || {}).compute || {};
  const sub   = phase.state === 'pending' ? 0 : (t.submitted || 0);
  page.querySelector('.lc-seg-done').style.width   = pct(t.done);
  page.querySelector('.lc-seg-failed').style.width = pct(t.failed);
  page.querySelector('.lc-seg-exec').style.width   = pct(t.executing);
  page.querySelector('.lc-seg-sub').style.width    = pct(sub);
  page.querySelector('.lc-legend').innerHTML =
      `tasks: <b>${t.total || 0}</b> · submitted <b>${sub}</b> · ` +
      `executing <b>${t.executing || 0}</b> · done <b>${t.done || 0}</b> · ` +
      `failed <b>${t.failed || 0}</b>`;

  // stats
  const s    = st.stats || {};
  const rows = [];
  if (s.stage_in)  rows.push(['staged in',  `${s.stage_in.files} files, ${gb(s.stage_in.bytes)}`]);
  if (s.compute)   rows.push(['compute',    `${s.compute.done}/${s.compute.tasks} tasks on ` +
                                            `${s.compute.nodes} node(s), ${s.compute.files} files created`]);
  if (s.stage_out) rows.push(['staged out', `${s.stage_out.files} files, ${gb(s.stage_out.bytes)}`]);
  page.querySelector('.lc-stats').innerHTML = rows.length
    ? `<table>${rows.map(([k, v]) => `<tr><td>${k}</td><td>${escHtml(v)}</td></tr>`).join('')}</table>`
    : '';

  const failed = Object.entries(st.failed_wells || {});
  page.querySelector('.lc-error').textContent =
      (st.error ? `Error: ${st.error}\n` : '') +
      failed.slice(0, 5).map(([w, e]) => `${w}: ${e}`).join('\n');

  if (st.status === 'done' && (st.images || []).length) showImages(page, api, st);
}

async function showImages(page, api, st) {
  // tracked per page: a rebuilt page (reconnect) shows the results again
  if (page.dataset.shownRun === st.run_id) return;
  page.dataset.shownRun = st.run_id;

  const card = page.querySelector('.lc-results-card');
  const out  = page.querySelector('.lc-images-out');
  card.style.display = '';
  page.querySelector('.lc-target').textContent =
      `Results staged to the LUCID collection: ${st.target}`;
  out.innerHTML = '';
  try {
    const sid = await getSession(api);
    for (const n of st.images) {
      const img = await api.fetch(`image/${sid}/${encodeURIComponent(st.run_id)}/${encodeURIComponent(n)}`);
      const el  = document.createElement('img');
      el.src       = `data:${img.mime};base64,${img.data}`;
      el.title     = n;
      el.className = n === 'montage.png' ? 'lc-montage' : 'lc-cell';
      out.appendChild(el);
    }
  } catch (e) {
    delete page.dataset.shownRun;
    api.flash(`Could not load result images: ${e.message}`, false);
  }
}
