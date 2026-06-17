/**
 * Globus Plugin Module for Radical Edge Explorer
 *
 * File staging via Globus Online (Transfer API).  Globus moves data
 * collection-to-collection out of band, so this UI is an orchestrator:
 * authenticate with a Transfer token, submit transfers between two
 * collection UUIDs, browse a collection, and monitor task state.
 *
 * The token is supplied at session registration.  It is persisted in
 * browser localStorage (key ``globus_tokens``, per edge) for convenience.
 */

export const name = 'globus';

// Deliver transfer_status notifications keyed by task_id.
export const notificationConfig = { topic: 'transfer_status', idField: 'task_id' };

const LS_KEY = 'globus_tokens';

let escHtml = s => String(s || '');

// Per-edge state: { sid, tasks: { task_id: {status, label} } }
const state = {};

// ---------------------------------------------------------------------------
//  Template / CSS
// ---------------------------------------------------------------------------

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">🌐</div>
      <h2>Globus Transfer — <span class="edge-label"></span></h2>
      <span class="globus-conn-state" style="margin-left:auto; font-size:0.85rem; color:var(--muted);">not connected</span>
    </div>

    <div class="card">
      <div class="card-title">🔑 Authentication</div>
      <p style="color:var(--muted); font-size:0.85rem; margin-top:0;">
        Supply a Globus Transfer <b>access token</b>, or a <b>refresh token + client ID</b>
        (auto-renews for long transfers).  Acquire one with <code>get_globus_token.py</code>.
      </p>
      <div class="form-group">
        <label>Access token</label>
        <input class="globus-access-token" type="password" placeholder="paste access token" autocomplete="off" />
      </div>
      <div class="grid2">
        <div class="form-group">
          <label>Refresh token</label>
          <input class="globus-refresh-token" type="password" placeholder="optional" autocomplete="off" />
        </div>
        <div class="form-group">
          <label>Client ID</label>
          <input class="globus-client-id" type="text" placeholder="required with refresh token" autocomplete="off" />
        </div>
      </div>
      <div class="form-group">
        <label>Local collection UUID (optional — overrides edge default; use "local" in forms)</label>
        <input class="globus-local-collection" type="text" placeholder="UUID of this site's collection" />
      </div>
      <button class="btn btn-success" data-action="connect">🔌 Connect</button>
      <label style="display:inline-flex; align-items:center; gap:6px; margin-left:12px; font-size:0.85rem;">
        <input type="checkbox" class="globus-remember" checked /> remember in browser
      </label>
      <div class="globus-conn-status" style="margin-top:8px; font-size:0.85rem;"></div>
    </div>

    <div class="card">
      <div class="card-title">📂 Browse Collection</div>
      <div style="display:flex; gap:8px; margin-bottom:12px; align-items:center; flex-wrap:wrap;">
        <input class="globus-ls-collection" type="text" placeholder="collection UUID or 'local'" style="flex:1; min-width:180px;" />
        <input class="globus-ls-path" type="text" placeholder="/path" value="/~/" style="flex:1; min-width:120px;" />
        <button class="btn btn-secondary btn-sm" data-action="browse">📂 Browse</button>
      </div>
      <div class="globus-browser" style="border:1px solid var(--border); border-radius:var(--card-r); max-height:340px; overflow-y:auto; background:var(--bg1);">
        <div class="empty" style="padding:20px;"><div class="empty-icon">📂</div>
          <p>Enter a collection UUID and path, then Browse</p></div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">🚀 Submit Transfer</div>
      <div class="grid2">
        <div class="form-group">
          <label>Source collection (UUID or 'local')</label>
          <input class="globus-src-coll" type="text" placeholder="source UUID" />
        </div>
        <div class="form-group">
          <label>Destination collection (UUID or 'local')</label>
          <input class="globus-dst-coll" type="text" placeholder="destination UUID" />
        </div>
        <div class="form-group">
          <label>Source path</label>
          <input class="globus-src-path" type="text" placeholder="/src/file_or_dir" />
        </div>
        <div class="form-group">
          <label>Destination path</label>
          <input class="globus-dst-path" type="text" placeholder="/dst/file_or_dir" />
        </div>
      </div>
      <div style="display:flex; gap:16px; align-items:center; margin-bottom:8px; flex-wrap:wrap;">
        <label style="display:inline-flex; align-items:center; gap:6px; margin:0;">
          <input type="checkbox" class="globus-recursive" /> recursive (directory)
        </label>
        <input class="globus-label" type="text" placeholder="label (optional)" style="flex:1; min-width:140px;" />
        <select class="globus-sync" style="min-width:120px;">
          <option value="">sync: none</option>
          <option value="exists">exists</option>
          <option value="size">size</option>
          <option value="mtime">mtime</option>
          <option value="checksum">checksum</option>
        </select>
      </div>
      <button class="btn btn-primary" data-action="submit">🚀 Submit Transfer</button>
      <div class="globus-submit-status" style="margin-top:8px; font-size:0.85rem;"></div>
    </div>

    <div class="card">
      <div class="card-title">📋 Transfers
        <button class="btn btn-secondary btn-sm" data-action="refresh-tasks" style="margin-left:12px;">↺ Refresh</button>
      </div>
      <div class="globus-tasks">
        <div class="empty" style="padding:16px;"><p style="color:var(--muted);">No transfers submitted yet</p></div>
      </div>
    </div>
  `;
}

export function css() {
  return `
    .globus-tasks table { width:100%; border-collapse:collapse; font-size:0.85rem; }
    .globus-tasks th, .globus-tasks td { padding:6px 10px; text-align:left; border-bottom:1px solid var(--border); }
    .globus-status-SUCCEEDED { color:var(--success); font-weight:600; }
    .globus-status-FAILED    { color:var(--danger);  font-weight:600; }
    .globus-status-ACTIVE    { color:var(--accent); }
    .globus-browser .row { display:flex; align-items:center; gap:8px; padding:6px 12px; }
    .globus-browser .row:hover { background:var(--hover); }
  `;
}

// ---------------------------------------------------------------------------
//  Init / event wiring
// ---------------------------------------------------------------------------

export function init(page, api) {
  escHtml = api.escHtml;
  const edge = api.edgeName;
  if (!state[edge]) state[edge] = { sid: null, tasks: {} };

  // Prefill auth fields from localStorage.
  const stored = loadTokens()[edge];
  if (stored) {
    if (stored.access_token)     page.querySelector('.globus-access-token').value     = stored.access_token;
    if (stored.refresh_token)    page.querySelector('.globus-refresh-token').value    = stored.refresh_token;
    if (stored.client_id)        page.querySelector('.globus-client-id').value        = stored.client_id;
    if (stored.local_collection) page.querySelector('.globus-local-collection').value = stored.local_collection;
  }

  page.querySelector('[data-action="connect"]')?.addEventListener('click', () => connect(page, api));
  page.querySelector('[data-action="browse"]')?.addEventListener('click', () => browse(page, api));
  page.querySelector('[data-action="submit"]')?.addEventListener('click', () => submit(page, api));
  page.querySelector('[data-action="refresh-tasks"]')?.addEventListener('click', () => refreshTasks(page, api));

  renderTasks(page, api);

  // Auto-connect if a token is already stored.
  if (stored && (stored.access_token || (stored.refresh_token && stored.client_id))) {
    connect(page, api, /*silent=*/true);
  }
}

export function onNotification(data, page, api) {
  if (data.topic !== 'transfer_status') return;
  const edge = api.edgeName;
  const d    = data.data || {};
  if (!d.task_id) return;
  const st = state[edge] || (state[edge] = { sid: null, tasks: {} });
  st.tasks[d.task_id] = {
    status: d.status || '?',
    label : d.label || (st.tasks[d.task_id] && st.tasks[d.task_id].label) || '',
    nice  : d.nice_status || '',
  };
  renderTasks(page, api);
}

// ---------------------------------------------------------------------------
//  Auth / session
// ---------------------------------------------------------------------------

function loadTokens() {
  try { return JSON.parse(localStorage.getItem(LS_KEY) || '{}'); }
  catch (e) { return {}; }
}

function saveTokens(edge, payload) {
  const all = loadTokens();
  all[edge] = payload;
  localStorage.setItem(LS_KEY, JSON.stringify(all));
}

function authPayload(page) {
  const access  = page.querySelector('.globus-access-token').value.trim();
  const refresh = page.querySelector('.globus-refresh-token').value.trim();
  const cid     = page.querySelector('.globus-client-id').value.trim();
  const local   = page.querySelector('.globus-local-collection').value.trim();

  const payload = {};
  if (access)        payload.access_token  = access;
  if (refresh)       payload.refresh_token = refresh;
  if (cid)           payload.client_id     = cid;
  if (local)         payload.local_collection = local;
  return payload;
}

async function connect(page, api, silent = false) {
  const edge   = api.edgeName;
  const status = page.querySelector('.globus-conn-status');
  const label  = page.querySelector('.globus-conn-state');
  const payload = authPayload(page);

  if (!payload.access_token && !(payload.refresh_token && payload.client_id)) {
    if (!silent) {
      status.innerHTML = '<span style="color:var(--danger);">Provide an access token, or a refresh token + client ID.</span>';
      api.flash('Globus: missing credential', false);
    }
    return;
  }

  status.innerHTML = '<span style="color:var(--accent);">Connecting…</span>';
  try {
    // Register a fresh session carrying the token (bypass the shared cache
    // so re-connecting with a new token always re-registers).
    const data = await api.fetch('register_session', {
      method: 'POST',
      body  : JSON.stringify(payload),
    });
    state[edge].sid = data.sid;

    if (page.querySelector('.globus-remember').checked) saveTokens(edge, payload);

    status.innerHTML = '<span style="color:var(--success);">Connected.</span>';
    label.textContent = 'connected';
    label.style.color = 'var(--success)';
    if (!silent) api.flash('Globus connected');
  } catch (e) {
    state[edge].sid = null;
    label.textContent = 'not connected';
    label.style.color = 'var(--muted)';
    status.innerHTML = `<span style="color:var(--danger);">Error: ${escHtml(e.message)}</span>`;
    if (!silent) api.flash('Globus connect failed: ' + e.message, false);
  }
}

function sidOrThrow(page, api) {
  const sid = state[api.edgeName]?.sid;
  if (!sid) throw new Error('not connected — Connect with a Globus token first');
  return sid;
}

// ---------------------------------------------------------------------------
//  Browse
// ---------------------------------------------------------------------------

async function browse(page, api) {
  const browser    = page.querySelector('.globus-browser');
  const collection = page.querySelector('.globus-ls-collection').value.trim() || 'local';
  const path       = page.querySelector('.globus-ls-path').value.trim() || null;

  browser.innerHTML = '<div class="empty" style="padding:20px;"><div class="spinner"></div><p>Loading…</p></div>';
  try {
    const sid  = sidOrThrow(page, api);
    const data = await api.fetch(`ls/${sid}`, {
      method: 'POST',
      body  : JSON.stringify({ collection, path }),
    });
    renderBrowser(page, data);
  } catch (e) {
    browser.innerHTML = `<div class="empty" style="padding:20px;"><p style="color:var(--danger);">Error: ${escHtml(e.message)}</p></div>`;
    api.flash('Globus ls error: ' + e.message, false);
  }
}

function renderBrowser(page, data) {
  const browser = page.querySelector('.globus-browser');
  const entries = data.entries || [];
  if (!entries.length) {
    browser.innerHTML = '<div class="empty" style="padding:20px;"><p style="color:var(--muted);">Empty directory</p></div>';
    return;
  }
  const sorted = [...entries].sort((a, b) => {
    const ad = a.type === 'dir', bd = b.type === 'dir';
    if (ad !== bd) return ad ? -1 : 1;
    return String(a.name).localeCompare(String(b.name));
  });
  let html = '';
  for (const e of sorted) {
    const isDir = e.type === 'dir';
    const size  = (!isDir && e.size != null) ? formatBytes(e.size) : '';
    html += `<div class="row">
      <span>${isDir ? '📁' : '📄'}</span>
      <span style="flex:1;">${escHtml(e.name)}</span>
      <span style="color:var(--muted); font-size:0.8rem;">${isDir ? 'directory' : size}</span>
    </div>`;
  }
  browser.innerHTML = html;
}

// ---------------------------------------------------------------------------
//  Submit transfer
// ---------------------------------------------------------------------------

async function submit(page, api) {
  const status  = page.querySelector('.globus-submit-status');
  const src     = page.querySelector('.globus-src-coll').value.trim();
  const dst     = page.querySelector('.globus-dst-coll').value.trim();
  const srcPath = page.querySelector('.globus-src-path').value.trim();
  const dstPath = page.querySelector('.globus-dst-path').value.trim();
  const recur   = page.querySelector('.globus-recursive').checked;
  const label   = page.querySelector('.globus-label').value.trim();
  const sync    = page.querySelector('.globus-sync').value || null;

  if (!src || !dst || !srcPath || !dstPath) {
    api.flash('Globus: source/destination collection and path are required', false);
    return;
  }

  status.innerHTML = '<span style="color:var(--accent);">Submitting…</span>';
  try {
    const sid  = sidOrThrow(page, api);
    const body = {
      source: src, destination: dst,
      items: [{ source: srcPath, destination: dstPath, recursive: recur }],
      label: label || null, sync_level: sync,
    };
    const data = await api.fetch(`submit/${sid}`, { method: 'POST', body: JSON.stringify(body) });

    const edge = api.edgeName;
    state[edge].tasks[data.task_id] = { status: data.status || 'ACTIVE', label: label, nice: '' };
    if (api.registerTask) api.registerTask('globus', data.task_id, label || data.task_id);
    renderTasks(page, api);

    status.innerHTML = `<span style="color:var(--success);">Submitted task ${escHtml(data.task_id)}</span>`;
    api.flash('Globus transfer submitted');
  } catch (e) {
    status.innerHTML = `<span style="color:var(--danger);">Error: ${escHtml(e.message)}</span>`;
    api.flash('Globus submit failed: ' + e.message, false);
  }
}

// ---------------------------------------------------------------------------
//  Tasks monitor
// ---------------------------------------------------------------------------

async function refreshTasks(page, api) {
  const edge = api.edgeName;
  const st   = state[edge];
  if (!st || !st.sid) { api.flash('Connect first', false); return; }
  await Promise.all(Object.keys(st.tasks).map(async tid => {
    try {
      const data = await api.fetch(`task/${st.sid}/${tid}`);
      st.tasks[tid].status = data.status || st.tasks[tid].status;
      st.tasks[tid].nice   = data.nice_status || '';
    } catch (e) { /* leave as-is */ }
  }));
  renderTasks(page, api);
}

async function cancelTask(page, api, tid) {
  const st = state[api.edgeName];
  if (!st || !st.sid) return;
  try {
    await api.fetch(`cancel/${st.sid}/${tid}`, { method: 'POST' });
    st.tasks[tid].status = 'CANCELED';
    renderTasks(page, api);
    api.flash('Globus transfer cancelled');
  } catch (e) {
    api.flash('Cancel failed: ' + e.message, false);
  }
}

function renderTasks(page, api) {
  const area = page.querySelector('.globus-tasks');
  if (!area) return;
  const edge  = api ? api.edgeName : null;
  const st    = (edge && state[edge]) || { tasks: {} };
  const tasks = Object.entries(st.tasks);
  if (!tasks.length) {
    area.innerHTML = '<div class="empty" style="padding:16px;"><p style="color:var(--muted);">No transfers submitted yet</p></div>';
    return;
  }
  let html = '<table><thead><tr><th>Task</th><th>Label</th><th>Status</th><th></th></tr></thead><tbody>';
  for (const [tid, t] of tasks) {
    const term = t.status === 'SUCCEEDED' || t.status === 'FAILED' || t.status === 'CANCELED' || t.status === 'CANCELLED';
    html += `<tr>
      <td><code style="font-size:0.78rem;">${escHtml(tid)}</code></td>
      <td>${escHtml(t.label || '')}</td>
      <td class="globus-status-${escHtml(t.status)}">${escHtml(t.status)}${t.nice ? ' (' + escHtml(t.nice) + ')' : ''}</td>
      <td>${term ? '' : `<button class="btn btn-secondary btn-sm" data-cancel="${escHtml(tid)}">❌</button>`}</td>
    </tr>`;
  }
  html += '</tbody></table>';
  area.innerHTML = html;
  if (api) {
    area.querySelectorAll('[data-cancel]').forEach(btn =>
      btn.addEventListener('click', () => cancelTask(page, api, btn.dataset.cancel)));
  }
}

// ---------------------------------------------------------------------------
//  Utilities
// ---------------------------------------------------------------------------

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  if (bytes == null) return '';
  const k = 1024, sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}
