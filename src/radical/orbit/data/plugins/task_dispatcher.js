/**
 * Task Dispatcher Plugin Module for ORBIT Explorer
 *
 * Shows the dispatcher's configured pools at a glance: queue/account,
 * strategy, pilot sizes (nodes/cpus/gpus/walltime/rhapsody backend),
 * min/max pilots, live pilot count, pending task count.
 *
 * Default view fetches GET /pools, which answers pools *grouped by owning
 * session*: `{pools: {"<sid>": {"<pool name>": <summary>}}}`.  Each pool of
 * each session gets its own card, tagged with its session.  Class pools
 * (`multi_member`) are then refreshed one by one from the verbose
 * `GET /pool/{sid}/{name}` route, which is the only place per-member pilot
 * sizes and node-hours come from.
 */

export const name = 'task_dispatcher';

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">📦</div>
      <h2>Task Dispatcher — <span class="endpoint-label"></span></h2>
      <span class="td-summary" title="Pool count">…</span>
      <button class="btn btn-secondary btn-sm" style="margin-left:auto" data-action="refresh">↺ Refresh</button>
    </div>
    <div class="td-content">
      <div class="empty">
        <div class="empty-icon">⏳</div>
        <p>Loading pools…</p>
      </div>
    </div>
  `;
}

export function css() {
  return `
    .td-summary {
      margin-left: 12px;
      padding: 2px 10px;
      border-radius: 12px;
      font-size: 0.78rem;
      font-weight: 600;
      background: var(--bg2);
      color: var(--muted);
      border: 1px solid var(--border, #ccc);
    }
    .td-pool-header {
      display: flex;
      align-items: baseline;
      gap: 12px;
      margin-bottom: 8px;
    }
    .td-pool-name {
      font-size: 1.05rem;
      font-weight: 600;
    }
    .td-strategy-badge {
      padding: 2px 8px;
      border-radius: 10px;
      font-size: 0.72rem;
      background: var(--bg2);
      color: var(--muted);
      border: 1px solid var(--border, #ccc);
    }
    .td-pilot-count {
      margin-left: auto;
      font-size: 0.85rem;
      color: var(--muted);
    }
    .td-pool-meta {
      font-size: 0.85rem;
      color: var(--muted);
      margin-bottom: 8px;
    }
    .td-pool-meta strong { color: var(--text, #333); font-weight: 500; }
    .td-sizes-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.85rem;
    }
    .td-sizes-table th,
    .td-sizes-table td {
      padding: 4px 8px;
      text-align: left;
      border-bottom: 1px solid var(--border, #eee);
    }
    .td-sizes-table th {
      font-weight: 500;
      color: var(--muted);
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.3px;
    }
    .td-sizes-table tr.td-default-size td { background: var(--bg2); font-weight: 500; }
    /* a member's own size table, nested one row below its member row */
    .td-member-sizes > td {
      padding: 0 0 8px 18px;
      border-bottom: 1px solid var(--border, #e5e5e5);
      background: var(--bg2);
    }
    .td-member-sizes .td-sizes-table { margin: 0; font-size: 0.78rem; }
    .td-empty-pools {
      padding: 12px;
      color: var(--muted);
      font-style: italic;
    }
  `;
}

export async function init(page, api) {
  page.querySelector('[data-action="refresh"]')
      .addEventListener('click', () => loadPools(page, api));
  await loadPools(page, api);
}

export async function onShow(page, api) {
  await loadPools(page, api);
}

export function onNotification() {}

async function loadPools(page, api) {
  const content = page.querySelector('.td-content');
  const summary = page.querySelector('.td-summary');
  try {
    const r = await api.fetch('pools');
    const entries = flattenPools(r.pools || {});
    const n = entries.length;
    summary.textContent = `${n} pool${n === 1 ? '' : 's'}`;
    content.innerHTML = renderEntries(entries, api);
    await refreshClassPools(page, entries, api);
  } catch (e) {
    summary.textContent = '?';
    content.innerHTML =
      `<div class="card"><p style="color:var(--danger)">Error: ${api.escHtml(e.message)}</p></div>`;
    api.flash('Task dispatcher: ' + e.message, false);
  }
}

// `GET /pools` groups pools by owning session: the top level maps a sid to
// that session's pools.  Flatten it into `{sid, pool}` entries so one card
// is rendered per *pool* (not per session).
function flattenPools(pools) {
  const entries = [];
  Object.keys(pools || {}).forEach(key => {
    const v = pools[key];
    if (!v || typeof v !== 'object') return;
    Object.keys(v).forEach(name => {
      const p = v[name];
      if (!p || typeof p !== 'object') return;
      entries.push({sid: key, pool: p});
    });
  });
  return entries;
}

function renderEntries(entries, api) {
  if (entries.length === 0) {
    return `<div class="card td-empty-pools">No pools configured.</div>`;
  }

  return entries.map((e, i) => renderPoolCard(e.pool, api, e.sid, i)).join('');
}

// Per-member pilot sizes and node-hours live only in the verbose per-pool
// route, so each class pool's card is re-rendered from `pool/{sid}/{name}`
// once the (cheap) grouped listing is on screen.  A failing fetch leaves the
// non-verbose card in place; this never throws.
async function refreshClassPools(page, entries, api) {
  for (let i = 0; i < entries.length; i++) {
    const {sid, pool} = entries[i];
    if (!sid || !pool.multi_member || !pool.name) continue;
    try {
      const v = await api.fetch(`pool/${encodeURIComponent(sid)}`
                              + `/${encodeURIComponent(pool.name)}`);
      if (!v || typeof v !== 'object') continue;
      const card = page.querySelector(`[data-td-card="${i}"]`);
      if (card) card.outerHTML = renderPoolCard(v, api, sid, i);
    } catch (e) {
      // keep the non-verbose card
    }
  }
}

function sizeRows(sizes, defaultSize, api) {
  return Object.keys(sizes || {}).map(sn => {
    const s = sizes[sn];
    const cls = (sn === defaultSize) ? 'td-default-size' : '';
    const walltime = formatWalltime(s.walltime_sec || 0);
    return `<tr class="${cls}">
      <td>${api.escHtml(sn)}${sn === defaultSize ? ' <span style="color:var(--muted);font-size:.7rem">(default)</span>' : ''}</td>
      <td>${s.nodes ?? '?'}</td>
      <td>${s.cpus_per_node ?? '?'}</td>
      <td>${s.gpus_per_node || 0}</td>
      <td>${walltime}</td>
      <td><code style="font-size:.78rem">${api.escHtml(s.rhapsody_backend || '?')}</code></td>
    </tr>`;
  }).join('');
}

function sizesTable(sizes, defaultSize, api) {
  const rows = sizeRows(sizes, defaultSize, api);
  return `<table class="td-sizes-table">
        <thead>
          <tr><th>size</th><th>nodes</th><th>cpus/node</th><th>gpus/node</th><th>walltime</th><th>backend</th></tr>
        </thead>
        <tbody>${rows || '<tr><td colspan="6" class="td-empty-pools">No sizes defined.</td></tr>'}</tbody>
      </table>`;
}

// Attribute chips: `site=NERSC`, `software: lammps, pytorch`.
function attrChips(attrs, api) {
  const keys = Object.keys(attrs || {});
  if (keys.length === 0) return '<em style="color:var(--muted)">none</em>';
  return keys.map(k => {
    const v = attrs[k];
    const txt = Array.isArray(v) ? `${k}: ${v.join(', ')}` : `${k}=${v}`;
    return `<code style="font-size:.72rem;margin-right:.35rem">${api.escHtml(txt)}</code>`;
  }).join('');
}

function nodeHours(m) {
  const used = (typeof m.node_hours_used === 'number')
    ? m.node_hours_used.toFixed(2) : '?';
  const left = (typeof m.node_hours_remaining === 'number')
    ? m.node_hours_remaining.toFixed(2) : '∞';
  return `${used} / ${left}`;
}

// Multi-member pools render a members table; each member row is followed
// by a nested row carrying that member's own size table, reusing the same
// renderer as a single-site pool.  Per-member node-hours and pilot_sizes
// come from the verbose `pool/{sid}/{name}` route, so a non-verbose
// `pools` listing shows '?' and omits the nested row until the card is
// opened.
function membersTable(p, api) {
  const members = p.members || (p.member_ids || []).map(id => ({member_id: id}));
  const rows = members.map(m => {
    const account = m.account ? api.escHtml(m.account)
                              : '<em style="color:var(--muted)">none</em>';
    const detail = m.pilot_sizes
      ? `<tr class="td-member-sizes"><td colspan="7">${sizesTable(m.pilot_sizes, m.default_size, api)}</td></tr>`
      : '';
    return `<tr>
      <td><strong>${api.escHtml(m.member_id || '?')}</strong></td>
      <td><code style="font-size:.78rem">${api.escHtml(m.endpoint_name || '?')}</code></td>
      <td>${api.escHtml(m.queue || '?')}</td>
      <td>${account}</td>
      <td>${attrChips(m.attributes, api)}</td>
      <td>${m.live_pilots ?? '?'} / ${m.max_pilots ?? '?'}</td>
      <td>${nodeHours(m)}</td>
    </tr>${detail}`;
  }).join('');

  return `<table class="td-sizes-table">
        <thead>
          <tr><th>member</th><th>endpoint</th><th>queue</th><th>account</th>
              <th>attributes</th><th>pilots live/max</th><th>node-hours used/left</th></tr>
        </thead>
        <tbody>${rows || '<tr><td colspan="7" class="td-empty-pools">No members declared.</td></tr>'}</tbody>
      </table>`;
}

function renderPoolCard(p, api, sid, idx) {
  const account = p.account ? api.escHtml(p.account) : '<em style="color:var(--muted)">none</em>';
  const classBadge = p.pool_class
    ? `<span class="td-strategy-badge">${api.escHtml(p.pool_class)}</span>` : '';
  // pools are session-scoped: say which session owns this one
  const sessionBadge = sid
    ? `<span class="td-strategy-badge">session ${api.escHtml(sid)}</span>` : '';
  // A class pool's ceiling is the sum over its members; a legacy pool's is
  // its own max_pilots, exactly as before.
  const maxPilots = p.multi_member ? (p.max_pilots_total ?? '?')
                                   : (p.max_pilots ?? '?');

  const body = p.multi_member
    ? membersTable(p, api)
    : `<div class="td-pool-meta">
        <strong>queue</strong>: ${api.escHtml(p.queue || '?')}
        &nbsp; <strong>account</strong>: ${account}
        &nbsp; <strong>min/max pilots</strong>: ${p.min_pilots ?? 0} / ${p.max_pilots ?? '?'}
      </div>
      ${sizesTable(p.pilot_sizes || {}, p.default_size, api)}`;

  return `
    <div class="card" data-td-card="${idx ?? 0}">
      <div class="td-pool-header">
        <span class="td-pool-name">${api.escHtml(p.name)}</span>
        ${sessionBadge}
        <span class="td-strategy-badge">${api.escHtml(p.strategy || 'conservative')}</span>
        ${classBadge}
        <span class="td-pilot-count">
          ${p.live_pilots ?? 0} / ${maxPilots} pilot${(maxPilots === 1) ? '' : 's'} live
          · ${p.pending_tasks ?? 0} pending task${(p.pending_tasks === 1) ? '' : 's'}
        </span>
      </div>
      ${body}
    </div>`;
}

function formatWalltime(secs) {
  if (!secs) return '?';
  if (secs >= 3600 && secs % 3600 === 0) return `${secs / 3600}h`;
  if (secs >= 3600) return `${(secs / 3600).toFixed(1)}h`;
  if (secs >= 60) return `${Math.round(secs / 60)}m`;
  return `${secs}s`;
}
