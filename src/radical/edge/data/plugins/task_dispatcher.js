/**
 * Task Dispatcher Plugin Module for Radical Edge Explorer
 *
 * Shows the dispatcher's configured pools at a glance: queue/account,
 * strategy, pilot sizes (nodes/cpus/gpus/walltime/rhapsody backend),
 * min/max pilots, live pilot count, pending task count.
 *
 * Default view fetches GET /pools (session-less); per-pool refresh
 * could later be extended to GET /pool/{name} for live pilot details.
 */

export const name = 'task_dispatcher';

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">📦</div>
      <h2>Task Dispatcher — <span class="edge-label"></span></h2>
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
    const pools = r.pools || {};
    const names = Object.keys(pools);
    summary.textContent = `${names.length} pool${names.length === 1 ? '' : 's'}`;
    content.innerHTML = renderPools(pools, api);
  } catch (e) {
    summary.textContent = '?';
    content.innerHTML =
      `<div class="card"><p style="color:var(--danger)">Error: ${api.escHtml(e.message)}</p></div>`;
    api.flash('Task dispatcher: ' + e.message, false);
  }
}

function renderPools(pools, api) {
  const names = Object.keys(pools);
  if (names.length === 0) {
    return `<div class="card td-empty-pools">No pools configured.</div>`;
  }

  return names.map(n => renderPoolCard(pools[n], api)).join('');
}

function renderPoolCard(p, api) {
  const sizes = p.pilot_sizes || {};
  const sizeNames = Object.keys(sizes);
  const defaultSize = p.default_size;

  const sizeRows = sizeNames.map(sn => {
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

  const account = p.account ? api.escHtml(p.account) : '<em style="color:var(--muted)">none</em>';

  return `
    <div class="card">
      <div class="td-pool-header">
        <span class="td-pool-name">${api.escHtml(p.name)}</span>
        <span class="td-strategy-badge">${api.escHtml(p.strategy || 'conservative')}</span>
        <span class="td-pilot-count">
          ${p.live_pilots ?? 0} / ${p.max_pilots ?? '?'} pilot${(p.max_pilots === 1) ? '' : 's'} live
          · ${p.pending_tasks ?? 0} pending task${(p.pending_tasks === 1) ? '' : 's'}
        </span>
      </div>
      <div class="td-pool-meta">
        <strong>queue</strong>: ${api.escHtml(p.queue || '?')}
        &nbsp; <strong>account</strong>: ${account}
        &nbsp; <strong>min/max pilots</strong>: ${p.min_pilots ?? 0} / ${p.max_pilots ?? '?'}
      </div>
      <table class="td-sizes-table">
        <thead>
          <tr><th>size</th><th>nodes</th><th>cpus/node</th><th>gpus/node</th><th>walltime</th><th>backend</th></tr>
        </thead>
        <tbody>${sizeRows || '<tr><td colspan="6" class="td-empty-pools">No sizes defined.</td></tr>'}</tbody>
      </table>
    </div>`;
}

function formatWalltime(secs) {
  if (!secs) return '?';
  if (secs >= 3600 && secs % 3600 === 0) return `${secs / 3600}h`;
  if (secs >= 3600) return `${(secs / 3600).toFixed(1)}h`;
  if (secs >= 60) return `${Math.round(secs / 60)}m`;
  return `${secs}s`;
}
