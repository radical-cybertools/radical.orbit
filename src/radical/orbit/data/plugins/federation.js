/**
 * Federation Plugin Module for ORBIT Explorer
 *
 * One table of joined resources, each followed by one indented **pilot row**
 * per shape that resource offers.  Two independent column sets share the
 * table: the resource row says what the machine is (site, software, the
 * capability classes it lands in) and how much work it holds; the pilot rows
 * say what one pilot of that shape looks like and how long it has left.
 *
 * The word "member" is the wire field, not a label: a reader sees the
 * endpoint that runs the pilot — `ep_odo` in allocation mode, where the
 * endpoint *is* the pilot, and `ep_perlmutter/gpu` in login mode, where the
 * endpoint submits one pilot per shape.
 *
 * Polls `GET resources/default` every 3 s — the plugin refreshes usage on
 * that call (server-side cache: 2 s), so the page needs no other state.
 *
 * A GPU in a pilot's size is what the operator **declared**, not a device
 * reserved for a task: pilot capacity is task-count based.
 *
 * The state column shows the record's derived `state` (`ok` / `idle` /
 * `stale` / `suspect` / `lost` / `failing`); the resource row carries the
 * worst of its pilot rows'.  A `failing` row — reachable, but its pilots
 * die at submit — gets a red badge and one monospace line underneath
 * carrying what psij said.
 *
 * Node-hours moved into the pilot row's tooltip: the table answers "how long
 * has this got left", the tooltip answers "what has it spent".
 *
 * All federation routes ride the reserved `default` session, so this module
 * never registers one of its own.
 */

export const name = 'federation';

const POLL_MS = 3000;

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">🌐</div>
      <h2>Federation — <span class="endpoint-label"></span></h2>
      <span class="fed-summary" title="Resource count">…</span>
      <button class="btn btn-secondary btn-sm" style="margin-left:auto" data-action="refresh">↺ Refresh</button>
    </div>
    <div class="fed-content">
      <div class="empty">
        <div class="empty-icon">⏳</div>
        <p>Loading resources…</p>
      </div>
    </div>
  `;
}

export function css() {
  return `
    .fed-summary {
      margin-left: 12px;
      padding: 2px 10px;
      border-radius: 12px;
      font-size: 0.78rem;
      font-weight: 600;
      background: var(--bg2);
      color: var(--muted);
      border: 1px solid var(--border, #ccc);
    }
    .fed-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.85rem;
    }
    .fed-table th,
    .fed-table td {
      padding: 5px 8px;
      text-align: left;
      border-bottom: 1px solid var(--border, #eee);
      white-space: nowrap;
    }
    .fed-table th {
      font-weight: 500;
      color: var(--muted);
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.3px;
    }
    .fed-name { font-weight: 600; }
    .fed-badge {
      padding: 2px 8px;
      border-radius: 10px;
      font-size: 0.72rem;
      background: var(--bg2);
      color: var(--muted);
      border: 1px solid var(--border, #ccc);
    }
    .fed-live-ok      { color: var(--success, #2e7d32); font-weight: 600; }
    .fed-live-suspect { color: var(--warning, #b26a00); font-weight: 600; }
    .fed-live-lost    { color: var(--danger,  #c62828); font-weight: 600; }
    /* reachable and blameless, just holding no pilot right now */
    .fed-live-idle    { color: var(--muted); font-weight: 600; }
    .fed-live-stale   { color: var(--muted); font-style: italic; }
    /* reachable, but nothing it is asked to start survives */
    .fed-live-failing {
      display: inline-block;
      padding: 1px 7px;
      border-radius: 9px;
      font-size: 0.72rem;
      font-weight: 600;
      color: #fff;
      background: var(--danger, #c62828);
    }
    /* the psij/batch-system reason, under the member row that owns it.
       The text is truncated in JS (the full reason is the title), so the
       cell only has to stay on one line. */
    .fed-error td {
      padding: 0 8px 6px 26px;
      border-bottom: 1px solid var(--border, #eee);
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 0.72rem;
      color: var(--danger, #c62828);
    }
    .fed-soft { color: var(--muted); font-size: 0.78rem; white-space: normal; }
    .fed-num  { text-align: right; font-variant-numeric: tabular-nums; }
    .fed-pilot td { padding-left: 18px; color: var(--muted); }
    .fed-pilot td:first-child { padding-left: 26px; }
    .fed-empty {
      padding: 12px;
      color: var(--muted);
      font-style: italic;
    }
  `;
}

export async function init(page, api) {
  page.querySelector('[data-action="refresh"]')
      .addEventListener('click', () => loadResources(page, api));
  startPolling(page, api);
  await loadResources(page, api);
}

export async function onShow(page, api) {
  startPolling(page, api);
  await loadResources(page, api);
}

export function onNotification() {}

/* Poll while the page is in the DOM; the timer stops itself once it is not. */
function startPolling(page, api) {
  if (page._fedTimer) return;
  page._fedTimer = setInterval(() => {
    if (!page.isConnected) {
      clearInterval(page._fedTimer);
      page._fedTimer = null;
      return;
    }
    loadResources(page, api);
  }, POLL_MS);
}

async function loadResources(page, api) {
  const content = page.querySelector('.fed-content');
  const summary = page.querySelector('.fed-summary');
  if (!content) return;
  try {
    const r = await api.fetch('resources/default');
    const resources = r.resources || [];
    summary.textContent =
      `${resources.length} resource${resources.length === 1 ? '' : 's'}`;
    content.innerHTML = renderTable(resources, api);
  } catch (e) {
    summary.textContent = '?';
    content.innerHTML =
      `<div class="card"><p style="color:var(--danger)">Error: ${api.escHtml(e.message)}</p></div>`;
    api.flash('Federation: ' + e.message, false);
  }
}

function renderTable(resources, api) {
  if (resources.length === 0) {
    return `<div class="card fed-empty">No resources joined.</div>`;
  }
  const rows = resources.map(r => {
    return renderResourceRow(r, r.members, api)
         + r.members.map(m => renderPilotRow(r, m, api)
                          + renderPilotError(m, api)).join('');
  }).join('');
  return `
    <div class="card">
      <table class="fed-table">
        <thead>
          <tr>
            <th>resource / pilot</th><th>mode</th>
            <th>nodes</th><th>cpn</th><th>gpn</th><th>mpn</th>
            <th>runtime</th><th>left</th>
            <th>run</th><th>done</th><th>failed</th><th>state</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

/* The resource row: what the machine is, which capability classes its
 * pilots land in, and the work it holds.
 *
 * The task counts are the **record's own**, never a sum over the pilot
 * rows: a task the dispatcher has not placed yet belongs to no shape and
 * would vanish from that sum.  `state` is the derived word the server
 * already folded (worst of the pilot rows). */
function renderResourceRow(r, members, api) {
  const caps  = r.capabilities || {};
  const usage = r.usage || {};
  const soft  = (caps.software || []).map(s => api.escHtml(s)).join(', ');
  const live  = r.state;
  const cls   = [...new Set(members.map(m => m.pool_name).filter(Boolean))]
                .map(p => `<span class="fed-badge">${api.escHtml(p)}</span>`)
                .join(' ');

  return `<tr>
    <td class="fed-name">${api.escHtml(r.name || '?')}</td>
    <td>${api.escHtml(r.site || '—')}</td>
    <td class="fed-soft" colspan="4">${soft || '—'}</td>
    <td colspan="2">${cls || '—'}</td>
    <td class="fed-num">${usage.tasks_running ?? 0}</td>
    <td class="fed-num">${usage.tasks_done ?? 0}</td>
    <td class="fed-num">${usage.tasks_failed ?? 0}</td>
    <td class="fed-live-${api.escHtml(live)}">${api.escHtml(live)}</td>
  </tr>`;
}

/* One indented row per pilot shape, named after the endpoint that runs it:
 * in allocation mode the endpoint *is* the pilot, so the row is the
 * endpoint; in login mode the endpoint submits one pilot per shape, so the
 * row is `<endpoint>/<shape>`.
 *
 * `left` is what this shape has until its allocation ends — `-` when the
 * broker cannot know (no live pilot, no allocation end).
 * Node-hours ride in the row tooltip. */
function renderPilotRow(r, m, api) {
  const usage = m.usage || {};
  const attrs = m.attributes || {};
  const caps  = r.capabilities || {};
  const live  = m.state;
  const alloc = (r.mode || '') === 'allocation';
  const ep    = m.endpoint || r.endpoint || '?';
  const name  = alloc ? ep : `${ep}/${m.member || '?'}`;
  const mpn   = attrs.mem_gb_per_node ?? caps.mem_gb;

  return `<tr class="fed-pilot" title="${api.escHtml(hoursTip(usage))}">
    <td>└ ${api.escHtml(name)}</td>
    <td><span class="fed-badge">${api.escHtml(alloc ? 'alloc' : 'login')}</span></td>
    <td class="fed-num">${fmtNum(m.nodes)}</td>
    <td class="fed-num">${fmtNum(m.cpus_per_node)}</td>
    <td class="fed-num">${fmtNum(m.gpus_per_node)}</td>
    <td class="fed-num">${fmtNum(mpn)}</td>
    <td class="fed-num">${fmtHours(m.walltime_sec)}</td>
    <td class="fed-num">${fmtHours(m.remaining_sec)}</td>
    <td class="fed-num">${usage.tasks_running ?? 0}</td>
    <td class="fed-num">${usage.tasks_done ?? 0}</td>
    <td class="fed-num">${usage.tasks_failed ?? 0}</td>
    <td class="fed-live-${api.escHtml(live)}">${api.escHtml(live)}</td>
  </tr>`;
}

/* The row under a pilot shape whose pilots are dying: what the batch system
 * or psij actually said, plus how long submissions stay paused.  Nothing at
 * all for a shape with no error — this is the line that was missing when
 * a site failed every submit for half an hour and the table said `ok`. */
function renderPilotError(m, api) {
  const usage = m.usage || {};
  const err   = usage.pilot_error;
  if (!err) return '';
  const text  = String(err);
  const short = text.length > 140 ? text.slice(0, 137) + '…' : text;
  const n     = usage.pilot_failures || 0;
  const until = usage.paused_until
              ? ` (paused until ${new Date(usage.paused_until * 1000)
                                  .toLocaleTimeString()})` : '';
  const count = n > 1 ? ` ×${n}` : '';
  return `<tr class="fed-error">
    <td colspan="12" title="${api.escHtml(text)}">! pilot${
      api.escHtml(count)}: ${api.escHtml(short)}${api.escHtml(until)}</td>
  </tr>`;
}

/* The budget, as the row's tooltip: the table is a live view, node-hours
 * are an accounting one. */
function hoursTip(usage) {
  const used = Number(usage.node_hours_used || 0);
  const left = Number(usage.node_hours_remaining || 0);
  return `node-hours: ${used.toFixed(2)} used · ${left.toFixed(2)} left`
       + (usage.stale ? ' (stale)' : '');
}

/* Seconds as hours with two decimals; `-` for anything unknown, which is
 * what a null `remaining_sec` means: not "zero left", but "nobody said". */
function fmtHours(sec) {
  if (sec === undefined || sec === null) return '-';
  return (Number(sec) / 3600).toFixed(2);
}

function fmtNum(v) {
  return (v === undefined || v === null) ? '-' : String(v);
}
