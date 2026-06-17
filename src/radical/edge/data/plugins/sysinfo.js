/**
 * SysInfo Plugin Module for Radical Edge Explorer
 *
 * Displays system metrics: CPU, memory, disks, network, GPUs.
 */

export const name = 'sysinfo';

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">🖥️</div>
      <h2>System Info — <span class="edge-label"></span></h2>
      <span class="sysinfo-role-badge" title="Host role">…</span>
      <button class="btn btn-secondary btn-sm" style="margin-left:auto" data-action="refresh">↺ Refresh</button>
    </div>
    <div class="sysinfo-content">
      <div class="empty">
        <div class="empty-icon">⏳</div>
        <p>Loading…</p>
      </div>
    </div>
  `;
}

export function css() {
  return `
    .sysinfo-role-badge {
      margin-left: 12px;
      padding: 2px 10px;
      border-radius: 12px;
      font-size: 0.78rem;
      font-weight: 600;
      background: var(--bg2);
      color: var(--muted);
      border: 1px solid var(--border, #ccc);
    }
    .sysinfo-role-badge.role-bridge     { background: #eef; color: #335; border-color: #ccd; }
    .sysinfo-role-badge.role-login      { background: #efe; color: #353; border-color: #cdc; }
    .sysinfo-role-badge.role-compute    { background: #fee; color: #533; border-color: #dcc; }
    .sysinfo-role-badge.role-standalone { background: #f5f0e0; color: #553; border-color: #ddc; }
    .sysinfo-content .metric-box {
      background: var(--bg2);
      padding: 12px;
      border-radius: 8px;
    }
    .sysinfo-content .metric-box label {
      display: block;
      font-size: 0.75rem;
      color: var(--muted);
      margin-bottom: 4px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    .sysinfo-content .metric-box .val {
      font-size: 1.1rem;
      font-weight: 600;
    }
    .sysinfo-content .metric-box .sub {
      font-size: 0.75rem;
      color: var(--muted);
      margin-top: 4px;
    }
  `;
}

export function init(page, api) {
  // Bind refresh button
  const refreshBtn = page.querySelector('[data-action="refresh"]');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => loadSysinfo(page, api));
  }

  // Host role rarely changes — fetch once on init.
  loadHostRole(page, api);

  // Auto-load on init
  loadSysinfo(page, api);
}

export function onNotification(data, page, api) {
  // SysInfo doesn't use notifications
}

// ─────────────────────────────────────────────────────────────
//  Internal functions
// ─────────────────────────────────────────────────────────────

async function loadSysinfo(page, api) {
  const content = page.querySelector('.sysinfo-content');
  content.innerHTML = '<div class="empty"><div class="spinner"></div><p style="margin-top:10px">Fetching metrics…</p></div>';

  try {
    const sid = await api.getSession('sysinfo');
    const m = await api.fetch(`metrics/${sid}`);
    content.innerHTML = renderSysinfo(m);
  } catch (e) {
    content.innerHTML = `<div class="card"><p style="color:var(--danger)">Error: ${api.escHtml(e.message)}</p></div>`;
    api.flash('SysInfo error: ' + e.message, false);
  }
}

// Fetches the session-less host_role endpoint and renders the badge in
// the page header.  Failure is non-fatal: we keep the placeholder and
// log to the console so the rest of the page still works.
async function loadHostRole(page, api) {
  const badge = page.querySelector('.sysinfo-role-badge');
  if (!badge) return;
  try {
    const r = await api.fetch('host_role');
    const role = r.role || '?';
    const icon = role === 'bridge'     ? '🌐'
              : role === 'compute'    ? '⚙️'
              : role === 'login'      ? '🖥'
              : role === 'standalone' ? '🧰'
              :                          '?';
    let label = `${icon} ${role}`;
    if (r.scheduler) {
      label += r.job_id ? ` (${r.scheduler} job ${r.job_id})`
                        : ` (${r.scheduler})`;
    }
    badge.textContent = label;
    badge.className = `sysinfo-role-badge role-${role}`;
  } catch (e) {
    badge.textContent = '? unknown';
    console.warn('sysinfo: host_role fetch failed:', e);
  }
}

function renderSysinfo(m) {
  const sys = m.system || {};
  const cpu = m.cpu || {};
  const mem = m.memory || {};
  const cpuPct = cpu.percent || 0;
  const memPct = mem.percent || 0;
  const cores = cpu.cores_logical || 1;
  const load = (cpu.load_avg || [0, 0, 0]).map(l => ((l / cores) * 100).toFixed(1));

  let html = `
    <div class="card">
      <div class="card-title">🖥️ System</div>
      <div class="grid4">
        <div class="metric-box"><label>Hostname</label><div class="val" style="font-size:.85rem">${sys.hostname || '?'}</div></div>
        <div class="metric-box"><label>User</label><div class="val" style="font-size:.85rem">${sys.user || '?'}</div></div>
        <div class="metric-box"><label>Kernel</label><div class="val" style="font-size:.85rem">${sys.kernel || '?'}</div></div>
        <div class="metric-box"><label>Arch</label><div class="val" style="font-size:.85rem">${sys.arch || '?'}</div></div>
        <div class="metric-box"><label>Uptime</label><div class="val" style="font-size:.85rem">${formatUptime(sys.uptime)}</div></div>
      </div>
    </div>

    <div class="grid2">
      <div class="card">
        <div class="card-title">⚙️ CPU</div>
        <div class="metric-box" style="margin-bottom:12px">
          <label>Model</label>
          <div class="val" style="font-size:.78rem;word-break:break-all">${cpu.model || '?'}</div>
        </div>
        <div class="grid2" style="margin-bottom:12px">
          <div class="metric-box"><label>Physical Cores</label><div class="val">${cpu.cores_physical || '?'}</div></div>
          <div class="metric-box"><label>Logical Cores</label><div class="val">${cpu.cores_logical || '?'}</div></div>
        </div>
        <div class="metric-box">
          <label>Usage ${cpuPct}%</label>
          <div class="progress-wrap"><div class="progress-bar"><div class="progress-fill ${pct2class(cpuPct)}" style="width:${cpuPct}%"></div></div></div>
          <div class="sub">Load avg: ${load[0]}% / ${load[1]}% / ${load[2]}%</div>
        </div>
      </div>
      <div class="card">
        <div class="card-title">🧠 Memory</div>
        <div class="metric-box" style="margin-bottom:12px">
          <label>Used ${memPct}%</label>
          <div class="val">${bytes2human(mem.used)} / ${bytes2human(mem.total)}</div>
          <div class="progress-wrap"><div class="progress-bar"><div class="progress-fill ${pct2class(memPct)}" style="width:${memPct}%"></div></div></div>
          <div class="sub">Available: ${bytes2human(mem.available)}</div>
        </div>
      </div>
    </div>`;

  // GPUs
  const gpus = m.gpus || [];
  if (gpus.length) {
    html += `<div class="card"><div class="card-title">🎮 GPUs</div><table>
      <thead><tr><th>ID</th><th>Name</th><th>Vendor</th><th>GPU%</th><th>Mem%</th><th>Total Mem</th></tr></thead><tbody>`;
    for (const g of gpus) {
      const memTotal = g.mem_total ? bytes2human(g.mem_total * 1024 * 1024) : 'N/A';
      html += `<tr>
        <td>${g.id}</td><td>${g.name}</td><td>${g.vendor || '-'}</td>
        <td>${g.util_gpu ?? '-'}%</td><td>${g.util_mem ?? '-'}%</td><td>${memTotal}</td>
      </tr>`;
    }
    html += '</tbody></table></div>';
  }

  // Disks
  const disks = m.disks || [];
  if (disks.length) {
    html += `<div class="card"><div class="card-title">💾 Disks</div><table>
      <thead><tr><th>Mount</th><th>Device</th><th>Type</th><th>Total</th><th>Used</th><th>Use%</th></tr></thead><tbody>`;
    for (const d of disks) {
      html += `<tr>
        <td>${d.mount}</td><td>${d.device}</td><td><span class="badge badge-gray">${d.type || d.fstype}</span></td>
        <td>${bytes2human(d.total)}</td><td>${bytes2human(d.used)}</td>
        <td><span class="badge ${d.percent > 80 ? 'badge-red' : 'badge-green'}">${d.percent}%</span></td>
      </tr>`;
    }
    html += '</tbody></table></div>';
  }

  // Network
  const nets = m.network || [];
  if (nets.length) {
    html += `<div class="card"><div class="card-title">🌐 Network</div><table>
      <thead><tr><th>Interface</th><th>IP</th><th>MAC</th><th>Speed</th><th>RX</th><th>TX</th></tr></thead><tbody>`;
    for (const n of nets) {
      html += `<tr>
        <td>${n.interface}</td><td>${n.ip || '-'}</td><td>${n.mac || '-'}</td>
        <td>${n.speed_mbps || 0} Mbps</td><td>${bytes2human(n.rx_bytes)}</td><td>${bytes2human(n.tx_bytes)}</td>
      </tr>`;
    }
    html += '</tbody></table></div>';
  }

  return html;
}

// ─────────────────────────────────────────────────────────────
//  Utility functions
// ─────────────────────────────────────────────────────────────

function formatUptime(secs) {
  if (!secs) return '?';
  const d = Math.floor(secs / 86400);
  const h = Math.floor((secs % 86400) / 3600);
  const m = Math.floor((secs % 3600) / 60);
  return `${d}d ${h}h ${m}m`;
}

function bytes2human(b) {
  if (!b) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  while (b >= 1024 && i < units.length - 1) {
    b /= 1024;
    i++;
  }
  return b.toFixed(1) + ' ' + units[i];
}

function pct2class(pct) {
  if (pct >= 90) return 'progress-red';
  if (pct >= 70) return 'progress-orange';
  return 'progress-green';
}
