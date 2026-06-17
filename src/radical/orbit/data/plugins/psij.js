/**
 * PsiJ Plugin Module for ORBIT Explorer
 *
 * HPC job submission via PsiJ (supports local, SLURM, PBS, LSF).
 */

export const name = 'psij';

// Shared with api.escHtml — set in init()
let escHtml = s => String(s || '');  // safe fallback until init()

// Per-endpoint child counters for generating unique endpoint names
const endpointCounters = {};

// Buffer for notifications that arrive before job entries are created
const pendingNotifications = {};  // jobId -> { data, state }

// Module-level job tracking: jobId -> {job_id, executable, arguments, state, ...}
let psijJobs = {};
let activePoller = null;   // interval ID for detail overlay polling

const TERMINAL = new Set(['COMPLETED', 'FAILED', 'CANCELED']);
const CANCELLABLE = new Set(['NEW', 'QUEUED', 'ACTIVE', 'STAGE_IN', 'PENDING']);

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">🚀</div>
      <h2>PsiJ Jobs — <span class="endpoint-label"></span></h2>
    </div>
    <div class="card">
      <div class="card-title">📝 Submit Job</div>
      <div class="grid2">
        <div>
          <div class="form-group"><label>Executable</label><input class="p-exec" type="text" value="orbit-endpoint-wrapper.sh" /></div>
          <div class="form-group"><label>Arguments (space-separated)</label><input class="p-args" type="text" value="" placeholder="auto-filled with --url and --name" /></div>
          <div class="form-group"><label>Executor</label>
            <select class="p-executor">
              <option value="local">local</option>
              <option value="slurm">slurm</option>
              <option value="pbs">pbs</option>
              <option value="lsf">lsf</option>
            </select>
          </div>
          <div class="form-group"><label>🌍 Environment Variables</label>
            <div class="psij-envvar-container">
              <div class="psij-envvar-rows"></div>
              <button type="button" class="btn btn-secondary btn-sm" data-action="add-env" style="margin-top:8px;">➕ Add Variable</button>
            </div>
          </div>
        </div>
        <div>
          <div class="form-group"><label>Queue / Partition</label><input class="p-queue" type="text" placeholder="optional" /></div>
          <div class="form-group"><label>Account / Project</label><input class="p-account" type="text" placeholder="optional" /></div>
          <div class="form-group"><label>Duration (seconds)</label><input class="p-duration" type="text" value="600" /></div>
          <div class="form-group"><label>Number of Nodes</label><input class="p-node-count" type="number" value="1" /></div>
          <div class="form-group"><label>🔧 Custom Attributes</label>
            <div class="psij-attributes-container" style="margin-bottom: 4px;">
              <div class="psij-attribute-rows"></div>
              <button type="button" class="btn btn-secondary btn-sm" data-action="add-attr" style="margin-top:8px;">➕ Add Attribute</button>
            </div>
          </div>
        </div>
      </div>
      <button class="btn btn-success" data-action="submit">🚀 Submit</button>
      <label style="margin-left:16px;cursor:pointer;user-select:none;">
        <input type="checkbox" class="p-tunnel" style="margin-right:4px;" />
        Reverse SSH tunnel
      </label>
      <input class="p-tunnel-port" type="text" style="width:80px; margin-left:8px; display:inline-block; text-align:right;" placeholder="port" title="Bridge port for reverse tunnel" />
    </div>
    <div class="card psij-jobs-card">
      <div class="card-title">📊 Job Monitor</div>
      <div class="psij-table-area"><p style="color:var(--muted)">No jobs submitted yet.</p></div>
    </div>
  `;
}

export function css() {
  return `
    .psij-table-area table { width: 100%; }
    .psij-table-area td:first-child { min-width: 9em; }
    .psij-job-row { cursor: pointer; transition: background 0.15s; }
    .psij-job-row:hover { background: var(--hover); }
  `;
}

export function init(page, api) {
  escHtml = api.escHtml;

  // Bind add attribute button
  const addAttrBtn = page.querySelector('[data-action="add-attr"]');
  if (addAttrBtn) {
    addAttrBtn.addEventListener('click', () => addAttributeRow(page));
  }

  // Bind add env-var button
  const addEnvBtn = page.querySelector('[data-action="add-env"]');
  if (addEnvBtn) {
    addEnvBtn.addEventListener('click', () => addEnvRow(page));
  }

  // Bind submit button — dispatches to submit_tunneled if tunnel checkbox is checked
  const submitBtn = page.querySelector('[data-action="submit"]');
  if (submitBtn) {
    submitBtn.addEventListener('click', () => {
      const tunnel = !!(page.querySelector('.p-tunnel') || {}).checked;
      if (tunnel) {
        submitTunneledJob(page, api);
      } else {
        submitJob(page, api);
      }
    });
  }

  // Pre-fill --url / --name args for first submission.
  // Default to `-p all` because the compute node can host more plugins
  // than the login node — using the login's plugin list as a default
  // would needlessly restrict the sub-endpoint.
  const argsInput = page.querySelector('.p-args');
  if (argsInput && !argsInput.value) {
    const nextName = getNextEndpointChildName(api.endpointName);
    argsInput.value = `--url ${api.bridgeUrl} --name ${nextName} -p all`;
  }

  // Pre-populate tunnel port from bridge URL
  const portInput = page.querySelector('.p-tunnel-port');
  if (portInput && !portInput.value) {
    try {
      const urlPort = new URL(api.bridgeUrl).port || '8000';
      portInput.value = urlPort;
    } catch (_) {
      portInput.value = '8000';
    }
  }

  // Prefill from cached queue data if already available
  const qd = api.getQueueData();
  if (qd) replaceQueueAccountDropdowns(page, qd);

  // Pre-select slurm executor if queue_info says we're on a SLURM login node
  const alloc = api.getJobAllocation();
  if (alloc === null) {
    const sel = page.querySelector('.p-executor');
    if (sel) sel.value = 'slurm';
  }

  // Toggle --tunnel in the arguments field when the checkbox changes.
  // Only modify args when the executable is the endpoint wrapper / service script,
  // since --tunnel is a orbit-endpoint flag, not a general job argument.
  const ENDPOINT_EXEC_RE = /orbit(?:-wrapper\.sh|-service(?:\.py)?)$/;
  const tunnelChk = page.querySelector('.p-tunnel');
  if (tunnelChk) {
    tunnelChk.addEventListener('change', () => {
      const argsInput = page.querySelector('.p-args');
      const execInput = page.querySelector('.p-exec');
      if (!argsInput || !execInput) return;
      if (!ENDPOINT_EXEC_RE.test(execInput.value.trim())) return;
      const parts = argsInput.value.trim().split(/\s+/).filter(Boolean);
      if (tunnelChk.checked) {
        if (!parts.includes('--tunnel')) parts.push('--tunnel');
      } else {
        const idx = parts.indexOf('--tunnel');
        if (idx !== -1) parts.splice(idx, 1);
      }
      argsInput.value = parts.join(' ');
    });
  }
}

export function onShow(page, api) {
  const qd = api.getQueueData();
  if (qd) replaceQueueAccountDropdowns(page, qd);

  const alloc = api.getJobAllocation();
  if (alloc === null) {
    const sel = page.querySelector('.p-executor');
    if (sel && sel.value === 'local') sel.value = 'slurm';
  }
}

export function onNotification(data, page, api) {
  if (data.topic !== 'job_status') return;

  const jobId = data.data?.job_id || '';
  const state = data.data?.state || '?';

  // Update module-level tracking
  if (psijJobs[jobId]) {
    psijJobs[jobId].state     = state;
    psijJobs[jobId].exit_code = data.data?.exit_code;
    if (data.data?.stdout) psijJobs[jobId].stdout = data.data.stdout;
    if (data.data?.stderr) psijJobs[jobId].stderr = data.data.stderr;
  }

  // Update table row; buffer if job entry doesn't exist yet
  const row = page.querySelector(`.psij-job-row[data-job-id="${CSS.escape(jobId)}"]`);
  if (row) {
    updateJobRow(page, jobId, state, data.data);
  } else if (jobId) {
    pendingNotifications[jobId] = { data: data.data, state };
  }
}

export const notificationConfig = {
  topic: 'job_status',
  idField: 'job_id'
};

// ─────────────────────────────────────────────────────────────
//  Internal functions
// ─────────────────────────────────────────────────────────────

function getNextEndpointChildName(endpointName) {
  if (!endpointCounters[endpointName]) endpointCounters[endpointName] = 0;
  endpointCounters[endpointName]++;
  return `${endpointName}.${endpointCounters[endpointName]}`;
}

function replaceQueueAccountDropdowns(page, queueData) {
  const { queues = [], allocations = [] } = queueData;

  const queueInput = page.querySelector('.p-queue');
  if (queueInput && queueInput.tagName === 'INPUT') {
    const sel = document.createElement('select');
    sel.className = queueInput.className;

    const getQName = q => q.name || q.partition || String(q);
    const pb       = queues.find(q => getQName(q) === 'debug');
    const pi       = queues.find(q => getQName(q) === 'interactive');
    const defaultQ = pb || pi || queues[0];
    const defaultQName = defaultQ ? getQName(defaultQ) : '';
    const currentVal   = queueInput.value;

    sel.innerHTML = '<option value="">(none)</option>' + queues.map(q => {
      const qn         = getQName(q);
      const isSelected = (currentVal && currentVal === qn) || (!currentVal && qn === defaultQName);
      return `<option value="${qn}" ${isSelected ? 'selected' : ''}>${qn}</option>`;
    }).join('');
    queueInput.parentNode.replaceChild(sel, queueInput);
  }

  const accountInput = page.querySelector('.p-account');
  if (accountInput && accountInput.tagName === 'INPUT') {
    const sel      = document.createElement('select');
    sel.className  = accountInput.className;

    const accounts   = [...new Set(allocations.map(a => a.account).filter(Boolean))];
    const defaultAcc = accounts[0] || '';
    const currentVal = accountInput.value;

    sel.innerHTML = '<option value="">(none)</option>' + accounts.map(a => {
      const isSelected = (currentVal && currentVal === a) || (!currentVal && a === defaultAcc);
      return `<option value="${a}" ${isSelected ? 'selected' : ''}>${a}</option>`;
    }).join('');
    accountInput.parentNode.replaceChild(sel, accountInput);
  }
}

function addEnvRow(page, key = '', value = '') {
  const container = page.querySelector('.psij-envvar-rows');
  const row = document.createElement('div');
  row.style.display = 'flex';
  row.style.gap = '10px';
  row.style.marginBottom = '8px';
  row.innerHTML = `
    <input class="p-env-key" type="text" placeholder="KEY" style="flex:1;" value="${escHtml(key)}" />
    <input class="p-env-val" type="text" placeholder="value" style="flex:2;" value="${escHtml(value)}" />
    <button class="task-cancel-btn">❌</button>
  `;
  row.querySelector('button').addEventListener('click', () => row.remove());
  container.appendChild(row);
}

function addAttributeRow(page) {
  const container = page.querySelector('.psij-attribute-rows');
  const row = document.createElement('div');
  row.style.display = 'flex';
  row.style.gap = '10px';
  row.style.marginBottom = '8px';
  row.innerHTML = `
    <input class="p-attr-key" type="text" placeholder="Key (e.g. slurm.constraint)" style="flex:1;" />
    <input class="p-attr-val" type="text" placeholder="Value (e.g. cpu_gen_1)" style="flex:2;" />
    <button class="task-cancel-btn">❌</button>
  `;
  row.querySelector('button').addEventListener('click', () => row.remove());
  container.appendChild(row);
}

// ─────────────────────────────────────────────────────────────
//  Job table rendering
// ─────────────────────────────────────────────────────────────

function ensureTable(page) {
  const area = page.querySelector('.psij-table-area');
  if (!area) return null;

  let table = area.querySelector('table');
  if (!table) {
    area.innerHTML = `<table>
      <thead><tr>
        <th>Native ID</th><th>State</th><th>Executable</th><th>Executor</th>
        <th>Tunnel</th><th></th>
      </tr></thead><tbody></tbody></table>`;
    table = area.querySelector('table');
  }
  return table;
}

function addJobRow(page, api, job) {
  const table = ensureTable(page);
  if (!table) return;

  const tbody = table.querySelector('tbody');
  const tr = document.createElement('tr');
  tr.className = 'psij-job-row';
  tr.dataset.jobId = job.job_id;

  const st = job.state || 'NEW';
  const badge = stateBadge(st);
  const shortExec = (job.executable || '?').split('/').pop();
  const canCancel = CANCELLABLE.has(st) || !TERMINAL.has(st);

  const nativeId = job.native_id ? escHtml(String(job.native_id)) : '—';
  tr.innerHTML = `
    <td><strong>${nativeId}</strong></td>
    <td><span class="badge ${badge}">${st}</span></td>
    <td><code>${escHtml(shortExec)}</code></td>
    <td>${escHtml(job.executor || 'local')}</td>
    <td class="psij-tunnel-cell">${job.endpoint_name ? '<span class="badge badge-orange psij-tunnel-badge">pending</span>' : ''}</td>
    <td>${canCancel ? `<button class="task-cancel-btn psij-cancel-btn" title="Cancel">❌</button>` : ''}</td>
  `;

  // Row click → detail overlay
  tr.addEventListener('click', (e) => {
    if (e.target.closest('.psij-cancel-btn')) return;
    openJobDetail(api, job.job_id);
  });

  // Cancel button — uses job_id from closure, not from data attribute
  const cancelBtn = tr.querySelector('.psij-cancel-btn');
  if (cancelBtn) {
    cancelBtn.addEventListener('click', async (e) => {
      e.stopPropagation();
      cancelBtn.disabled = true;
      cancelBtn.textContent = '…';
      try {
        const sid = await api.getSession('psij');
        await api.fetch(`cancel/${sid}/${encodeURIComponent(job.job_id)}`, { method: 'POST' });
        api.flash(`Job ${job.job_id.slice(0, 8)}… canceled`);
      } catch (err) {
        api.flash('Cancel failed: ' + err.message, false);
        cancelBtn.disabled = false;
        cancelBtn.textContent = '❌';
      }
    });
  }

  tbody.insertBefore(tr, tbody.firstChild);
}

function updateJobRow(page, jobId, state, data) {
  const row = page.querySelector(`.psij-job-row[data-job-id="${CSS.escape(jobId)}"]`);
  if (!row) return;

  const badge = row.querySelector('.badge:not(.psij-tunnel-badge)');
  if (badge) {
    badge.textContent = state;
    badge.className = `badge ${stateBadge(state)}`;
  }

  if (TERMINAL.has(state)) {
    const cancelBtn = row.querySelector('.psij-cancel-btn');
    if (cancelBtn) cancelBtn.remove();
  }
}

function updateJobRowTunnel(page, jobId, tunnelStatus) {
  const row = page.querySelector(`.psij-job-row[data-job-id="${CSS.escape(jobId)}"]`);
  if (!row) return;
  const badge = row.querySelector('.psij-tunnel-badge');
  if (!badge) return;
  const cls = tunnelStatus === 'active' || tunnelStatus === 'done' ? 'badge-green'
            : tunnelStatus === 'failed' ? 'badge-red'
            : 'badge-orange';
  badge.className = `badge ${cls} psij-tunnel-badge`;
  badge.textContent = tunnelStatus;
}

// ─────────────────────────────────────────────────────────────
//  Job detail overlay with streaming
// ─────────────────────────────────────────────────────────────

async function openJobDetail(api, jobId) {
  // Stop any existing poller
  stopPoller();

  const sid = await api.getSession('psij');

  // Initial full fetch (offset=0)
  let status;
  try {
    status = await api.fetch(`status/${sid}/${jobId}`);
  } catch (e) {
    api.flash('Error loading job details: ' + e.message, false);
    return;
  }

  // Update module-level cache
  if (psijJobs[jobId]) Object.assign(psijJobs[jobId], status);

  // Render overlay
  renderDetailOverlay(api, status);

  // Start polling if non-terminal
  if (!TERMINAL.has(status.state)) {
    let stdoutOff = status.stdout_offset || 0;
    let stderrOff = status.stderr_offset || 0;

    activePoller = setInterval(async () => {
      // Stop if overlay closed
      const overlay = document.getElementById('jobs-overlay');
      if (!overlay || !overlay.classList.contains('visible')) {
        stopPoller();
        return;
      }

      try {
        const upd = await api.fetch(
          `status/${sid}/${jobId}?stdout_offset=${stdoutOff}&stderr_offset=${stderrOff}`
        );

        // Update state badge in overlay
        const stateEl = document.getElementById('psij-detail-state');
        if (stateEl) {
          stateEl.className = `badge ${stateBadge(upd.state)}`;
          stateEl.textContent = upd.state;
        }

        // Append new stdout
        if (upd.stdout) {
          const outEl = document.getElementById('psij-detail-stdout');
          if (outEl) outEl.textContent += upd.stdout;
        }
        stdoutOff = upd.stdout_offset || stdoutOff;

        // Append new stderr
        if (upd.stderr) {
          const errEl = document.getElementById('psij-detail-stderr');
          if (errEl) errEl.textContent += upd.stderr;
        }
        stderrOff = upd.stderr_offset || stderrOff;

        // Update exit code
        if (upd.exit_code != null) {
          const rcEl = document.getElementById('psij-detail-rc');
          if (rcEl) rcEl.textContent = upd.exit_code;
        }

        // Stop polling on terminal
        if (TERMINAL.has(upd.state)) {
          stopPoller();

          // Also update the table row
          if (psijJobs[jobId]) {
            psijJobs[jobId].state = upd.state;
            psijJobs[jobId].exit_code = upd.exit_code;
          }
        }
      } catch (e) {
        // Silently ignore poll errors
      }
    }, 3000);
  }
}

function stopPoller() {
  if (activePoller) {
    clearInterval(activePoller);
    activePoller = null;
  }
}

function renderDetailOverlay(api, job) {
  const st = job.state || '-';
  const badge = stateBadge(st);
  const argsStr = Array.isArray(job.arguments) ? job.arguments.join(' ') : (job.arguments || '-');

  const fields = [
    ['Job ID',    escHtml(job.job_id || '-')],
    ['Native ID', escHtml(job.native_id || '-')],
    ['State',     `<span id="psij-detail-state" class="badge ${badge}">${st}</span>`],
    ['Exit Code', `<span id="psij-detail-rc">${job.exit_code ?? '-'}</span>`],
    ['Executable', job.executable || '-'],
    ['Arguments', `<code>${escHtml(argsStr)}</code>`],
    ['Executor',  escHtml(job.executor || '-')],
    ['Queue',     escHtml(job.queue_name || '-')],
    ['Account',   escHtml(job.account || '-')],
    ['Nodes',     job.node_count || '-'],
    ['Duration',  job.duration ? `${job.duration}s` : '-'],
    ['Directory', escHtml(job.directory || '-')],
    ['Message',   escHtml(job.message || '-')],
  ];

  let body = '<div class="job-detail-grid">';
  for (const [label, value] of fields) {
    body += `<div class="job-detail-item">
      <span class="label">${label}</span>
      <span class="value">${value}</span>
    </div>`;
  }
  body += '</div>';

  // stdout / stderr sections
  const outText = job.stdout || '';
  const errText = job.stderr || '';
  const noOutput = '<span style="color:var(--muted);font-style:italic">(no output captured)</span>';
  body += `
    <div class="job-output-section">
      <h4>stdout</h4>
      <pre id="psij-detail-stdout" class="out-stream">${outText ? escHtml(outText) : noOutput}</pre>
    </div>
    <div class="job-output-section">
      <h4>stderr</h4>
      <pre id="psij-detail-stderr" class="err-stream">${errText ? escHtml(errText) : noOutput}</pre>
    </div>
  `;

  const title = `🚀 Job Details: ${escHtml((job.job_id || '').slice(0, 12))}…`;
  api.showOverlay(title, body);
}

// ─────────────────────────────────────────────────────────────
//  Submit
// ─────────────────────────────────────────────────────────────

async function submitJob(page, api) {
  const exec = page.querySelector('.p-exec').value.trim();
  const args = page.querySelector('.p-args').value.trim().split(/\s+/).filter(Boolean);
  const executor = page.querySelector('.p-executor').value;
  const queue = page.querySelector('.p-queue').value.trim();
  const account = page.querySelector('.p-account').value.trim();
  const duration = page.querySelector('.p-duration').value.trim();
  const nodeCountEl = page.querySelector('.p-node-count');
  const nodeCount = nodeCountEl ? nodeCountEl.value.trim() : '';

  const job_spec = { executable: exec, arguments: args, attributes: {} };
  if (queue) job_spec.attributes.queue_name = queue;
  if (account) job_spec.attributes.account = account;
  if (duration) job_spec.attributes.duration = duration;
  if (nodeCount) job_spec.attributes.node_count = parseInt(nodeCount, 10);

  const attrRows = page.querySelectorAll('.psij-attribute-rows > div');
  attrRows.forEach(row => {
    const key = row.querySelector('.p-attr-key').value.trim();
    const val = row.querySelector('.p-attr-val').value.trim();
    if (key && val) {
      if (!job_spec.custom_attributes) job_spec.custom_attributes = {};
      job_spec.custom_attributes[key] = val;
    }
  });

  const envRows = page.querySelectorAll('.psij-envvar-rows > div');
  envRows.forEach(row => {
    const key = row.querySelector('.p-env-key').value.trim();
    const val = row.querySelector('.p-env-val').value.trim();
    if (key) {
      if (!job_spec.environment) job_spec.environment = {};
      job_spec.environment[key] = val;
    }
  });

  try {
    const sid = await api.getSession('psij');

    const res = await api.fetch(`submit/${sid}`, {
      method: 'POST',
      body: JSON.stringify({ job_spec, executor })
    });

    const jobId = res.job_id;
    api.flash(`Job submitted: ${jobId}`);
    api.registerTask('psij', jobId, `${exec} ${args.join(' ')}`);

    // Track in module state
    const jobData = {
      job_id:     jobId,
      native_id:  res.native_id,
      executable: exec,
      arguments:  args,
      executor:   executor,
      state:      'NEW',
      queue_name: queue || null,
      account:    account || null,
      node_count: nodeCount || null,
      duration:   duration || null,
    };
    psijJobs[jobId] = jobData;

    // Add row to table
    addJobRow(page, api, jobData);

    // Drain any buffered notifications that arrived before row existed
    if (pendingNotifications[jobId]) {
      const pending = pendingNotifications[jobId];
      updateJobRow(page, jobId, pending.state, pending.data);
      if (psijJobs[jobId]) Object.assign(psijJobs[jobId], pending.data);
      delete pendingNotifications[jobId];
    }

    // Update only --name for the NEXT submission
    const argsInput = page.querySelector('.p-args');
    const nextName  = getNextEndpointChildName(api.endpointName);
    argsInput.value = argsInput.value.replace(/--name\s+\S+/, `--name ${nextName}`);

  } catch (e) {
    api.flash('PsiJ error: ' + e.message, false);
  }
}

// ─────────────────────────────────────────────────────────────
//  Submit Tunneled Endpoint Service
// ─────────────────────────────────────────────────────────────

// Active tunnel pollers: endpoint_name -> intervalId
const tunnelPollers = {};

async function submitTunneledJob(page, api) {
  const exec = page.querySelector('.p-exec').value.trim();
  const args = page.querySelector('.p-args').value.trim().split(/\s+/).filter(Boolean);
  const executor = page.querySelector('.p-executor').value;
  const queue = page.querySelector('.p-queue').value.trim();
  const account = page.querySelector('.p-account').value.trim();
  const duration = page.querySelector('.p-duration').value.trim();
  const nodeCountEl = page.querySelector('.p-node-count');
  const nodeCount = nodeCountEl ? nodeCountEl.value.trim() : '';
  const tunnel = !!(page.querySelector('.p-tunnel') || {}).checked;

  const job_spec = { executable: exec, arguments: args, attributes: {} };
  if (queue) job_spec.attributes.queue_name = queue;
  if (account) job_spec.attributes.account = account;
  if (duration) job_spec.attributes.duration = duration;
  if (nodeCount) job_spec.attributes.node_count = parseInt(nodeCount, 10);

  const attrRows = page.querySelectorAll('.psij-attribute-rows > div');
  attrRows.forEach(row => {
    const key = row.querySelector('.p-attr-key').value.trim();
    const val = row.querySelector('.p-attr-val').value.trim();
    if (key && val) {
      if (!job_spec.custom_attributes) job_spec.custom_attributes = {};
      job_spec.custom_attributes[key] = val;
    }
  });

  const envRows = page.querySelectorAll('.psij-envvar-rows > div');
  envRows.forEach(row => {
    const key = row.querySelector('.p-env-key').value.trim();
    const val = row.querySelector('.p-env-val').value.trim();
    if (key) {
      if (!job_spec.environment) job_spec.environment = {};
      job_spec.environment[key] = val;
    }
  });

  try {
    const sid = await api.getSession('psij');

    const res = await api.fetch(`submit_tunneled/${sid}`, {
      method: 'POST',
      body: JSON.stringify({ job_spec, executor, tunnel })
    });

    const jobId     = res.job_id;
    const endpointName  = res.endpoint_name;
    api.flash(`Endpoint job submitted: ${jobId} (endpoint: ${endpointName})`);
    api.registerTask('psij', jobId, `${exec} ${args.join(' ')}`);

    // Track in module state
    const jobData = {
      job_id:     jobId,
      native_id:  res.native_id,
      executable: exec,
      arguments:  args,
      executor:   executor,
      state:      'NEW',
      queue_name: queue || null,
      account:    account || null,
      node_count: nodeCount || null,
      duration:   duration || null,
      endpoint_name:  endpointName,
    };
    psijJobs[jobId] = jobData;
    addJobRow(page, api, jobData);

    if (pendingNotifications[jobId]) {
      const pending = pendingNotifications[jobId];
      updateJobRow(page, jobId, pending.state, pending.data);
      if (psijJobs[jobId]) Object.assign(psijJobs[jobId], pending.data);
      delete pendingNotifications[jobId];
    }

    // Update --name for next submission
    const argsInput = page.querySelector('.p-args');
    const nextName  = getNextEndpointChildName(api.endpointName);
    argsInput.value = argsInput.value.replace(/--name\s+\S+/, `--name ${nextName}`);

    // Start tunnel status poller (updates the job table row)
    if (tunnel && endpointName) {
      startTunnelPoller(page, api, endpointName);
    }

  } catch (e) {
    api.flash('Endpoint job error: ' + e.message, false);
  }
}

function startTunnelPoller(page, api, endpointName) {
  if (tunnelPollers[endpointName]) {
    clearInterval(tunnelPollers[endpointName]);
  }
  const jobId = Object.keys(psijJobs).find(id => psijJobs[id].endpoint_name === endpointName);

  tunnelPollers[endpointName] = setInterval(async () => {
    try {
      const s = await api.fetch(`tunnel_status/${encodeURIComponent(endpointName)}`);
      if (jobId) updateJobRowTunnel(page, jobId, s.status);
      if (s.status === 'active' || s.status === 'done' || s.status === 'failed') {
        clearInterval(tunnelPollers[endpointName]);
        delete tunnelPollers[endpointName];
      }
    } catch (_) { /* silently ignore */ }
  }, 3000);
}

// ─────────────────────────────────────────────────────────────
//  Utility functions
// ─────────────────────────────────────────────────────────────

function stateBadge(state) {
  const s = (state || '').toUpperCase();
  if (['COMPLETED'].includes(s))               return 'badge-green';
  if (['ACTIVE', 'STAGE_OUT', 'CLEANUP'].includes(s)) return 'badge-blue';
  if (['FAILED', 'CANCELED'].includes(s))      return 'badge-red';
  return 'badge-orange';
}

