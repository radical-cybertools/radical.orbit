/**
 * IRI Instance Plugin Module for ORBIT Explorer
 *
 * Combined resource info + job submission for a single IRI endpoint.
 * The plugin instance is already connected (token set at creation time
 * by iri_connect), so there is no connection card.
 */

export const name = 'iri_instance';

let escHtml = s => String(s || '');

let iriJobs = {};
const pendingNotifications = {};

const TERMINAL    = new Set(['completed', 'failed', 'canceled']);
const CANCELLABLE = new Set(['new', 'queued', 'held', 'active']);

// ---------------------------------------------------------------------------
//  Template
// ---------------------------------------------------------------------------

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">🔬</div>
      <h2>IRI — <span class="endpoint-label"></span></h2>
      <button class="btn btn-secondary btn-sm iri-inst-disconnect-btn"
              style="margin-left:auto;">Disconnect</button>
    </div>

    <div class="card">
      <div class="card-title">🖥️ Compute Resources
        <button class="btn btn-secondary btn-sm" data-action="refresh-resources"
                style="margin-left:12px;">↺ Refresh</button>
      </div>
      <div class="iri-resources-area">
        <div class="empty"><div class="spinner"></div>
          <p style="margin-top:10px">Loading…</p></div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">🗄️ Storage Resources
        <button class="btn btn-secondary btn-sm" data-action="refresh-storage"
                style="margin-left:12px;">↺ Refresh</button>
      </div>
      <div class="iri-storage-area">
        <p style="color:var(--muted)">Click refresh to load.</p>
      </div>
    </div>

    <div class="card">
      <div class="card-title">📝 Submit Job</div>
      <div class="grid2">
        <div>
          <div class="form-group">
            <label>Resource</label>
            <select class="iri-resource-sel">
              <option value="">— select after loading —</option>
            </select>
          </div>
          <div class="form-group"><label>Executable</label>
            <input class="iri-exec" type="text" placeholder="/bin/bash" /></div>
          <div class="form-group"><label>Arguments (space-separated)</label>
            <input class="iri-args" type="text" placeholder="-lc 'hostname'" /></div>
          <div class="form-group"><label>Job Name</label>
            <input class="iri-name" type="text" placeholder="my-job" /></div>
          <div class="form-group"><label>Working Directory</label>
            <input class="iri-dir" type="text" placeholder="/scratch/myproj" /></div>
          <div class="form-group">
            <label>🌍 Environment Variables</label>
            <div class="iri-envvar-rows"></div>
            <button type="button" class="btn btn-secondary btn-sm"
                    data-action="add-env" style="margin-top:8px;">➕ Add Variable</button>
          </div>
        </div>
        <div>
          <div class="form-group"><label>Queue / Partition</label>
            <input class="iri-queue" type="text" placeholder="debug" /></div>
          <div class="form-group"><label>Account / Project</label>
            <input class="iri-account" type="text" placeholder="myproject" /></div>
          <div class="form-group"><label>Duration (seconds)</label>
            <input class="iri-duration" type="number" value="300" /></div>
          <div class="form-group"><label>Node Count</label>
            <input class="iri-nodes" type="number" value="1" min="1" /></div>
          <div class="form-group"><label>Process Count</label>
            <input class="iri-procs" type="number" value="1" min="1" /></div>
        </div>
      </div>
      <button class="btn btn-success" data-action="submit">🚀 Submit Job</button>
    </div>

    <div class="card">
      <div class="card-title">📊 Job Monitor
        <button class="btn btn-secondary btn-sm" data-action="refresh-jobs"
                style="margin-left:12px;">↺ Refresh</button>
      </div>
      <div class="iri-jobs-area">
        <p style="color:var(--muted)">No jobs submitted yet.</p>
      </div>
    </div>

    <div class="card">
      <div class="card-title">💼 Projects &amp; Allocations
        <button class="btn btn-secondary btn-sm" data-action="refresh-projects"
                style="margin-left:12px;">↺ Refresh</button>
      </div>
      <div class="iri-projects-area">
        <p style="color:var(--muted)">Click refresh to load.</p>
      </div>
    </div>
  `;
}

export function css() {
  return `
    .iri-job-row     { cursor: pointer; transition: background 0.15s; }
    .iri-job-row:hover  { background: var(--hover); }
    .iri-project-row    { cursor: pointer; transition: background 0.15s; }
    .iri-project-row:hover { background: var(--hover); }
  `;
}

// ---------------------------------------------------------------------------
//  Lifecycle hooks
// ---------------------------------------------------------------------------

export async function init(page, api) {
  escHtml = api.escHtml;

  page.querySelector('[data-action="refresh-resources"]')
      .addEventListener('click', () => loadResources(page, api, 'compute', '.iri-resources-area'));
  page.querySelector('[data-action="refresh-storage"]')
      .addEventListener('click', () => loadResources(page, api, 'storage', '.iri-storage-area'));
  page.querySelector('[data-action="submit"]')
      .addEventListener('click', () => submitJob(page, api));
  page.querySelector('[data-action="add-env"]')
      .addEventListener('click', () => addEnvRow(page));
  page.querySelector('[data-action="refresh-jobs"]')
      .addEventListener('click', () => refreshJobs(page, api));
  page.querySelector('[data-action="refresh-projects"]')
      .addEventListener('click', () => loadProjects(page, api));

  // Disconnect button — calls back to iri_connect
  page.querySelector('.iri-inst-disconnect-btn')
      .addEventListener('click', () => doDisconnect(page, api));

  // Auto-load compute resources + populate resource dropdown
  await loadResources(page, api, 'compute', '.iri-resources-area');
}

export async function onShow(page, api) {
  const resourceId = page.querySelector('.iri-resource-sel')?.value;
  if (resourceId) await refreshJobs(page, api);
}

export function onNotification(data, page, api) {
  if (data.topic !== 'job_status') return;

  const jobId = data.data?.job_id || '';
  const state = (data.data?.state || '?').toLowerCase();

  if (iriJobs[jobId]) iriJobs[jobId].state = state;

  const row = page.querySelector(`.iri-job-row[data-job-id="${CSS.escape(jobId)}"]`);
  if (row) {
    updateJobRow(page, jobId, state);
  } else if (jobId) {
    pendingNotifications[jobId] = { state, data: data.data };
  }
}

export const notificationConfig = {
  topic  : 'job_status',
  idField: 'job_id',
};

// ---------------------------------------------------------------------------
//  Resource loading
// ---------------------------------------------------------------------------

async function loadResources(page, api, resourceType, areaSelector) {
  const area = page.querySelector(areaSelector);
  area.innerHTML = '<div class="empty"><div class="spinner"></div>' +
                   '<p style="margin-top:10px">Loading…</p></div>';

  try {
    const data      = await api.fetch(
      `resources?resource_type=${encodeURIComponent(resourceType)}`);
    const resources = data.resources || [];

    if (!resources.length) {
      area.innerHTML = `<p style="color:var(--muted)">No ${escHtml(resourceType)} resources.</p>`;
      return;
    }

    let html = `<table><thead><tr>
      <th>Name</th><th>Status</th><th>Description</th>
    </tr></thead><tbody>`;

    for (const r of resources) {
      const rname = escHtml(r.name || r.id || '-');
      const group = r.group ? escHtml(r.group) : '';
      const label = group ? `${group} / ${rname}` : rname;
      const st    = (r.current_status || 'unknown').toLowerCase();
      const badge = st === 'up'       ? 'badge-green'
                  : st === 'down'     ? 'badge-red'
                  : st === 'degraded' ? 'badge-orange'
                  : 'badge-gray';
      html += `<tr>
        <td><strong>${label}</strong></td>
        <td><span class="badge ${badge}">${escHtml(st)}</span></td>
        <td>${escHtml(r.description || '-')}</td>
      </tr>`;
    }
    html += '</tbody></table>';
    area.innerHTML = html;

    // Also populate the resource dropdown for job submission
    if (resourceType === 'compute') {
      const sel = page.querySelector('.iri-resource-sel');
      sel.innerHTML = resources.map(r => {
        const id    = r.name || r.id || '-';
        const group = r.group || '';
        const label = group ? `${group} / ${id}` : id;
        const st    = (r.current_status || '').toLowerCase();
        const dot   = st === 'up' ? '🟢' : st === 'down' ? '🔴' : '⚪';
        return `<option value="${escHtml(id)}">${dot} ${escHtml(label)}</option>`;
      }).join('');
    }

  } catch (e) {
    area.innerHTML = `<p style="color:var(--danger)">Error: ${escHtml(e.message)}</p>`;
  }
}

// ---------------------------------------------------------------------------
//  Job submission
// ---------------------------------------------------------------------------

async function submitJob(page, api) {
  const resourceId = page.querySelector('.iri-resource-sel').value?.trim();
  if (!resourceId) { api.flash('Select a resource first', false); return; }

  const exec = page.querySelector('.iri-exec').value.trim();
  if (!exec) { api.flash('Executable is required', false); return; }

  const args    = page.querySelector('.iri-args').value.trim()
                      .split(/\s+/).filter(Boolean);
  const jobName = page.querySelector('.iri-name').value.trim();
  const dir     = page.querySelector('.iri-dir').value.trim();
  const queue   = page.querySelector('.iri-queue').value.trim();
  const account = page.querySelector('.iri-account').value.trim();
  const dur     = page.querySelector('.iri-duration').value.trim();
  const nodes   = parseInt(page.querySelector('.iri-nodes').value, 10) || 1;
  const procs   = parseInt(page.querySelector('.iri-procs').value, 10) || 1;

  const job_spec = {
    executable: exec,
    arguments : args,
    resources : { node_count: nodes, process_count: procs },
    attributes: {},
  };
  if (jobName) job_spec.name      = jobName;
  if (dir)     job_spec.directory = dir;
  if (queue)   job_spec.attributes.queue_name = queue;
  if (account) job_spec.attributes.account    = account;
  if (dur)     job_spec.attributes.duration   = parseInt(dur, 10);

  const envRows = page.querySelectorAll('.iri-envvar-rows > div');
  envRows.forEach(row => {
    const key = row.querySelector('.iri-env-key').value.trim();
    const val = row.querySelector('.iri-env-val').value.trim();
    if (key) {
      if (!job_spec.environment) job_spec.environment = {};
      job_spec.environment[key] = val;
    }
  });

  try {
    const res = await api.fetch(`submit/${encodeURIComponent(resourceId)}`, {
      method: 'POST',
      quiet : true,
      body  : JSON.stringify({ job_spec }),
    });

    const jobId = res.job_id;
    api.flash(`Job submitted: ${jobId}`);

    const jobData = {
      job_id     : jobId,
      resource_id: resourceId,
      name       : jobName || exec,
      executable : exec,
      state      : 'new',
    };
    iriJobs[jobId] = jobData;
    addJobRow(page, api, jobData);

    if (pendingNotifications[jobId]) {
      updateJobRow(page, jobId, pendingNotifications[jobId].state);
      if (iriJobs[jobId]) iriJobs[jobId].state = pendingNotifications[jobId].state;
      delete pendingNotifications[jobId];
    }

  } catch (e) {
    api.showOverlay('Submit Failed', `<p style="color:var(--danger)">${escHtml(cleanIRIError(e.message))}</p>`);
  }
}

// ---------------------------------------------------------------------------
//  Job table
// ---------------------------------------------------------------------------

function ensureTable(page) {
  const area = page.querySelector('.iri-jobs-area');
  if (!area) return null;
  let table = area.querySelector('table');
  if (!table) {
    area.innerHTML = `<table>
      <thead><tr>
        <th>Job ID</th><th>Name</th><th>State</th><th>Resource</th><th></th>
      </tr></thead><tbody></tbody></table>`;
    table = area.querySelector('table');
  }
  return table;
}

function addJobRow(page, api, job) {
  const table = ensureTable(page);
  if (!table) return;
  const tbody = table.querySelector('tbody');
  const tr    = document.createElement('tr');
  tr.className     = 'iri-job-row';
  tr.dataset.jobId = job.job_id;

  const st      = (job.state || 'new').toLowerCase();
  const badge   = stateBadge(st);
  const shortId = job.job_id ? job.job_id.slice(0, 12) : '?';

  tr.innerHTML = `
    <td><strong title="${escHtml(job.job_id)}">${escHtml(shortId)}…</strong></td>
    <td>${escHtml(job.name || job.executable || '?')}</td>
    <td><span class="badge ${badge}">${escHtml(st)}</span></td>
    <td>${escHtml(job.resource_id || '?')}</td>
    <td>${CANCELLABLE.has(st)
      ? '<button class="task-cancel-btn iri-cancel-btn" title="Cancel">❌</button>'
      : ''}</td>`;

  tr.addEventListener('click', e => {
    if (e.target.closest('.iri-cancel-btn')) return;
    openJobDetail(api, job.resource_id, job.job_id);
  });

  const cancelBtn = tr.querySelector('.iri-cancel-btn');
  if (cancelBtn) {
    cancelBtn.addEventListener('click', async e => {
      e.stopPropagation();
      cancelBtn.disabled    = true;
      cancelBtn.textContent = '…';
      try {
        await api.fetch(
          `cancel/${encodeURIComponent(job.resource_id)}/${encodeURIComponent(job.job_id)}`,
          { method: 'POST' });
        api.flash(`Job ${shortId}… canceled`);
        updateJobRow(page, job.job_id, 'canceled');
      } catch (err) {
        api.flash('Cancel failed: ' + err.message, false);
        cancelBtn.disabled    = false;
        cancelBtn.textContent = '❌';
      }
    });
  }
  tbody.insertBefore(tr, tbody.firstChild);
}

function updateJobRow(page, jobId, state) {
  const row = page.querySelector(`.iri-job-row[data-job-id="${CSS.escape(jobId)}"]`);
  if (!row) return;
  const badge = row.querySelector('.badge');
  if (badge) {
    badge.textContent = state;
    badge.className   = `badge ${stateBadge(state)}`;
  }
  if (TERMINAL.has(state)) {
    const btn = row.querySelector('.iri-cancel-btn');
    if (btn) btn.remove();
  }
}

async function refreshJobs(page, api) {
  const sel        = page.querySelector('.iri-resource-sel');
  const resourceId = sel?.value?.trim();
  if (!resourceId) return;

  try {
    const data = await api.fetch(
      `jobs/${encodeURIComponent(resourceId)}`,
      { method: 'POST', body: '{}' });
    const jobs = data.jobs || data || [];
    if (!Array.isArray(jobs)) return;

    for (const job of jobs) {
      const jobId = job.job_id || job.id;
      if (!jobId) continue;
      const state = (job.status?.state || job.state || 'unknown').toLowerCase();
      if (iriJobs[jobId]) {
        iriJobs[jobId].state = state;
        updateJobRow(page, jobId, state);
      }
    }
  } catch (e) {
    api.flash('Refresh failed: ' + e.message, false);
  }
}

// ---------------------------------------------------------------------------
//  Job detail overlay
// ---------------------------------------------------------------------------

async function openJobDetail(api, resourceId, jobId) {
  try {
    const status = await api.fetch(
      `status/${encodeURIComponent(resourceId)}/${encodeURIComponent(jobId)}`);

    const st    = (status.status?.state || status.state || 'unknown').toLowerCase();
    const badge = stateBadge(st);

    const fields = [
      ['Job ID',     escHtml(jobId)],
      ['Resource',   escHtml(resourceId)],
      ['State',      `<span class="badge ${badge}">${escHtml(st)}</span>`],
      ['Name',       escHtml(status.name || status.status?.name || '-')],
      ['Executable', escHtml(status.executable || '-')],
      ['Queue',      escHtml(status.queue_name || status.attributes?.queue_name || '-')],
      ['Account',    escHtml(status.account    || status.attributes?.account    || '-')],
      ['Nodes',      status.node_count || status.resources?.node_count || '-'],
    ];

    let body = '<div class="job-detail-grid">';
    for (const [label, value] of fields) {
      body += `<div class="job-detail-item">
        <span class="label">${label}</span>
        <span class="value">${value}</span></div>`;
    }
    body += '</div>';
    body += `<details style="margin-top:16px;">
      <summary style="cursor:pointer;color:var(--muted);">Raw status</summary>
      <pre style="font-size:0.75em;overflow:auto;">${escHtml(JSON.stringify(status, null, 2))}</pre>
    </details>`;

    api.showOverlay(`🔬 Job: ${escHtml(jobId.slice(0, 16))}…`, body);

  } catch (e) {
    api.flash('Error loading job details: ' + e.message, false);
  }
}

// ---------------------------------------------------------------------------
//  Projects & Allocations
// ---------------------------------------------------------------------------

async function loadProjects(page, api) {
  const area = page.querySelector('.iri-projects-area');
  area.innerHTML = '<div class="empty"><div class="spinner"></div>' +
                   '<p style="margin-top:10px">Loading…</p></div>';

  try {
    const data     = await api.fetch('projects');
    const projects = data.projects || data || [];

    if (!Array.isArray(projects) || !projects.length) {
      area.innerHTML = '<p style="color:var(--muted)">No projects returned.</p>';
      return;
    }

    let html = `<table><thead><tr>
      <th>Name</th><th>Description</th><th>Allocations</th>
    </tr></thead><tbody>`;

    for (const proj of projects) {
      const projId   = proj.id || proj.project_id || proj.name || '-';
      const projName = escHtml(proj.name || projId);
      html += `<tr class="iri-project-row" data-project-id="${escHtml(String(projId))}">
        <td><strong>${projName}</strong></td>
        <td>${escHtml(proj.description || '-')}</td>
        <td><button class="btn btn-secondary btn-sm iri-alloc-btn"
            data-project-id="${escHtml(String(projId))}">View</button></td>
      </tr>`;
    }

    html += '</tbody></table>';
    html += '<div class="iri-allocations-area" style="margin-top:16px;"></div>';
    area.innerHTML = html;

    area.querySelectorAll('.iri-alloc-btn').forEach(btn => {
      btn.addEventListener('click', () =>
        loadAllocations(area, api, btn.dataset.projectId));
    });

  } catch (e) {
    area.innerHTML = `<p style="color:var(--danger)">Error: ${escHtml(e.message)}</p>`;
  }
}

async function loadAllocations(area, api, projectId) {
  const allocArea = area.querySelector('.iri-allocations-area');
  if (!allocArea) return;
  allocArea.innerHTML = '<div class="empty"><div class="spinner"></div></div>';

  try {
    const data   = await api.fetch(`allocations/${encodeURIComponent(projectId)}`);
    const allocs = data.allocations || data || [];

    if (!Array.isArray(allocs) || !allocs.length) {
      allocArea.innerHTML = `<p style="color:var(--muted)">No allocations for ${escHtml(projectId)}.</p>`;
      return;
    }

    let html = `<div class="card-title" style="margin-top:8px;">
      Allocations: ${escHtml(projectId)}
    </div>
    <table><thead><tr>
      <th>Resource</th><th>Allocation</th><th>Used</th><th>Remaining</th><th>Expires</th>
    </tr></thead><tbody>`;

    for (const a of allocs) {
      html += `<tr>
        <td>${escHtml(a.resource || a.resource_id || '-')}</td>
        <td>${escHtml(String(a.allocation || a.hours || '-'))}</td>
        <td>${escHtml(String(a.used       || '-'))}</td>
        <td>${escHtml(String(a.remaining  || '-'))}</td>
        <td>${escHtml(a.expiration_date   || a.end_date || '-')}</td>
      </tr>`;
    }

    html += '</tbody></table>';
    allocArea.innerHTML = html;

  } catch (e) {
    allocArea.innerHTML = `<p style="color:var(--danger)">Error: ${escHtml(e.message)}</p>`;
  }
}

// ---------------------------------------------------------------------------
//  Disconnect (calls iri_connect plugin)
// ---------------------------------------------------------------------------

async function doDisconnect(page, api) {
  // Extract endpoint key from instance name (e.g. 'iri.nersc' -> 'nersc')
  const pluginName = api.pluginName || '';
  const ep = pluginName.startsWith('iri.') ? pluginName.slice(4) : pluginName;

  try {
    // Call iri_connect's disconnect endpoint on the same endpoint
    await api.fetch(`disconnect/${encodeURIComponent(ep)}`, {
      method: 'POST',
    }, 'iri_connect');

    api.flash(`Disconnected ${pluginName}`);
  } catch (e) {
    api.flash('Disconnect failed: ' + e.message, false);
  }
}

// ---------------------------------------------------------------------------
//  Helpers
// ---------------------------------------------------------------------------

function addEnvRow(page, key = '', value = '') {
  const container = page.querySelector('.iri-envvar-rows');
  const row       = document.createElement('div');
  row.style.cssText = 'display:flex;gap:10px;margin-bottom:8px;';
  row.innerHTML = `
    <input class="iri-env-key" type="text" placeholder="KEY" style="flex:1;"
           value="${escHtml(key)}" />
    <input class="iri-env-val" type="text" placeholder="value" style="flex:2;"
           value="${escHtml(value)}" />
    <button class="task-cancel-btn">❌</button>`;
  row.querySelector('button').addEventListener('click', () => row.remove());
  container.appendChild(row);
}

function stateBadge(state) {
  const s = (state || '').toLowerCase();
  if (s === 'completed')                      return 'badge-green';
  if (s === 'active')                         return 'badge-blue';
  if (['failed', 'canceled'].includes(s))     return 'badge-red';
  return 'badge-orange';
}

function cleanIRIError(msg) {
  // Extract the "detail" field from the HTTP error JSON wrapper.
  // The Python _iri_raise already produces a clean message; we just
  // need to unwrap the "HTTP NNN: {json}" envelope added by apiFetch.
  try {
    const s = msg.replace(/^HTTP \d+:\s*/, '');
    const obj = JSON.parse(s);
    return obj.detail || obj.message || msg;
  } catch (_) {
    return msg;
  }
}
