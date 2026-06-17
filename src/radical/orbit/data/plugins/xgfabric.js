/**
 * XGFabric Plugin Module for ORBIT Explorer
 *
 * CFDaAI workflow orchestrator for HPC clusters.
 */

export const name = 'xgfabric';

// Shared with api.escHtml — set in init()
let escHtml = s => String(s || '');  // safe fallback until init()

// Module-level state
let currentConfig = null;
const sessions = {};      // endpointName -> sid
const statusCache = {};   // endpointName -> last loaded status

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">🌊</div>
      <h2>XGFabric Workflow — <span class="endpoint-label"></span></h2>
      <a href="#" class="btn btn-secondary btn-sm" style="margin-left:auto" data-action="api-docs">REST API</a>
      <button class="btn btn-secondary btn-sm" data-action="refresh">↺ Refresh</button>
      <button class="btn btn-danger btn-sm" data-action="terminate">Terminate</button>
    </div>

    <!-- Configuration Section -->
    <div class="card">
      <div class="card-title">📋 Configuration</div>
      <div class="form-group">
        <label>Config Directory</label>
        <div style="display:flex; gap:8px;">
          <input class="xgf-workdir" type="text" style="flex:1" placeholder="/path/to/configs" />
          <button class="btn btn-secondary btn-sm" data-action="set-workdir">Set</button>
        </div>
      </div>
      <div class="form-group">
        <label>Active Config</label>
        <div style="display:flex; gap:8px; align-items:center;">
          <select class="xgf-config-select" style="flex:1">
            <option value="">-- Select Config --</option>
          </select>
          <button class="btn btn-secondary btn-sm" data-action="edit-config">✏️ Edit</button>
          <button class="btn btn-success btn-sm xgf-start-btn" data-action="start-workflow">▶ Start</button>
        </div>
      </div>
    </div>

    <!-- Workflow Status Section -->
    <div class="card xgf-status-card">
      <div class="card-title">📊 Workflow Status</div>
      <div class="xgf-status-content">
        <div class="xgf-status-idle">
          <p style="color:var(--muted)">No workflow running. Select a config and click Start.</p>
        </div>
      </div>
    </div>

    <!-- Cluster Status Section -->
    <div class="card">
      <div class="card-title">🖥️ Cluster Status</div>
      <div class="grid2">
        <div>
          <h4 style="margin:0 0 8px 0; color:var(--muted)">Immediate Clusters</h4>
          <div class="xgf-immediate-clusters">
            <div class="xgf-cluster-empty" style="padding:12px; border:1px dashed var(--border); border-radius:4px; color:var(--muted); text-align:center;">
              No immediate clusters configured
            </div>
          </div>
        </div>
        <div>
          <h4 style="margin:0 0 8px 0; color:var(--muted)">Allocate Clusters (GPU)</h4>
          <div class="xgf-allocate-clusters">
            <div class="xgf-cluster-empty" style="padding:12px; border:1px dashed var(--border); border-radius:4px; color:var(--muted); text-align:center;">
              No allocate clusters configured
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Execution Log Section -->
    <div class="card">
      <div class="card-title">📜 Execution Log</div>
      <div class="xgf-log" style="max-height:200px; overflow-y:auto; font-family:monospace; font-size:12px;">
        <p style="color:var(--muted)">No log entries yet.</p>
      </div>
    </div>
  `;
}

export function overlayTemplate() {
  return `
    <div id="xgf-config-overlay" class="overlay">
      <div class="overlay-content" style="max-width:600px;">
        <div class="overlay-header">
          <h3>Edit Configuration</h3>
          <button class="task-cancel-btn overlay-close" data-action="close-overlay">❌</button>
        </div>
        <div class="overlay-body">
          <div class="form-group">
            <label>Config Name</label>
            <input class="xgf-edit-name" type="text" placeholder="my_config" />
          </div>
          <div class="form-group">
            <label>Description</label>
            <input class="xgf-edit-description" type="text" placeholder="Optional description" />
          </div>

          <h4 style="margin:16px 0 8px 0; border-bottom:1px solid var(--border); padding-bottom:4px;">CSPOT Settings</h4>
          <div class="grid2">
            <div class="form-group">
              <label>WOOF URL</label>
              <input class="xgf-edit-cspot-url" type="text" />
            </div>
            <div class="form-group">
              <label>Record Limit</label>
              <input class="xgf-edit-cspot-limit" type="number" />
            </div>
          </div>

          <h4 style="margin:16px 0 8px 0; border-bottom:1px solid var(--border); padding-bottom:4px;">Workflow Settings</h4>
          <div class="grid2">
            <div class="form-group">
              <label>Simulations</label>
              <input class="xgf-edit-num-sims" type="number" />
            </div>
            <div class="form-group">
              <label>Batch Size</label>
              <input class="xgf-edit-batch-size" type="number" />
            </div>
          </div>

          <h4 style="margin:16px 0 8px 0; border-bottom:1px solid var(--border); padding-bottom:4px;">Clusters</h4>
          <div class="xgf-edit-clusters" style="font-size:12px; color:var(--muted);">
            <p>Cluster configuration is managed via the config file directly.</p>
          </div>
        </div>
        <div class="overlay-footer">
          <button class="btn btn-secondary" data-action="close-overlay">Cancel</button>
          <button class="btn btn-primary" data-action="save-as">📄 Save As New</button>
          <button class="btn btn-success" data-action="save">💾 Save</button>
        </div>
      </div>
    </div>
  `;
}

export function css() {
  return `
    .xgf-status-content .progress-wrap {
      margin: 4px 0;
    }
    .xgf-cluster-empty {
      padding: 12px;
      border: 1px dashed var(--border);
      border-radius: 4px;
      color: var(--muted);
      text-align: center;
    }
    .overlay-footer {
      display: flex;
      justify-content: flex-end;
      gap: 8px;
      padding: 12px 16px;
      border-top: 1px solid var(--border);
    }
  `;
}

export function init(page, api) {
  escHtml = api.escHtml;

  // Bind buttons
  page.querySelector('[data-action="api-docs"]')?.addEventListener('click', (e) => {
    e.preventDefault();
    window.open(`/${api.endpointName}/docs`, '_blank');
  });

  page.querySelector('[data-action="refresh"]')?.addEventListener('click', () => loadXgfabric(page, api));
  page.querySelector('[data-action="terminate"]')?.addEventListener('click', (e) => api.disconnectEndpoint(e));
  page.querySelector('[data-action="set-workdir"]')?.addEventListener('click', () => setWorkdir(page, api));
  page.querySelector('[data-action="edit-config"]')?.addEventListener('click', () => editConfig(page, api));
  page.querySelector('[data-action="start-workflow"]')?.addEventListener('click', (e) => startWorkflow(page, api, e.target));

  // Update start button when config selection changes
  page.querySelector('.xgf-config-select')?.addEventListener('change', () => {
    updateStartButton(page);
  });

  // Create overlay if needed
  ensureOverlay(api);

  // Auto-load
  loadXgfabric(page, api);
}

export function onNotification(data, page, api) {
  console.log('[xgfabric] onNotification:', data.topic,
              'immediate:', data.data?.immediate_clusters?.map(c=>c.name),
              'allocate:',  data.data?.allocate_clusters?.map(c=>c.name));
  if (data.topic === 'workflow_status') {
    renderStatus(page, api, data.data);
  }
}


// ─────────────────────────────────────────────────────────────
//  Internal functions
// ─────────────────────────────────────────────────────────────

async function getSession(api) {
  if (!sessions[api.endpointName]) {
    // Store promise immediately to prevent concurrent callers from racing
    sessions[api.endpointName] = api.getSession('xgfabric');
  }
  return await sessions[api.endpointName];
}

function ensureOverlay(api) {
  if (document.getElementById('xgf-config-overlay')) return;

  const container = document.createElement('div');
  container.innerHTML = overlayTemplate();
  document.body.appendChild(container.firstElementChild);

  const overlay = document.getElementById('xgf-config-overlay');

  // Bind overlay actions
  overlay.querySelector('[data-action="close-overlay"]')?.addEventListener('click', () => closeOverlay());
  overlay.querySelectorAll('[data-action="close-overlay"]').forEach(el => {
    el.addEventListener('click', () => closeOverlay());
  });
  overlay.querySelector('[data-action="save"]')?.addEventListener('click', () => saveConfig(api, false));
  overlay.querySelector('[data-action="save-as"]')?.addEventListener('click', () => saveConfig(api, true));

  // Close on click outside
  overlay.addEventListener('click', (e) => {
    if (e.target === overlay) closeOverlay();
  });

  // Close on Escape
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && overlay.classList.contains('visible')) {
      closeOverlay();
    }
  });
}

async function loadXgfabric(page, api) {
  try {
    const sid = await getSession(api);

    // Load status
    const status = await api.fetch(`status/${sid}`);
    console.log('[xgfabric] loadXgfabric status:', status.status,
                'immediate:', status.immediate_clusters?.map(c=>c.name),
                'allocate:',  status.allocate_clusters?.map(c=>c.name));
    statusCache[api.endpointName] = status;
    page._xgfStatus = status;  // Store on page for reliable access
    renderStatus(page, api, status);

    // Load configs list
    const configs = await api.fetch(`configs/${sid}`);
    renderConfigDropdown(page, configs);

    // Set workdir
    page.querySelector('.xgf-workdir').value = status.config_dir || '';

    // Update start button state
    updateStartButton(page, status);

  } catch (e) {
    api.flash('XGFabric error: ' + e.message, false);
  }
}

function renderStatus(page, api, status) {
  const container = page.querySelector('.xgf-status-content');
  const logContainer = page.querySelector('.xgf-log');

  if (status.status === 'idle') {
    container.innerHTML = `
      <div class="xgf-status-idle">
        <p style="color:var(--muted)">No workflow running. Select a config and click Start.</p>
      </div>`;
  } else {
    const statusColor = status.status === 'running' ? 'var(--accent)' :
                        status.status === 'completed' ? 'var(--success)' :
                        status.status === 'failed' ? 'var(--danger)' : 'var(--muted)';
    const statusDot = status.status === 'running' ? '●' : status.status === 'completed' ? '✓' : '✗';

    container.innerHTML = `
      <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-bottom:12px;">
        <div><strong>Status:</strong> <span style="color:${statusColor}">${statusDot} ${status.status.toUpperCase()}</span></div>
        <div><strong>Phase:</strong> ${escHtml(status.phase || '-')}</div>
        <div><strong>Started:</strong> ${status.start_time ? new Date(status.start_time).toLocaleString() : '-'}</div>
        <div><strong>Active Cluster:</strong> ${escHtml(status.active_cluster || '-')}</div>
      </div>
      <div style="margin-bottom:8px;">
        <div style="background:var(--hover); border-radius:4px; height:20px; overflow:hidden;">
          <div style="background:var(--accent); height:100%; width:${status.progress}%; transition:width 0.3s;"></div>
        </div>
        <div style="text-align:center; font-size:12px; margin-top:4px;">${status.progress}%</div>
      </div>
      <div style="margin-bottom:12px;">${escHtml(status.message || '')}</div>
      ${status.status === 'running' ? `
        <div style="margin-bottom:8px;">
          <strong>Batch:</strong> ${status.current_batch}/${status.total_batches} |
          <strong>Simulations:</strong> ${status.completed_simulations}/${status.total_simulations}
        </div>
        <button class="btn btn-danger btn-sm" data-action="stop-workflow">⏹ Stop Workflow</button>
      ` : ''}
      ${status.error ? `<div style="color:var(--danger); margin-top:8px;"><strong>Error:</strong> ${escHtml(status.error)}</div>` : ''}
    `;

    // Bind stop button if present
    const stopBtn = container.querySelector('[data-action="stop-workflow"]');
    if (stopBtn) {
      stopBtn.addEventListener('click', () => stopWorkflow(page, api, stopBtn));
    }
  }

  // Render clusters
  renderClusters(page, status.immediate_clusters || [], status.allocate_clusters || []);

  // Render log
  if (status.log && status.log.length > 0) {
    logContainer.innerHTML = status.log.map(entry =>
      `<div style="padding:2px 0;"><span style="color:var(--muted)">${escHtml(entry.time)}</span> ${escHtml(entry.message)}</div>`
    ).join('');
  } else {
    logContainer.innerHTML = '<p style="color:var(--muted)">No log entries yet.</p>';
  }
}

function renderClusters(page, immediate, allocate) {
  console.log('[xgfabric] renderClusters: immediate=', immediate, 'allocate=', allocate);
  const immediateContainer = page.querySelector('.xgf-immediate-clusters');
  const allocateContainer = page.querySelector('.xgf-allocate-clusters');

  if (immediate.length === 0) {
    immediateContainer.innerHTML = `
      <div class="xgf-cluster-empty">
        No immediate clusters configured
      </div>`;
  } else {
    immediateContainer.innerHTML = immediate.map(c => `
      <div style="padding:8px; margin-bottom:8px; border:1px solid var(--border); border-radius:4px;">
        <div style="display:flex; justify-content:space-between; align-items:center;">
          <strong>${escHtml(c.name)}</strong>
          <span style="color:${c.online ? 'var(--success)' : 'var(--danger)'}">
            ${c.online ? '● online' : '○ offline'}
          </span>
        </div>
        <div style="font-size:12px; color:var(--muted); margin-top:4px;">
          Endpoint: ${escHtml(c.endpoint_name)} | GPU: ${c.has_gpu ? '✓' : '✗'}
          ${c.tasks_running > 0 ? ` | Tasks: ${c.tasks_running}` : ''}
        </div>
      </div>
    `).join('');
  }

  if (allocate.length === 0) {
    allocateContainer.innerHTML = `
      <div class="xgf-cluster-empty">
        No allocate clusters configured
      </div>`;
  } else {
    allocateContainer.innerHTML = allocate.map(c => `
      <div style="padding:8px; margin-bottom:8px; border:1px solid var(--border); border-radius:4px;">
        <div style="display:flex; justify-content:space-between; align-items:center;">
          <strong>${escHtml(c.name)}</strong>
          <span style="color:${c.online ? 'var(--success)' : 'var(--danger)'}">
            ${c.online ? '● online' : '○ offline'}
          </span>
        </div>
        <div style="font-size:12px; color:var(--muted); margin-top:4px;">
          Endpoint: ${escHtml(c.endpoint_name)} | GPU: ${c.has_gpu ? '✓' : '✗'}
          ${c.pilot_job_id ? ` | Pilot: ${c.pilot_status || 'pending'} (${c.pilot_job_id})` : ''}
        </div>
      </div>
    `).join('');
  }
}

function renderConfigDropdown(page, configs) {
  const select = page.querySelector('.xgf-config-select');
  const currentValue = select.value;
  select.innerHTML = '<option value="">-- Select Config --</option>';

  // Always include default config option
  const defaultOpt = document.createElement('option');
  defaultOpt.value = '__default__';
  defaultOpt.textContent = 'default (built-in template)';
  select.appendChild(defaultOpt);

  // Add saved configs
  configs.forEach(c => {
    const opt = document.createElement('option');
    opt.value = c.name;
    opt.textContent = c.name + (c.description ? ` (${c.description})` : '');
    select.appendChild(opt);
  });

  if (currentValue) select.value = currentValue;
}

function updateStartButton(page, status = null) {
  const startBtn = page.querySelector('.xgf-start-btn');
  const configSelect = page.querySelector('.xgf-config-select');
  // Use passed status, or get from page, or fallback to empty
  const st = status || page._xgfStatus || {};
  const hasConfig = configSelect.value !== '';
  const isRunning = st.status === 'running';

  // Enable button when a config is selected and workflow is not running
  // Cluster availability is validated by the backend when starting
  startBtn.disabled = !hasConfig || isRunning;
  if (!hasConfig) {
    startBtn.title = 'Select a configuration first';
  } else if (isRunning) {
    startBtn.title = 'Workflow already running';
  } else {
    startBtn.title = 'Start workflow';
  }
}

async function setWorkdir(page, api) {
  const path = page.querySelector('.xgf-workdir').value.trim();

  if (!path) {
    api.flash('Please enter a path', false);
    return;
  }

  try {
    const sid = await getSession(api);
    await api.fetch(`workdir/${sid}`, {
      method: 'POST',
      body: JSON.stringify({ path })
    });
    api.flash('Config directory updated');
    await loadXgfabric(page, api);
  } catch (e) {
    api.flash('Failed to set directory: ' + e.message, false);
  }
}

async function editConfig(page, api) {
  const configName = page.querySelector('.xgf-config-select').value;

  const overlay = document.getElementById('xgf-config-overlay');
  overlay.dataset.endpointName = api.endpointName;
  overlay.classList.add('visible');

  try {
    const sid = await getSession(api);

    let config;
    if (configName && configName !== '__default__') {
      config = await api.fetch(`config/${sid}/${configName}`);
    } else {
      config = await api.fetch(`config/${sid}/default`);
    }

    currentConfig = config;

    // Fill form
    overlay.querySelector('.xgf-edit-name').value = config.name || '';
    overlay.querySelector('.xgf-edit-description').value = config.description || '';
    overlay.querySelector('.xgf-edit-cspot-url').value = config.cspot_woof_url || '';
    overlay.querySelector('.xgf-edit-cspot-limit').value = config.cspot_limit || 72;
    overlay.querySelector('.xgf-edit-num-sims').value = config.num_simulations || 16;
    overlay.querySelector('.xgf-edit-batch-size').value = config.batch_size || 4;

  } catch (e) {
    api.flash('Failed to load config: ' + e.message, false);
    closeOverlay();
  }
}

function closeOverlay() {
  document.getElementById('xgf-config-overlay').classList.remove('visible');
  currentConfig = null;
}

async function saveConfig(api, saveAs) {
  const overlay = document.getElementById('xgf-config-overlay');
  const endpointName = overlay.dataset.endpointName;

  let newName = null;
  if (saveAs) {
    newName = prompt('Enter new config name:');
    if (!newName) return;
  }

  const config = {
    ...currentConfig,
    name: saveAs ? newName : overlay.querySelector('.xgf-edit-name').value,
    description: overlay.querySelector('.xgf-edit-description').value,
    cspot_woof_url: overlay.querySelector('.xgf-edit-cspot-url').value,
    cspot_limit: parseInt(overlay.querySelector('.xgf-edit-cspot-limit').value) || 72,
    num_simulations: parseInt(overlay.querySelector('.xgf-edit-num-sims').value) || 16,
    batch_size: parseInt(overlay.querySelector('.xgf-edit-batch-size').value) || 4,
  };

  try {
    const sid = sessions[endpointName];
    await api.fetchRaw(`/${endpointName}/xgfabric/config/${sid}`, {
      method: 'POST',
      body: JSON.stringify(config)
    });
    api.flash(`Config '${config.name}' saved`);
    closeOverlay();

    // Refresh the page
    const page = document.querySelector(`[data-endpoint-name="${endpointName}"][data-plugin="xgfabric"]`);
    if (page) {
      await loadXgfabric(page, api);
      page.querySelector('.xgf-config-select').value = config.name;
    }
  } catch (e) {
    api.flash('Failed to save config: ' + e.message, false);
  }
}

async function startWorkflow(page, api, btn) {
  const configName = page.querySelector('.xgf-config-select').value;

  if (!configName) {
    api.flash('Please select a configuration first', false);
    return;
  }

  btn.disabled = true;
  try {
    const sid = await getSession(api);
    await api.fetch(`start/${sid}`, {
      method: 'POST',
      body: JSON.stringify({ config_name: configName })
    });
    api.flash(`Workflow started with config: ${configName}`);
    await loadXgfabric(page, api);
  } catch (e) {
    api.flash('Failed to start workflow: ' + e.message, false);
    btn.disabled = false;
  }
}

async function stopWorkflow(page, api, btn) {
  btn.disabled = true;
  try {
    const sid = await getSession(api);
    await api.fetch(`stop/${sid}`, { method: 'POST' });
    api.flash('Workflow stopped');
    await loadXgfabric(page, api);
  } catch (e) {
    api.flash('Failed to stop workflow: ' + e.message, false);
    btn.disabled = false;
  }
}

// ─────────────────────────────────────────────────────────────
//  Utility functions
// ─────────────────────────────────────────────────────────────

