/**
 * SFAPI Connect Plugin Module for ORBIT Explorer
 *
 * Endpoint configurator: view SFAPI endpoints (NERSC) and their status.
 * On connect, a dynamic sfapi.<endpoint> plugin instance appears in the tree.
 *
 * SFAPI authenticates with an OAuth2 client id + RSA private key, NOT a
 * paste-able bearer token — private keys must never be entered into a
 * browser.  This page is therefore read-only: connect programmatically via
 * the client API (``sfapi_connect.connect(endpoint, client_id, private_key)``);
 * disconnect is available here since it carries no credential.
 */

export const name = 'sfapi_connect';

let escHtml = s => String(s || '');

// ---------------------------------------------------------------------------
//  Template
// ---------------------------------------------------------------------------

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">🔐</div>
      <h2>SFAPI Connect — <span class="endpoint-label"></span></h2>
    </div>

    <div class="card">
      <div class="card-title">🌐 Endpoints</div>
      <p style="color:var(--muted);font-size:0.85em;margin:0 0 10px;">
        SFAPI endpoints authenticate with a client id + private key.  Private
        keys must not be pasted into a browser — connect programmatically via
        the client API:
        <code>sfapi_connect.connect(endpoint, client_id, private_key)</code>.
      </p>
      <div class="sfapi-connect-endpoints-area">
        <div class="empty"><div class="spinner"></div>
          <p style="margin-top:10px">Loading…</p></div>
      </div>
    </div>
  `;
}

export function css() {
  return `
    .sfapi-connect-status-ok   { color: var(--success); font-weight: 600; }
    .sfapi-connect-status-off  { color: var(--muted); }
  `;
}

// ---------------------------------------------------------------------------
//  Lifecycle hooks
// ---------------------------------------------------------------------------

export async function init(page, api) {
  escHtml = api.escHtml;
  await renderTable(page, api);
}

export async function onShow(page, api) {
  await renderTable(page, api);
}

export function onNotification() {}

// ---------------------------------------------------------------------------
//  Endpoint table
// ---------------------------------------------------------------------------

async function renderTable(page, api) {
  const area = page.querySelector('.sfapi-connect-endpoints-area');
  let eps;
  try {
    eps = await api.fetch('endpoints');
  } catch (e) {
    area.innerHTML = `<p style="color:var(--danger)">Error: ${escHtml(e.message)}</p>`;
    return;
  }

  let html = `<table><thead><tr>
    <th>Name</th>
    <th>URL</th>
    <th style="width:100px;text-align:center;">Status</th>
    <th style="width:140px;text-align:center;">Action</th>
  </tr></thead><tbody>`;

  for (const [key, ep] of Object.entries(eps)) {
    const connected = !!ep.connected;

    const statusHtml = connected
      ? `<span class="sfapi-connect-status-ok">Connected</span>`
      : `<span class="sfapi-connect-status-off">—</span>`;

    // Connect happens via the client API (client id + private key) — the
    // Explorer only offers disconnect, which carries no credential.
    const actionHtml = connected
      ? `<button class="btn btn-secondary btn-sm sfapi-disconnect-btn"
                   data-ep="${escHtml(key)}">Disconnect</button>`
      : `<span class="sfapi-connect-status-off"
                   title="Connect via the client API with client_id + private key"
                   >client credential</span>`;

    html += `<tr>
      <td><strong>${escHtml(ep.label || key)}</strong></td>
      <td style="font-family:monospace;font-size:0.85em;">${escHtml(ep.url || '')}</td>
      <td style="text-align:center;">${statusHtml}</td>
      <td style="text-align:center;">${actionHtml}</td>
    </tr>`;
  }

  html += '</tbody></table>';
  area.innerHTML = html;

  // Bind disconnect buttons
  area.querySelectorAll('.sfapi-disconnect-btn').forEach(btn => {
    btn.addEventListener('click', () => doDisconnect(page, api, btn.dataset.ep));
  });
}

async function doDisconnect(page, api, ep) {
  try {
    await api.fetch(`disconnect/${encodeURIComponent(ep)}`, { method: 'POST' });
    api.flash(`Disconnected sfapi.${ep}`);
    await renderTable(page, api);
  } catch (e) {
    api.flash('Disconnect failed: ' + e.message, false);
  }
}
