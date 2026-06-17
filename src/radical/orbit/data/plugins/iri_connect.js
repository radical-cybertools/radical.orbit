/**
 * IRI Connect Plugin Module for ORBIT Explorer
 *
 * Endpoint configurator: connect/disconnect to IRI endpoints.
 * On connect, a dynamic iri.<endpoint> plugin instance appears in the tree.
 * Tokens are stored in localStorage (client-side only).
 */

export const name = 'iri_connect';

let escHtml = s => String(s || '');

// ---------------------------------------------------------------------------
//  localStorage helpers (shared key with iri_instance.js)
// ---------------------------------------------------------------------------

function getTokens() {
  try { return JSON.parse(localStorage.getItem('iri_tokens') || '{}'); }
  catch (_) { return {}; }
}

function setToken(ep, token) {
  const t = getTokens();
  t[ep] = token;
  localStorage.setItem('iri_tokens', JSON.stringify(t));
}

// ---------------------------------------------------------------------------
//  Template
// ---------------------------------------------------------------------------

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">🔌</div>
      <h2>IRI Connect — <span class="endpoint-label"></span></h2>
    </div>

    <div class="card">
      <div class="card-title">🌐 Endpoints</div>
      <div class="iri-connect-endpoints-area">
        <div class="empty"><div class="spinner"></div>
          <p style="margin-top:10px">Loading…</p></div>
      </div>
    </div>

    <!-- Token popup -->
    <div class="iri-connect-token-overlay overlay">
      <div class="overlay-content" style="max-width:520px;">
        <div class="overlay-header">
          <h3>🔑 Connect — <span class="iri-connect-popup-ep-label"></span></h3>
          <button class="task-cancel-btn overlay-close iri-connect-popup-close">❌</button>
        </div>
        <div class="overlay-body">
          <div class="form-group">
            <label>Bearer Token</label>
            <textarea class="iri-connect-popup-token" rows="5"
              placeholder="Paste bearer token here"
              autocomplete="off"
              style="font-family:monospace;font-size:0.82em;resize:vertical;width:100%;"></textarea>
          </div>
          <div style="display:flex;align-items:center;gap:12px;margin-top:12px;">
            <button class="btn btn-primary iri-connect-popup-go">🔌 Connect</button>
            <span class="iri-connect-popup-status" style="color:var(--muted);font-size:0.85em;"></span>
          </div>
        </div>
      </div>
    </div>
  `;
}

export function css() {
  return `
    .iri-connect-popup-token { font-family: monospace; }
    .iri-connect-status-ok   { color: var(--success); font-weight: 600; }
    .iri-connect-status-off  { color: var(--muted); }
  `;
}

// ---------------------------------------------------------------------------
//  Lifecycle hooks
// ---------------------------------------------------------------------------

let _popupEp = null;

export async function init(page, api) {
  escHtml = api.escHtml;

  // Popup bindings
  page.querySelector('.iri-connect-popup-close')
      .addEventListener('click', () => closePopup(page));
  page.querySelector('.iri-connect-token-overlay')
      .addEventListener('click', e => {
        if (e.target === e.currentTarget) closePopup(page);
      });
  page.querySelector('.iri-connect-popup-go')
      .addEventListener('click', () => doConnect(page, api));

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
  const area = page.querySelector('.iri-connect-endpoints-area');
  let eps;
  try {
    eps = await api.fetch('endpoints');
  } catch (e) {
    area.innerHTML = `<p style="color:var(--danger)">Error: ${escHtml(e.message)}</p>`;
    return;
  }

  const tokens = getTokens();

  let html = `<table><thead><tr>
    <th>Name</th>
    <th>URL</th>
    <th style="width:100px;text-align:center;">Status</th>
    <th style="width:140px;text-align:center;">Action</th>
  </tr></thead><tbody>`;

  for (const [key, ep] of Object.entries(eps)) {
    const connected = !!ep.connected;
    const hasToken  = !!(tokens[key] && tokens[key].trim());

    const statusHtml = connected
      ? `<span class="iri-connect-status-ok">Connected</span>`
      : `<span class="iri-connect-status-off">—</span>`;

    let actionHtml;
    if (connected) {
      actionHtml = `<button class="btn btn-secondary btn-sm iri-disconnect-btn"
                      data-ep="${escHtml(key)}">Disconnect</button>`;
    } else {
      actionHtml = `<button class="btn btn-primary btn-sm iri-do-connect-btn"
                      data-ep="${escHtml(key)}">${hasToken ? '🔑 Connect' : 'Connect'}</button>`;
    }

    html += `<tr>
      <td><strong>${escHtml(ep.label || key)}</strong></td>
      <td style="font-family:monospace;font-size:0.85em;">${escHtml(ep.url || '')}</td>
      <td style="text-align:center;">${statusHtml}</td>
      <td style="text-align:center;">${actionHtml}</td>
    </tr>`;
  }

  html += '</tbody></table>';
  area.innerHTML = html;

  // Bind connect buttons
  area.querySelectorAll('.iri-do-connect-btn').forEach(btn => {
    btn.addEventListener('click', () => openPopup(page, btn.dataset.ep));
  });

  // Bind disconnect buttons
  area.querySelectorAll('.iri-disconnect-btn').forEach(btn => {
    btn.addEventListener('click', () => doDisconnect(page, api, btn.dataset.ep));
  });
}

// ---------------------------------------------------------------------------
//  Token popup
// ---------------------------------------------------------------------------

function openPopup(page, ep) {
  _popupEp = ep;
  const overlay = page.querySelector('.iri-connect-token-overlay');
  const tokens  = getTokens();

  page.querySelector('.iri-connect-popup-ep-label').textContent = ep;
  page.querySelector('.iri-connect-popup-token').value = tokens[ep] || '';
  page.querySelector('.iri-connect-popup-status').textContent = '';

  overlay.classList.add('visible');
  page.querySelector('.iri-connect-popup-token').focus();
}

function closePopup(page) {
  page.querySelector('.iri-connect-token-overlay').classList.remove('visible');
  _popupEp = null;
}

async function doConnect(page, api) {
  if (!_popupEp) return;

  const ep     = _popupEp;
  const token  = page.querySelector('.iri-connect-popup-token').value.trim();
  const status = page.querySelector('.iri-connect-popup-status');

  if (!token) {
    status.textContent = 'Enter a token first.';
    status.style.color = 'var(--danger)';
    return;
  }

  status.textContent = 'Connecting…';
  status.style.color = 'var(--muted)';

  try {
    await api.fetch('connect', {
      method: 'POST',
      body: JSON.stringify({ endpoint: ep, token }),
    });

    setToken(ep, token);
    closePopup(page);
    api.flash(`Connected to iri.${ep}`);
    // Topology SSE event will update the tree; re-render our table too
    await renderTable(page, api);

  } catch (e) {
    closePopup(page);
    api.showOverlay('❌ Connection Failed',
      `<p style="color:var(--danger)">${escHtml(e.message)}</p>
       <p style="color:var(--muted);margin-top:8px;">
         Check your token and try again.
       </p>`);
  }
}

async function doDisconnect(page, api, ep) {
  try {
    await api.fetch(`disconnect/${encodeURIComponent(ep)}`, { method: 'POST' });
    api.flash(`Disconnected iri.${ep}`);
    await renderTable(page, api);
  } catch (e) {
    api.flash('Disconnect failed: ' + e.message, false);
  }
}
