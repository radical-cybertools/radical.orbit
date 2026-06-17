/**
 * Staging Plugin Module for ORBIT Explorer
 *
 * File transfer between client and endpoint (upload/download).
 */

export const name = 'staging';

// Shared with api.escHtml — set in init()
let escHtml = s => String(s || '');  // safe fallback until init()

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">📁</div>
      <h2>File Staging — <span class="endpoint-label"></span></h2>
    </div>
    <div class="card">
      <div class="card-title">📂 Remote File Browser</div>
      <div style="display:flex; gap:8px; margin-bottom:12px; align-items:center;">
        <label style="margin:0; min-width:fit-content;">Path:</label>
        <input class="staging-path" type="text" value="/" style="flex:1;" />
        <button class="btn btn-secondary btn-sm" data-action="browse">📂 Browse</button>
        <button class="btn btn-secondary btn-sm" data-action="go-up">⬆️ Up</button>
      </div>
      <div class="staging-browser" style="border:1px solid var(--border); border-radius:var(--card-r); max-height:400px; overflow-y:auto; background:var(--bg1);">
        <div class="empty" style="padding:20px;">
          <div class="empty-icon">📂</div>
          <p>Enter a path and click Browse to view directory contents</p>
        </div>
      </div>
    </div>
    <div class="card">
      <div class="card-title">📤 Upload File</div>
      <div class="grid2">
        <div class="form-group">
          <label>Local File</label>
          <input type="file" class="staging-upload-file" />
        </div>
        <div class="form-group">
          <label>Remote Path (absolute)</label>
          <input class="staging-upload-path" type="text" placeholder="/path/on/endpoint/filename.txt" />
        </div>
      </div>
      <button class="btn btn-success" data-action="upload">📤 Upload</button>
      <div class="staging-upload-status" style="margin-top:8px; font-size:0.85rem;"></div>
    </div>
    <div class="card">
      <div class="card-title">📥 Download File</div>
      <div class="form-group">
        <label>Remote Path (absolute)</label>
        <input class="staging-download-path" type="text" placeholder="/path/on/endpoint/filename.txt" />
      </div>
      <button class="btn btn-primary" data-action="download">📥 Download</button>
      <div class="staging-download-status" style="margin-top:8px; font-size:0.85rem;"></div>
    </div>
  `;
}

export function css() {
  return `
    .staging-browser .tree {
      margin: 0;
      padding: 8px 0;
      list-style: none;
    }
    .staging-browser .tree-item {
      display: flex;
      align-items: center;
      gap: 8px;
      cursor: pointer;
      transition: background 0.15s;
    }
    .staging-browser .tree-item:hover {
      background: var(--hover);
    }
    .staging-browser .tree-item .icon {
      flex-shrink: 0;
    }
  `;
}

export function init(page, api) {
  escHtml = api.escHtml;

  // Bind browse button
  page.querySelector('[data-action="browse"]')?.addEventListener('click', () => navigate(page, api));

  // Bind go-up button
  page.querySelector('[data-action="go-up"]')?.addEventListener('click', () => goUp(page, api));

  // Bind upload button
  page.querySelector('[data-action="upload"]')?.addEventListener('click', () => upload(page, api));

  // Bind download button
  page.querySelector('[data-action="download"]')?.addEventListener('click', () => download(page, api));
}

export function onNotification(data, page, api) {
  // Staging doesn't use notifications
}

// ─────────────────────────────────────────────────────────────
//  Internal functions
// ─────────────────────────────────────────────────────────────

async function navigate(page, api, targetPath = null) {
  const pathInput = page.querySelector('.staging-path');
  const browser = page.querySelector('.staging-browser');

  const path = targetPath || pathInput.value.trim() || '/';
  pathInput.value = path;

  browser.innerHTML = '<div class="empty" style="padding:20px;"><div class="spinner"></div><p>Loading…</p></div>';

  try {
    const sid = await api.getSession('staging');
    const data = await api.fetch(`list/${sid}`, {
      method: 'POST',
      body: JSON.stringify({ path })
    });

    renderBrowser(page, api, data);
  } catch (e) {
    browser.innerHTML = `<div class="empty" style="padding:20px;"><p style="color:var(--danger);">Error: ${api.escHtml(e.message)}</p></div>`;
    api.flash('Staging error: ' + e.message, false);
  }
}

function goUp(page, api) {
  const pathInput = page.querySelector('.staging-path');
  const currentPath = pathInput.value.trim() || '/';

  // Get parent directory
  let parent = currentPath.replace(/\/[^/]+\/?$/, '') || '/';
  if (!parent.startsWith('/')) parent = '/' + parent;

  pathInput.value = parent;
  navigate(page, api, parent);
}

function renderBrowser(page, api, data) {
  const browser = page.querySelector('.staging-browser');
  const entries = data.entries || [];

  if (entries.length === 0) {
    browser.innerHTML = '<div class="empty" style="padding:20px;"><p style="color:var(--muted);">Empty directory</p></div>';
    return;
  }

  // Sort: directories first, then files, alphabetically
  const sorted = [...entries].sort((a, b) => {
    if (a.type === 'dir' && b.type !== 'dir') return -1;
    if (a.type !== 'dir' && b.type === 'dir') return 1;
    return a.name.localeCompare(b.name);
  });

  let html = '<ul class="tree" style="margin:0; padding:8px 0;">';

  for (const entry of sorted) {
    const fullPath = data.path.replace(/\/$/, '') + '/' + entry.name;
    const escapedPath = escHtml(fullPath);

    if (entry.type === 'dir') {
      html += `
        <li class="tree-item" style="padding:6px 12px;" data-path="${escapedPath}" data-type="dir">
          <span class="icon">📁</span>
          <span style="flex:1;">${escHtml(entry.name)}</span>
          <span style="color:var(--muted); font-size:0.8rem;">directory</span>
        </li>`;
    } else {
      const sizeStr = entry.size !== null ? formatBytes(entry.size) : '';
      html += `
        <li class="tree-item" style="padding:6px 12px;" data-path="${escapedPath}" data-type="file">
          <span class="icon">📄</span>
          <span style="flex:1;">${escHtml(entry.name)}</span>
          <span style="color:var(--muted); font-size:0.8rem;">${sizeStr}</span>
          <button class="btn btn-secondary btn-sm" style="margin-left:8px; padding:2px 8px; font-size:0.75rem;" data-action="download-file">📥</button>
        </li>`;
    }
  }

  html += '</ul>';
  browser.innerHTML = html;

  // Bind click handlers
  browser.querySelectorAll('.tree-item').forEach(item => {
    const path = item.dataset.path;
    const type = item.dataset.type;

    if (type === 'dir') {
      item.addEventListener('click', () => clickDir(page, api, item, path));
      item.addEventListener('dblclick', () => enterDir(page, api, path));
    } else {
      item.addEventListener('click', () => clickFile(page, item, path));
    }

    // Download button for files
    const downloadBtn = item.querySelector('[data-action="download-file"]');
    if (downloadBtn) {
      downloadBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        downloadFile(page, api, path);
      });
    }
  });
}

function clickDir(page, api, li, path) {
  // Highlight the directory
  const browser = li.closest('.staging-browser');
  browser.querySelectorAll('.tree-item').forEach(el => el.style.background = '');
  li.style.background = 'var(--bg3)';

  // Update upload path suggestion
  const uploadPath = page.querySelector('.staging-upload-path');
  if (uploadPath && !uploadPath.value) {
    uploadPath.placeholder = path + '/filename.txt';
  }
}

function enterDir(page, api, path) {
  const pathInput = page.querySelector('.staging-path');
  pathInput.value = path;
  navigate(page, api, path);
}

function clickFile(page, li, path) {
  // Highlight the file
  const browser = li.closest('.staging-browser');
  browser.querySelectorAll('.tree-item').forEach(el => el.style.background = '');
  li.style.background = 'var(--bg3)';

  // Update download path
  const downloadPath = page.querySelector('.staging-download-path');
  if (downloadPath) {
    downloadPath.value = path;
  }
}

async function downloadFile(page, api, path) {
  const status = page.querySelector('.staging-download-status');
  status.innerHTML = '<span style="color:var(--accent);">Downloading…</span>';

  try {
    const sid = await api.getSession('staging');
    const data = await api.fetch(`get/${sid}`, {
      method: 'POST',
      body: JSON.stringify({ filename: path })
    });

    // Decode base64 and trigger download
    const content = atob(data.content);
    const bytes = new Uint8Array(content.length);
    for (let i = 0; i < content.length; i++) {
      bytes[i] = content.charCodeAt(i);
    }
    const blob = new Blob([bytes]);
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = path.split('/').pop();
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    status.innerHTML = `<span style="color:var(--success);">Downloaded: ${escHtml(path)} (${formatBytes(data.size)})</span>`;
    api.flash(`Downloaded: ${path.split('/').pop()}`);
  } catch (e) {
    status.innerHTML = `<span style="color:var(--danger);">Error: ${api.escHtml(e.message)}</span>`;
    api.flash('Download failed: ' + e.message, false);
  }
}

async function download(page, api) {
  const path = page.querySelector('.staging-download-path').value.trim();

  if (!path) {
    api.flash('Please enter a remote file path', false);
    return;
  }

  await downloadFile(page, api, path);
}

async function upload(page, api) {
  const fileInput = page.querySelector('.staging-upload-file');
  const pathInput = page.querySelector('.staging-upload-path');
  const status = page.querySelector('.staging-upload-status');

  const file = fileInput.files[0];
  const remotePath = pathInput.value.trim();

  if (!file) {
    api.flash('Please select a file to upload', false);
    return;
  }
  if (!remotePath) {
    api.flash('Please enter a remote path', false);
    return;
  }
  if (!remotePath.startsWith('/')) {
    api.flash('Remote path must be absolute (start with /)', false);
    return;
  }

  status.innerHTML = '<span style="color:var(--accent);">Uploading…</span>';

  try {
    // Read file as base64
    const content = await new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => {
        const base64 = reader.result.split(',')[1];
        resolve(base64);
      };
      reader.onerror = reject;
      reader.readAsDataURL(file);
    });

    const sid = await api.getSession('staging');
    const data = await api.fetch(`put/${sid}`, {
      method: 'POST',
      body: JSON.stringify({ filename: remotePath, content })
    });

    status.innerHTML = `<span style="color:var(--success);">Uploaded: ${escHtml(data.path)} (${formatBytes(data.size)})</span>`;
    api.flash(`Uploaded: ${remotePath.split('/').pop()}`);

    // Refresh browser if we're viewing the parent directory
    const currentPath = page.querySelector('.staging-path').value.trim();
    const parentOfUpload = remotePath.replace(/\/[^/]+$/, '') || '/';
    if (currentPath === parentOfUpload) {
      navigate(page, api);
    }
  } catch (e) {
    status.innerHTML = `<span style="color:var(--danger);">Error: ${api.escHtml(e.message)}</span>`;
    api.flash('Upload failed: ' + e.message, false);
  }
}

// ─────────────────────────────────────────────────────────────
//  Utility functions
// ─────────────────────────────────────────────────────────────

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  if (bytes === null || bytes === undefined) return '';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

