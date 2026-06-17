/**
 * Lucid Plugin Module for ORBIT Explorer
 *
 * RADICAL Pilot integration (stub - not yet implemented).
 */

export const name = 'lucid';

export function template() {
  return `
    <div class="page-header">
      <div class="page-icon">🧠</div>
      <h2>Lucid — <span class="endpoint-label"></span></h2>
    </div>
    <div class="card">
      <p style="color:var(--muted)">Advanced web interface for Lucid is not yet available.</p>
    </div>
  `;
}

export function css() {
  return '';
}

export function init(page, api) {
  // No initialization needed for stub
}

export function onNotification(data, page, api) {
  // No notifications for stub
}
