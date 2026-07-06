// Pure session-heal helpers for the Explorer.
//
// Shared by two callers so the logic is written (and tested) once:
//   - the browser imports this as an ES module (orbit_explorer.html, served at
//     /plugins/session_util.js);
//   - pytest exercises it under the quickjs engine
//     (tests/unittests/test_explorer_js.py).
//
// Deliberately free of DOM / fetch / global state — every input is a plain
// argument — so both callers can drive the same functions.

// A session-scoped request 404/410s because its sid is dead (the endpoint
// restarted, or the server's idle-expiry fired) — as opposed to a plain
// "job/task not found" 404.  Only the former should trigger a re-register:
// a 410, or a 404 whose detail says "unknown session id".
function isStaleSession(status, detail) {
  return status === 410 ||
         (status === 404 && (detail || '').includes('unknown session id'));
}

// Reverse-lookup.  Given the cached sessions map ("endpoint/plugin" -> sid) and
// a request path, find the session this request was scoped to: the entry whose
// plugin namespace prefixes the path and whose sid appears in it.
// `nsOf(endpoint, plugin)` returns the plugin namespace (e.g. "/ep/psij").
// Entries still holding an in-flight registration promise (non-string) are
// skipped.  Returns {key, endpoint, plugin, sid} or null.
function matchSessionForPath(path, sessions, nsOf) {
  for (const key of Object.keys(sessions)) {
    const val = sessions[key];
    if (typeof val !== 'string') continue;
    const slash    = key.indexOf('/');
    const endpoint = key.slice(0, slash);
    const plugin   = key.slice(slash + 1);
    const ns = nsOf(endpoint, plugin);
    if (!ns || !path.startsWith(ns + '/') || !path.includes(val)) continue;
    return { key: key, endpoint: endpoint, plugin: plugin, sid: val };
  }
  return null;
}

// Rewrite a request path, replacing the stale sid with a fresh one.  The sid is
// a unique UUID segment, so a plain global replace is safe.
function swapSid(path, oldSid, newSid) {
  return path.split(oldSid).join(newSid);
}

export { isStaleSession, matchSessionForPath, swapSid };
