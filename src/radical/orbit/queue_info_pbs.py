"""PBSPro implementation of QueueInfo (qstat / pbsnodes).

Parses text output rather than JSON because ``qstat -F json`` is not
universal on PBSPro (Aurora's deployment supports it but older sites do
not). Output formats follow the PBSPro 2024.x reference manual.
"""

import logging
import os
import re
import shutil
import subprocess
import time

from .queue_info import QueueInfo
from .batch_system_pbs import _parse_qstat_f, _parse_pbs_walltime, _parse_exec_host

log = logging.getLogger("radical.orbit")


def _sbank_clean_env() -> dict:
    """Env for invoking sbank without the caller's venv polluting sys.prefix.

    ALCF's sbank wrapper reads its config from ``<sys.prefix>/etc/ni/...``,
    so running it under a virtualenv makes it look in the wrong place and
    it crashes.  Strip venv vars and pin PATH to the system defaults.
    """
    env = {k: v for k, v in os.environ.items()
           if k not in ('VIRTUAL_ENV', 'PYTHONHOME', 'PYTHONPATH')}
    env['PATH'] = '/usr/bin:/bin'
    return env


def _parse_sbank_list_allocations(stdout: str) -> list:
    """Parse the whitespace-aligned table from ``sbank-list-allocations``.

    The table uses a dashes row (``---``) below the header; we derive
    column boundaries from the spans of dashes so the parser tolerates
    variable widths.  Stops at the first blank line after the data (the
    ``Totals:`` section is not an allocation record).
    """
    lines = stdout.splitlines()

    # Locate the dashes row; it follows the header.
    dash_idx = None
    for i, line in enumerate(lines):
        if line.strip() and set(line.strip()) <= {'-', ' '}:
            dash_idx = i
            break
    if dash_idx is None or dash_idx == 0:
        return []

    # Column spans = dash runs on that line.
    spans = [(m.start(), m.end()) for m in re.finditer(r'-+', lines[dash_idx])]
    headers = [lines[dash_idx - 1][s:e].strip() for s, e in spans]

    def _num(s: str, cast):
        s = s.replace(',', '').strip()
        if not s:
            return None
        try:
            return cast(s)
        except ValueError:
            return None

    allocs = []
    for line in lines[dash_idx + 1:]:
        if not line.strip():
            break  # end of data; Totals section follows a blank line
        cols = {h: line[s:e].strip() for h, (s, e) in zip(headers, spans)}
        project = cols.get('Project', '')
        if not project:
            continue
        allocs.append({
            'account'             : project,
            'user'                : '',     # sbank shows only the caller
            'fairshare'           : None,
            'qos'                 : '',
            'max_jobs'            : None,
            'max_submit'          : None,
            'max_wall'            : None,
            'grp_tres'            : None,
            'allocated_node_hours': None,
            'used_node_hours'     : _num(cols.get('Charged', ''), float),
            'remaining_node_hours': _num(cols.get('Available Balance', ''),
                                         float),
            'allocation_id'       : _num(cols.get('Allocation', ''), int),
            'suballocation_id'    : _num(cols.get('Suballocation', ''), int),
            'start_date'          : cols.get('Start', ''),
            'end_date'            : cols.get('End', ''),
            'resource'            : cols.get('Resource', ''),
            'jobs'                : _num(cols.get('Jobs', ''), int),
        })
    return allocs


# Job state code → display string used by the existing queue_info UI.
_STATE_DISPLAY = {
    'Q': 'PENDING',
    'W': 'PENDING',
    'T': 'PENDING',
    'R': 'RUNNING',
    'B': 'RUNNING',
    'E': 'COMPLETING',
    'F': 'COMPLETED',
    'X': 'COMPLETED',
    'H': 'HELD',
    'S': 'SUSPENDED',
    'M': 'MOVED',
    'U': 'SUSPENDED',
}


def _acl_match(acl_str: str, candidates: set) -> bool:
    """Check a PBSPro ACL list against a set of identifier *candidates*.

    Entries are comma-separated ``name[@host]`` with an optional ``+``
    (allow) or ``-`` (deny) prefix.  Host suffixes are ignored for
    matching purposes.  Returns True iff some entry allows one of the
    candidates and no entry denies one of them.
    """
    denied = False
    allowed = False
    for entry in acl_str.split(','):
        entry = entry.strip()
        if not entry:
            continue
        prefix = ''
        if entry[0] in '+-':
            prefix = entry[0]
            entry = entry[1:]
        name = entry.split('@', 1)[0]
        if name in candidates:
            if prefix == '-':
                denied = True
            else:
                allowed = True
    return allowed and not denied


def _user_groups(user: str) -> set:
    """Return the set of group names *user* belongs to (incl. primary).

    Returns an empty set on any resolver failure.
    """
    try:
        import pwd  # local import; stdlib only
        import grp
        pw = pwd.getpwnam(user)
        gids = os.getgrouplist(user, pw.pw_gid)
        names = set()
        for gid in gids:
            try:
                names.add(grp.getgrgid(gid).gr_name)
            except KeyError:
                continue
        return names
    except (KeyError, OSError):
        return set()


def _user_can_submit(attrs: dict, user: str,
                     user_groups: 'set | None' = None) -> bool:
    """Decide whether *user* can submit to the queue described by *attrs*.

    Applies PBSPro user and group ACLs conjunctively: if either ACL is
    enabled and rejects the user, submission is denied; disabled ACLs
    are skipped.  *user_groups* may be supplied to avoid re-resolving
    group membership per queue.
    """
    if attrs.get('acl_user_enable', '').strip().lower() == 'true':
        if not _acl_match(attrs.get('acl_users', ''), {user}):
            return False

    if attrs.get('acl_group_enable', '').strip().lower() == 'true':
        if user_groups is None:
            user_groups = _user_groups(user)
        if not _acl_match(attrs.get('acl_groups', ''), user_groups):
            return False

    return True


def _run(cmd, timeout=60):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=timeout, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Command {cmd} failed (rc={e.returncode}): "
            f"{e.stderr.strip()}") from e
    return r.stdout


def _parse_qstat_records(stdout):
    """Split a multi-job ``qstat -f`` output into per-job dicts.

    Returns: list of (job_id, info_dict) tuples.
    """
    records = []
    cur_id = None
    cur_lines = []
    for line in stdout.splitlines():
        if line.startswith('Job Id:'):
            if cur_id is not None:
                records.append((cur_id, _parse_qstat_f('\n'.join(cur_lines))))
            cur_id = line.split(':', 1)[1].strip()
            cur_lines = []
        else:
            cur_lines.append(line)
    if cur_id is not None:
        records.append((cur_id, _parse_qstat_f('\n'.join(cur_lines))))
    return records


def _parse_pbsnodes(stdout):
    """Parse ``pbsnodes -a`` text output into a list of node dicts.

    Each block starts with the node name and contains indented "key = value"
    lines (or "key: value" on some PBSPro versions).
    """
    nodes = []
    cur = None
    for raw in stdout.splitlines():
        if not raw:
            continue
        if not raw[0].isspace():
            if cur is not None:
                nodes.append(cur)
            cur = {'name': raw.strip()}
            continue
        if cur is None:
            continue
        s = raw.strip()
        if '=' in s:
            k, v = s.split('=', 1)
        elif ':' in s:
            k, v = s.split(':', 1)
        else:
            continue
        cur[k.strip()] = v.strip()
    if cur is not None:
        nodes.append(cur)
    return nodes


def _node_resources(node):
    """Extract (ncpus, ngpus, mem_mb) from a parsed pbsnodes entry.

    Looks at the available resources first, falling back to total. Handles
    PBSPro keys ``resources_available.<key>`` and ``resources_assigned.<key>``.
    """
    def _intval(key):
        v = node.get(key, '')
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0

    ncpus = _intval('resources_available.ncpus')
    ngpus = _intval('resources_available.ngpus')
    mem   = node.get('resources_available.mem', '')

    mem_mb = 0
    if mem:
        m = re.match(r'^\s*(\d+)\s*(kb|mb|gb|tb)?\s*$', mem, re.IGNORECASE)
        if m:
            n = int(m.group(1))
            unit = (m.group(2) or 'kb').lower()
            mem_mb = {'kb': n // 1024,
                      'mb': n,
                      'gb': n * 1024,
                      'tb': n * 1024 * 1024}.get(unit, 0)
    return ncpus, ngpus, mem_mb


class QueueInfoPBSPro(QueueInfo):
    """PBSPro backend for queue information."""

    backend_name = 'pbs'

    def _collect_raw_queues(self):
        """Return ``{queue_name: parsed_qstat_attributes}`` from ``qstat -Qf``.

        Raises the underlying exception if qstat cannot be run.
        """
        stdout = _run(['qstat', '-Qf'])
        queues = {}
        cur = None
        cur_lines = []
        for raw in stdout.splitlines():
            if raw.startswith('Queue:'):
                if cur is not None:
                    queues[cur] = _parse_qstat_f('\n'.join(cur_lines))
                cur = raw.split(':', 1)[1].strip()
                cur_lines = []
            else:
                cur_lines.append(raw)
        if cur is not None:
            queues[cur] = _parse_qstat_f('\n'.join(cur_lines))
        return queues

    def _collect_info(self):
        """Collect queue/partition info via qstat -Qf and pbsnodes -a."""

        # --- queue list ---
        try:
            queues = self._collect_raw_queues()
        except Exception:
            return {'queues': {}}

        # --- node info ---
        try:
            nstdout = _run(['pbsnodes', '-a'])
            nodes = _parse_pbsnodes(nstdout)
        except Exception:
            nodes = []

        # All PBS nodes are in a single shared pool — partition each by the
        # queue listed in their resources_default.queue, if any. When that's
        # absent we accumulate everything under each named queue (Aurora's
        # typical pattern: every node serves every queue).
        node_total     = len(nodes)
        node_available = 0
        node_idle      = 0
        cpus_max       = 0
        gpus_max       = 0
        mem_max_mb     = 0

        for n in nodes:
            state = n.get('state', '').lower()
            ncpus, ngpus, mem_mb = _node_resources(n)
            cpus_max  = max(cpus_max,  ncpus)
            gpus_max  = max(gpus_max,  ngpus)
            mem_max_mb = max(mem_max_mb, mem_mb)
            if 'down' in state or 'offline' in state or 'unavailable' in state:
                continue
            node_available += 1
            jobs_field = n.get('jobs', '').strip()
            if not jobs_field:
                node_idle += 1

        def _safe_int(s):
            try:
                return int(s) if s else None
            except (TypeError, ValueError):
                return None

        result = {}
        for qname, qinfo in queues.items():
            wall_max = _parse_pbs_walltime(qinfo.get('resources_max.walltime', ''))
            wall_min = _parse_pbs_walltime(qinfo.get('resources_min.walltime', ''))
            time_limit = wall_max if wall_max is not None else 'UNLIMITED'

            nodes_min = _safe_int(qinfo.get('resources_min.nodect', ''))
            nodes_max = _safe_int(qinfo.get('resources_max.nodect', ''))

            enabled = qinfo.get('enabled', '').lower() == 'true'
            started = qinfo.get('started', '').lower() == 'true'
            state   = 'UP' if (enabled and started) else 'DOWN'

            result[qname] = {
                'name'             : qname,
                'state'            : state,
                'time_limit'       : time_limit,
                'walltime_min'     : wall_min,
                'walltime_max'     : wall_max,
                'nodes_min'        : nodes_min,
                'nodes_max'        : nodes_max,
                'default'          : None,
                'nodes_total'      : node_total,
                'nodes_available'  : node_available,
                'nodes_idle'       : node_idle,
                'cpus_per_node'    : cpus_max,
                'mem_per_node_mb'  : mem_max_mb,
                'gpus_per_node'    : gpus_max,
                'max_jobs_per_user': None,
                'features'         : [],
            }

        return {'queues': result}


    def _collect_jobs(self, queue, user):
        """Collect jobs in *queue*, optionally filtered by *user*."""
        cmd = ['qstat', '-f']
        if user:
            cmd.extend(['-u', user])
        try:
            stdout = _run(cmd)
        except Exception:
            return {'jobs': []}
        records = _parse_qstat_records(stdout)
        jobs = [self._render_job(jid, info) for jid, info in records
                if info.get('queue', '').strip() == queue]
        return {'jobs': jobs}


    def _collect_all_user_jobs(self, user):
        """All jobs for *user* across all queues."""
        cmd = ['qstat', '-f']
        if user:
            cmd.extend(['-u', user])
        try:
            stdout = _run(cmd)
        except Exception:
            return {'jobs': []}
        records = _parse_qstat_records(stdout)
        return {'jobs': [self._render_job(jid, info) for jid, info in records]}


    def _get_user_partitions(self, user):
        """Return the set of queue names *user* can submit to.

        Aurora's ``qstat -Qf`` reports every queue PBS knows about —
        including per-reservation ``R<jobid>`` queues and project-scoped
        ``M<jobid>``/workshop queues — which collectively dwarf the
        handful of public queues most users actually see.  We use the
        queue's ``acl_user_enable`` / ``acl_users`` attributes (already
        returned by qstat) to filter.

        Returns ``None`` if qstat cannot be run, to degrade gracefully
        to the "show everything" path upstream.
        """
        try:
            queues = self._collect_raw_queues()
        except Exception:
            return None
        groups = _user_groups(user)   # resolve once, reuse per queue
        return {name for name, attrs in queues.items()
                if _user_can_submit(attrs, user, user_groups=groups)}

    def _collect_allocations(self, user):
        """Collect project allocations.

        PBSPro has no native sacctmgr, but ALCF layers an accounting
        system on top (``sbank-list-allocations``) that returns the
        caller's projects, node-hour balance, and charging info.  When
        that tool is available we shell out; otherwise return an empty
        list so the UI degrades gracefully.  The ``user`` arg is ignored
        — sbank only ever reports the current user's allocations.
        """
        exe = shutil.which('sbank-list-allocations')
        if not exe:
            return {'allocations': []}

        try:
            r = subprocess.run([exe], capture_output=True, text=True,
                               timeout=30, env=_sbank_clean_env())
        except (OSError, subprocess.TimeoutExpired) as exc:
            log.debug("sbank-list-allocations failed: %s", exc)
            return {'allocations': []}
        if r.returncode != 0:
            log.debug("sbank-list-allocations rc=%d stderr=%s",
                      r.returncode, r.stderr.strip())
            return {'allocations': []}

        try:
            allocs = _parse_sbank_list_allocations(r.stdout)
        except Exception as exc:
            log.debug("Failed to parse sbank output: %s", exc)
            return {'allocations': []}
        return {'allocations': allocs}


    @staticmethod
    def _render_job(jid, info):
        """Convert a parsed qstat -f record to the dict shape used by the UI."""
        code = (info.get('job_state', '') or '').strip()[:1].upper()
        state = _STATE_DISPLAY.get(code, 'UNKNOWN')

        wall_lim = _parse_pbs_walltime(info.get('Resource_List.walltime', ''))
        wall_used = _parse_pbs_walltime(info.get('resources_used.walltime', ''))

        try:
            nodes_n = int(info.get('Resource_List.nodect', '0') or 0)
        except ValueError:
            nodes_n = 0
        try:
            cpus_n = int(info.get('Resource_List.ncpus', '0') or 0)
        except ValueError:
            cpus_n = 0

        # PBSPro timestamps (qtime, stime, mtime) are RFC-2822-ish strings;
        # parse what we can, leave 0 otherwise.
        def _ts(key):
            s = info.get(key, '')
            if not s:
                return 0
            try:
                return int(time.mktime(time.strptime(s)))
            except (ValueError, OverflowError):
                return 0

        return {
            'job_id'     : jid.split('.', 1)[0],
            'job_name'   : info.get('Job_Name', ''),
            'user'       : (info.get('Job_Owner', '').split('@', 1)[0] or ''),
            'partition'  : info.get('queue', ''),
            'state'      : state,
            'nodes'      : nodes_n,
            'cpus'       : cpus_n,
            'time_limit' : wall_lim,
            'time_used'  : wall_used or 0,
            'submit_time': _ts('qtime'),
            'start_time' : _ts('stime'),
            'priority'   : 0,
            'account'    : info.get('Account_Name', ''),
            'node_list'  : ','.join(_parse_exec_host(info.get('exec_host', ''))),
        }
