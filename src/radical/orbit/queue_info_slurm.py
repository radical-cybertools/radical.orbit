"""SLURM implementation of QueueInfo (sinfo / squeue / sacctmgr)."""

import os
import json
import re
import time

from .queue_info import QueueInfo
from .batch_system import run_cmd_strict


# Node states considered unavailable for scheduling (SLURM vocabulary).
_UNAVAIL_STATES = {'DOWN',    'DRAIN',   'DRAINING',
                   'FAIL',    'FAILING', 'MAINT',
                   'FUTURE',  'POWER_DOWN', 'POWERED_DOWN',
                   'NOT_RESPONDING', 'REBOOT_ISSUED'}


def _unwrap(obj):
    """
    Extract a value from SLURM's {set, infinite, number} wrapper.

    Returns:
      The numeric value, or None if the field is infinite or unset.
    """

    if not isinstance(obj, dict):
        return obj

    if obj.get('infinite'):
        return None
    if not obj.get('set', True):
        return None

    return obj.get('number')


def _exit_code(job):
    """
    Extract a numeric exit/return code from a squeue JSON job object.

    Newer SLURM wraps it as ``{status, return_code:{set,number}, signal}``;
    older versions expose a plain ``{set, infinite, number}`` wrapper.
    Returns None when the field is absent or unset.
    """

    ec = job.get('exit_code')
    if isinstance(ec, dict):
        if 'return_code' in ec:
            return _unwrap(ec.get('return_code'))
        return _unwrap(ec)
    return ec


def _parse_gpus(gres_str):
    """
    Parse GPU count from a SLURM GRES string.

    Handles formats like:
      "gpu:8(S:0-7)"
      "gpu:mi250:8(S:0-7)"
      "gpu:8"
      "(null)"
      ""

    Returns:
      int: number of GPUs, or 0 if none.
    """

    if not gres_str or gres_str == '(null)':
        return 0

    total = 0
    for entry in gres_str.split(','):
        entry = entry.strip()
        if not entry.startswith('gpu'):
            continue

        # strip socket binding like (S:0-7)
        entry = re.sub(r'\(.*?\)', '', entry)

        parts = entry.split(':')
        # gpu:N or gpu:TYPE:N
        for part in reversed(parts):
            try:
                total += int(part)
                break
            except ValueError:
                continue

    return total


class QueueInfoSlurm(QueueInfo):
    """
    SLURM backend for queue information.

    Calls sinfo, squeue, scontrol, and sacctmgr with --json and parses the
    results.  Target SLURM version: 24.11.5+.

    Args:
      slurm_conf (str): Optional path to slurm.conf.  When set, all
          subprocess calls run with SLURM_CONF=<path> in their environment,
          allowing a single endpoint service to query multiple clusters.
    """

    backend_name = 'slurm'

    def __init__(self, slurm_conf=None):

        super().__init__()

        self._env = dict(os.environ)
        if slurm_conf:
            self._env['SLURM_CONF'] = slurm_conf


    def _collect_info(self):
        """
        Collect queue/partition info via sinfo --json and scontrol show
        nodes --json (for configured memory).

        Returns:
          dict: {"queues": {<partition_name>: {...}, ...}}
        """

        # --- sinfo ---
        stdout  = run_cmd_strict(['sinfo', '--json'], timeout=60, env=self._env)
        entries = json.loads(stdout).get('sinfo', [])

        # --- scontrol show nodes (for real_memory) ---
        node_mem = {}
        try:
            stdout = run_cmd_strict(['scontrol', 'show', 'nodes', '--json'],
                                    timeout=60, env=self._env)
            nodes  = json.loads(stdout).get('nodes', [])
            for node in nodes:
                name = node.get('name', '')
                if name:
                    node_mem[name] = node.get('real_memory', 0)
        except Exception:
            pass   # scontrol may not be available, mem stays 0

        # group entries by partition name
        partitions = {}
        for entry in entries:
            pinfo = entry.get('partition', {})
            pname = pinfo.get('name', '')
            if not pname:
                continue

            node_states = set(entry.get('node', {}).get('state', []))
            n_total     = entry.get('nodes', {}).get('total', 0)
            n_idle      = entry.get('nodes', {}).get('idle',  0)
            is_unavail  = bool(node_states & _UNAVAIL_STATES)

            if pname not in partitions:
                # extract partition-level config from first entry
                time_val = _unwrap(pinfo.get('maximums', {}).get('time', {}))
                if time_val is None:
                    time_limit = 'UNLIMITED'
                else:
                    time_limit = int(time_val)

                # memory: find first node in this partition for real_memory
                node_names = entry.get('nodes', {}).get('nodes', [])
                mem = 0
                for nn in node_names:
                    if nn in node_mem:
                        mem = node_mem[nn]
                        break

                partitions[pname] = {
                    'name'             : pname,
                    'state'            : pinfo.get('partition', {})
                                              .get('state', ['UNKNOWN'])[0],
                    'time_limit'       : time_limit,
                    'default'          : None,
                    'nodes_total'      : 0,
                    'nodes_available'  : 0,
                    'nodes_idle'       : 0,
                    'cpus_per_node'    : entry.get('cpus', {})
                                              .get('maximum', 0),
                    'mem_per_node_mb'  : mem,
                    'gpus_per_node'    : _parse_gpus(
                                            entry.get('gres', {})
                                                 .get('total', '')),
                    'max_jobs_per_user': None,
                    'features'         : [f for f in
                                          entry.get('features', {})
                                               .get('total', '')
                                               .split(',')
                                          if f],
                }

            p = partitions[pname]
            p['nodes_total'] += n_total
            p['nodes_idle']  += n_idle
            if not is_unavail:
                p['nodes_available'] += n_total

        return {'queues': partitions}


    @staticmethod
    def _parse_squeue_jobs(jobs):
        """
        Convert a list of raw squeue JSON job objects to normalised dicts.

        Shared by _collect_jobs and _collect_all_user_jobs.
        """
        now    = time.time()
        result = []
        for job in jobs:
            start = _unwrap(job.get('start_time', {})) or 0
            state = (job.get('job_state', ['UNKNOWN']) or ['UNKNOWN'])[0]

            time_used = int(now - start) if (state == 'RUNNING' and start > 0) else 0

            reason = job.get('state_reason')
            if reason is None:
                reason = job.get('reason', '')

            result.append({
                'job_id'     : str(job.get('job_id', '')),
                'job_name'   : job.get('name', ''),
                'user'       : job.get('user_name', ''),
                'partition'  : job.get('partition', ''),
                'state'      : state,
                'nodes'      : _unwrap(job.get('node_count', {})) or 0,
                'cpus'       : _unwrap(job.get('cpus', {}))       or 0,
                'time_limit' : _unwrap(job.get('time_limit', {})),
                'time_used'  : time_used,
                'submit_time': _unwrap(job.get('submit_time', {})) or 0,
                'start_time' : start,
                'priority'   : _unwrap(job.get('priority', {}))   or 0,
                'account'    : job.get('account', ''),
                'node_list'  : job.get('nodes', ''),
                # extra detail surfaced for the Explorer (issue #40); all
                # additive with None/'' defaults so older SLURM still parses
                'reason'       : reason or '',
                'qos'          : job.get('qos', ''),
                'dependency'   : job.get('dependency', ''),
                'std_out'      : job.get('standard_output', ''),
                'std_err'      : job.get('standard_error', ''),
                'work_dir'     : job.get('current_working_directory', ''),
                'command'      : job.get('command', ''),
                'exit_code'    : _exit_code(job),
                'array_job_id' : _unwrap(job.get('array_job_id')),
                'array_task_id': _unwrap(job.get('array_task_id')),
                'tres_req'     : job.get('tres_req_str', ''),
                'tres_alloc'   : job.get('tres_alloc_str', ''),
                'restart_cnt'  : _unwrap(job.get('restart_cnt')),
            })
        return result

    def _collect_jobs(self, queue, user):
        """
        Collect job list via squeue --json.
        """
        cmd = ['squeue', '--json', '-p', queue]
        if user:
            cmd.extend(['--user', user])
        stdout = run_cmd_strict(cmd, timeout=60, env=self._env)
        jobs   = json.loads(stdout).get('jobs', [])
        return {'jobs': self._parse_squeue_jobs(jobs)}

    def _collect_all_user_jobs(self, user):
        """
        Collect all jobs for a user across all partitions via squeue --json.
        """
        cmd = ['squeue', '--json']
        if user:
            cmd.extend(['--user', user])
        stdout = run_cmd_strict(cmd, timeout=60, env=self._env)
        jobs   = json.loads(stdout).get('jobs', [])
        return {'jobs': self._parse_squeue_jobs(jobs)}


    def _collect_allocations(self, user):
        """
        Collect allocation/association data via sacctmgr show assoc --json.
        Falls back to sacctmgr -P -n if --json fails.
        """

        try:
            return self._collect_allocations_json(user)
        except Exception:
            return self._collect_allocations_parsable(user)

    def _get_user_partitions(self, user):
        """
        Return the set of partition names the user has access to.
        """
        try:
            partitions = self._collect_user_partitions_json(user)
        except Exception:
            partitions = self._collect_user_partitions_parsable(user)

        # None in the set means at least one association grants access to all
        if None in partitions:
            return None

        return partitions

    def _collect_user_partitions_json(self, user):
        """Collect user's allowed partitions via sacctmgr --json."""

        cmd = ['sacctmgr', 'show', 'assoc', '--json', f'Users={user}']
        stdout = run_cmd_strict(cmd, timeout=60, env=self._env)
        data   = json.loads(stdout)
        assocs = data.get('associations') or data.get('association', [])

        partitions = set()
        for assoc in assocs:
            part = assoc.get('partition', '')
            if not part:
                # Empty partition = access to all partitions
                partitions.add(None)
            else:
                partitions.add(part)

        return partitions

    def _collect_user_partitions_parsable(self, user):
        """
        Fallback: collect user's allowed partitions via sacctmgr -P -n.
        """

        cmd = ['sacctmgr', 'show', 'assoc', '-P', '-n', f'Users={user}']
        stdout = run_cmd_strict(cmd, timeout=60, env=self._env)

        partitions = set()
        for line in stdout.strip().splitlines():
            fields = line.split('|')
            if len(fields) < 4:
                continue
            part = fields[3].strip()
            if not part:
                partitions.add(None)
            else:
                partitions.add(part)

        return partitions


    def _collect_allocations_json(self, user):
        """Collect allocations via sacctmgr --json."""

        cmd = ['sacctmgr', 'show', 'assoc', '--json']
        if user:
            cmd.append(f'Users={user}')

        stdout = run_cmd_strict(cmd, timeout=60, env=self._env)
        data   = json.loads(stdout)
        assocs = data.get('associations') or data.get('association', [])

        return {'allocations': self._parse_assocs(assocs)}


    def _collect_allocations_parsable(self, user):
        """
        Fallback: collect allocations via sacctmgr -P -n (pipe-delimited).
        """

        cmd = ['sacctmgr', 'show', 'assoc', '-P', '-n']
        if user:
            cmd.append(f'Users={user}')

        stdout = run_cmd_strict(cmd, timeout=60, env=self._env)
        return {'allocations': self._parse_assocs_parsable(stdout)}


    def _parse_assocs(self, assocs):
        """Parse association list from JSON data."""

        result = []
        for assoc in assocs:

            maxj = assoc.get('max', {}).get('jobs', {})

            result.append({
                'account'             : assoc.get('account', ''),
                'user'                : assoc.get('user', ''),
                'fairshare'           : _unwrap(
                                            assoc.get('shares_raw', {})),
                'qos'                 : ','.join(assoc.get('qos', [])),
                'max_jobs'            : _unwrap(maxj.get('active', {})),
                'max_submit'          : _unwrap(
                                            maxj.get('per', {})
                                                .get('submitted', {})),
                'max_wall'            : _unwrap(
                                            maxj.get('per', {})
                                                .get('wall_clock', {})),
                'grp_tres'            : assoc.get('max', {})
                                             .get('tres', {})
                                             .get('total', None) or None,
                'allocated_node_hours': None,
                'used_node_hours'     : None,
                'remaining_node_hours': None,
            })

        return result


    @staticmethod
    def _parse_assocs_parsable(stdout):
        """
        Parse sacctmgr -P -n output (pipe-delimited).

        Expected columns (order from sacctmgr show assoc -P -n):
          Cluster|Account|User|Partition|Share|Priority|GrpJobs|GrpTRES|
          GrpSubmit|GrpWall|GrpTRESMins|MaxJobs|MaxTRES|MaxTRESPerNode|
          MaxSubmit|MaxWall|MaxTRESMins|QOS|Def QOS|GrpTRESRunMins
        """

        result = []
        for line in stdout.strip().splitlines():
            fields = line.split('|')
            if len(fields) < 18:
                continue

            def _int_or_none(s):
                try:
                    return int(s)
                except (ValueError, TypeError):
                    return None

            result.append({
                'account'             : fields[1],
                'user'                : fields[2],
                'fairshare'           : _int_or_none(fields[4]),
                'qos'                 : fields[17],
                'max_jobs'            : _int_or_none(fields[11]),
                'max_submit'          : _int_or_none(fields[14]),
                'max_wall'            : fields[15] or None,
                'grp_tres'            : fields[7] or None,
                'allocated_node_hours': None,
                'used_node_hours'     : None,
                'remaining_node_hours': None,
            })

        return result
