"""SLURM implementation of QueueInfo (sinfo / squeue / sacctmgr)."""

import os
import json
import time
import subprocess

from .queue_info import (QueueInfo, _UNAVAIL_STATES, _unwrap, _parse_gpus)


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


    def _run(self, cmd):
        """Run a subprocess with self._env, return stdout."""

        try:
            result = subprocess.run(cmd, capture_output=True, text=True,
                                    timeout=60, env=self._env, check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Command {cmd} failed (rc={e.returncode}): "
                f"{e.stderr.strip()}") from e
        return result.stdout


    def _collect_info(self):
        """
        Collect queue/partition info via sinfo --json and scontrol show
        nodes --json (for configured memory).

        Returns:
          dict: {"queues": {<partition_name>: {...}, ...}}
        """

        # --- sinfo ---
        stdout  = self._run(['sinfo', '--json'])
        entries = json.loads(stdout).get('sinfo', [])

        # --- scontrol show nodes (for real_memory) ---
        node_mem = {}
        try:
            stdout = self._run(['scontrol', 'show', 'nodes', '--json'])
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
            })
        return result

    def _collect_jobs(self, queue, user):
        """
        Collect job list via squeue --json.
        """
        cmd = ['squeue', '--json', '-p', queue]
        if user:
            cmd.extend(['--user', user])
        stdout = self._run(cmd)
        jobs   = json.loads(stdout).get('jobs', [])
        return {'jobs': self._parse_squeue_jobs(jobs)}

    def _collect_all_user_jobs(self, user):
        """
        Collect all jobs for a user across all partitions via squeue --json.
        """
        cmd = ['squeue', '--json']
        if user:
            cmd.extend(['--user', user])
        stdout = self._run(cmd)
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
        stdout = self._run(cmd)
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
        stdout = self._run(cmd)

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

        stdout = self._run(cmd)
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

        stdout = self._run(cmd)
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


# ---------------------------------------------------------------------------
# Backwards-compat: get_job_nodes used to live as a static method on this
# class (referenced by plugin_psij.py before the BatchSystem refactor).
# Provide a thin shim that delegates to SlurmBatchSystem.job_nodes for the
# remainder of the rewire.
# ---------------------------------------------------------------------------

def _get_job_nodes(native_id, env=None):
    from .batch_system_slurm import SlurmBatchSystem
    bs = SlurmBatchSystem()
    nodes = bs.job_nodes(native_id)
    return nodes


QueueInfoSlurm.get_job_nodes = staticmethod(_get_job_nodes)  # type: ignore
