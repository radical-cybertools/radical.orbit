"""Unit tests for QueueInfoPBSPro."""

from unittest.mock import patch, MagicMock

import pytest

from radical.orbit.queue_info_pbs import (
    QueueInfoPBSPro, _parse_pbsnodes, _node_resources, _parse_qstat_records)


# ---------------------------------------------------------------------------
# pbsnodes parser
# ---------------------------------------------------------------------------

def test_parse_pbsnodes_basic():
    out = (
        "nid001\n"
        "    state = free\n"
        "    resources_available.ncpus = 64\n"
        "    resources_available.ngpus = 4\n"
        "    resources_available.mem = 128gb\n"
        "    jobs = \n"
        "\n"
        "nid002\n"
        "    state = job-busy\n"
        "    resources_available.ncpus = 64\n"
        "    resources_available.ngpus = 4\n"
        "    resources_available.mem = 128gb\n"
        "    jobs = 1.x\n")
    nodes = _parse_pbsnodes(out)
    assert len(nodes) == 2
    assert nodes[0]['name'] == 'nid001'
    assert nodes[1]['name'] == 'nid002'
    assert nodes[0]['state'] == 'free'
    assert nodes[1]['jobs'] == '1.x'


def test_node_resources_with_units():
    n = {'resources_available.ncpus': '64',
         'resources_available.ngpus': '4',
         'resources_available.mem'  : '128gb'}
    assert _node_resources(n) == (64, 4, 128 * 1024)


def test_node_resources_kb():
    n = {'resources_available.ncpus': '8',
         'resources_available.ngpus': '0',
         'resources_available.mem'  : '4194304kb'}  # 4 GiB in KB
    assert _node_resources(n) == (8, 0, 4096)


def test_node_resources_missing():
    assert _node_resources({}) == (0, 0, 0)


# ---------------------------------------------------------------------------
# qstat -f multi-record parser
# ---------------------------------------------------------------------------

def test_parse_qstat_records_two_jobs():
    out = (
        "Job Id: 1.x\n"
        "    job_state = R\n"
        "    queue = compute\n"
        "Job Id: 2.x\n"
        "    job_state = Q\n"
        "    queue = debug\n")
    recs = _parse_qstat_records(out)
    assert len(recs) == 2
    assert recs[0][0] == '1.x'
    assert recs[0][1]['job_state'] == 'R'
    assert recs[1][0] == '2.x'
    assert recs[1][1]['queue'] == 'debug'


# ---------------------------------------------------------------------------
# _collect_info / _collect_jobs
# ---------------------------------------------------------------------------

class TestCollectInfo:

    def _qstat_qf(self):
        return (
            "Queue: compute\n"
            "    enabled = True\n"
            "    started = True\n"
            "    resources_max.walltime = 02:00:00\n"
            "Queue: debug\n"
            "    enabled = False\n"
            "    started = True\n"
            "    resources_max.walltime = 00:30:00\n")

    def _pbsnodes(self):
        return (
            "nid001\n"
            "    state = free\n"
            "    resources_available.ncpus = 64\n"
            "    resources_available.ngpus = 4\n"
            "    resources_available.mem = 128gb\n"
            "    jobs = \n"
            "nid002\n"
            "    state = job-busy\n"
            "    resources_available.ncpus = 64\n"
            "    resources_available.ngpus = 4\n"
            "    resources_available.mem = 128gb\n"
            "    jobs = 1.x\n"
            "nid003\n"
            "    state = down\n"
            "    resources_available.ncpus = 64\n"
            "    resources_available.ngpus = 4\n"
            "    resources_available.mem = 128gb\n"
            "    jobs = \n")

    def test_collect_info_marks_disabled(self):
        backend = QueueInfoPBSPro()
        with patch('radical.orbit.queue_info_pbs._run',
                   side_effect=[self._qstat_qf(), self._pbsnodes()]):
            res = backend._collect_info()
        queues = res['queues']
        assert set(queues.keys()) == {'compute', 'debug'}
        assert queues['compute']['state']      == 'UP'
        assert queues['debug']['state']        == 'DOWN'
        assert queues['compute']['time_limit'] == 2 * 3600
        assert queues['compute']['cpus_per_node'] == 64
        assert queues['compute']['gpus_per_node'] == 4
        assert queues['compute']['nodes_total'] == 3
        assert queues['compute']['nodes_available'] == 2   # nid003 down
        assert queues['compute']['nodes_idle'] == 1        # nid002 has jobs


class TestCollectJobs:

    def test_filter_by_queue(self):
        backend = QueueInfoPBSPro()
        out = (
            "Job Id: 1.x\n"
            "    job_state = R\n"
            "    queue = compute\n"
            "    Job_Owner = alice@x\n"
            "    Job_Name = job1\n"
            "    Resource_List.nodect = 2\n"
            "    Resource_List.ncpus = 128\n"
            "    Resource_List.walltime = 01:00:00\n"
            "Job Id: 2.x\n"
            "    job_state = Q\n"
            "    queue = debug\n"
            "    Job_Owner = alice@x\n"
            "    Job_Name = job2\n"
            "    Resource_List.nodect = 1\n"
            "    Resource_List.ncpus = 64\n"
            "    Resource_List.walltime = 00:30:00\n")
        with patch('radical.orbit.queue_info_pbs._run', return_value=out):
            res = backend._collect_jobs('compute', 'alice')
        jobs = res['jobs']
        assert len(jobs) == 1
        j = jobs[0]
        assert j['job_id'] == '1'
        assert j['job_name'] == 'job1'
        assert j['user'] == 'alice'
        assert j['state'] == 'RUNNING'
        assert j['nodes'] == 2
        assert j['cpus'] == 128
        assert j['time_limit'] == 3600


def test_collect_allocations_returns_empty_without_sbank():
    """When sbank-list-allocations is unavailable we degrade gracefully."""
    backend = QueueInfoPBSPro()
    with patch('shutil.which', return_value=None):
        assert backend._collect_allocations('alice') == {'allocations': []}


def test_collect_allocations_parses_sbank_output():
    """Aurora-style sbank-list-allocations output is parsed into records."""
    sample = (
        " Allocation  Suballocation  Start       End         Resource  Project    Jobs  Charged  Available Balance \n"
        " ----------  -------------  ----------  ----------  --------  ---------  ----  -------  ----------------- \n"
        " 15370       15337          2025-11-17  2026-10-01  aurora    Fusion-FM    10      0.2           -5,533.5 \n"
        "\n"
        "Totals:\n"
        "  Rows: 1\n"
    )
    backend = QueueInfoPBSPro()
    fake_proc = MagicMock(returncode=0, stdout=sample, stderr='')
    with patch('shutil.which', return_value='/usr/bin/sbank-list-allocations'), \
         patch('radical.orbit.queue_info_pbs.subprocess.run',
               return_value=fake_proc):
        result = backend._collect_allocations('alice')
    assert len(result['allocations']) == 1
    a = result['allocations'][0]
    assert a['account']              == 'Fusion-FM'
    assert a['allocation_id']        == 15370
    assert a['used_node_hours']      == 0.2
    assert a['remaining_node_hours'] == -5533.5
    assert a['resource']             == 'aurora'
