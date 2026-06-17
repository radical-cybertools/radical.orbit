"""Smoke tests for bin/radical-edge-run.

Covers the pure-Python helpers (argv splitting, flatten, task_id).
End-to-end tests against a live bridge/edge would belong under
tests/integration/ and are deferred.
"""

import sys
import importlib.util
from importlib.machinery import SourceFileLoader
from pathlib import Path

import pytest


_loader = SourceFileLoader(
    'run_mod',
    str(Path(__file__).resolve().parents[2]
        / 'bin' / 'radical-edge-run')
)
_spec = importlib.util.spec_from_loader('run_mod', _loader)
_RUN  = importlib.util.module_from_spec(_spec)
sys.modules['run_mod'] = _RUN
_loader.exec_module(_RUN)

compute_task_id = _RUN.compute_task_id
_split_argv     = _RUN._split_argv
_flatten        = _RUN._flatten


# ---------------------------------------------------------------------------
# _split_argv
# ---------------------------------------------------------------------------

class TestSplitArgv:

    def test_separator_present(self):
        opts, cmd = _split_argv(
            ['prog', '--edge=e', '--pool=p', '--', 'echo', 'hi'])
        assert opts == ['--edge=e', '--pool=p']
        assert cmd == ['echo', 'hi']

    def test_missing_separator_exits(self):
        with pytest.raises(SystemExit):
            _split_argv(['prog', '--edge=e'])

    def test_empty_cmd_exits(self):
        with pytest.raises(SystemExit):
            _split_argv(['prog', '--edge=e', '--'])


# ---------------------------------------------------------------------------
# _flatten (for space-separated --in values)
# ---------------------------------------------------------------------------

class TestFlatten:

    def test_space_separated_string(self):
        assert _flatten(['a b c']) == ['a', 'b', 'c']

    def test_multiple_appends(self):
        assert _flatten(['a', 'b']) == ['a', 'b']

    def test_mixed(self):
        assert _flatten(['a b', 'c']) == ['a', 'b', 'c']

    def test_empty(self):
        assert _flatten([]) == []


# ---------------------------------------------------------------------------
# task_id stability
# ---------------------------------------------------------------------------

class TestTaskId:

    def test_format(self):
        tid = compute_task_id(['echo'], [], [], 'run')
        assert tid.startswith('t.')
        assert len(tid) == 18   # 't.' + 16 hex

    def test_order_independence_inputs(self):
        t1 = compute_task_id(['c'], ['a', 'b'], ['x'], 'r')
        t2 = compute_task_id(['c'], ['b', 'a'], ['x'], 'r')
        assert t1 == t2

    def test_order_independence_outputs(self):
        t1 = compute_task_id(['c'], ['i'], ['a', 'b'], 'r')
        t2 = compute_task_id(['c'], ['i'], ['b', 'a'], 'r')
        assert t1 == t2

    def test_cmd_order_matters(self):
        t1 = compute_task_id(['echo', 'a'], [], [], 'r')
        t2 = compute_task_id(['echo', 'b'], [], [], 'r')
        assert t1 != t2

    def test_run_id_changes_result(self):
        t1 = compute_task_id(['c'], [], [], 'r1')
        t2 = compute_task_id(['c'], [], [], 'r2')
        assert t1 != t2

    def test_inputs_vs_outputs_disambiguated(self):
        """A file appearing as input vs output must yield different ids."""
        t1 = compute_task_id(['c'], ['x'], [], 'r')
        t2 = compute_task_id(['c'], [], ['x'], 'r')
        assert t1 != t2
