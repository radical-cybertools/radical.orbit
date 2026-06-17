#!/usr/bin/env python

__author__    = 'Radical Development Team'
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright date +%Y, RADICAL@Rutgers'
__license__   = 'MIT'


import radical.edge
import os


def test_radical_edge():
    '''
    ###FIXME### test stub
    '''

    assert (True)


# ---------------------------------------------------------------------------
# _resolve_plugin_names (pure function in service.py)
# ---------------------------------------------------------------------------

import pytest
from radical.edge.service import _resolve_plugin_names


def test_resolve_plugin_names_exact():
    available = ["sysinfo", "psij", "queue_info"]
    result = _resolve_plugin_names(["psij", "sysinfo"], available)
    assert result == ["psij", "sysinfo"]


def test_resolve_plugin_names_prefix():
    available = ["sysinfo", "psij", "queue_info"]
    result = _resolve_plugin_names(["sys", "q"], available)
    assert result == ["sysinfo", "queue_info"]


def test_resolve_plugin_names_ambiguous_raises():
    available = ["sysinfo", "syslog"]
    with pytest.raises(ValueError, match="Ambiguous"):
        _resolve_plugin_names(["sys"], available)


def test_resolve_plugin_names_unknown_raises():
    available = ["sysinfo", "psij"]
    with pytest.raises(ValueError, match="No plugin matches"):
        _resolve_plugin_names(["rhapsody"], available)


def test_resolve_plugin_names_empty():
    assert _resolve_plugin_names([], ["sysinfo"]) == []


if __name__ == '__main__':

    test_radical_edge()



