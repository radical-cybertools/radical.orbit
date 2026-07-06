#!/usr/bin/env python

__author__    = 'Radical Development Team'
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright date +%Y, RADICAL@Rutgers'
__license__   = 'MIT'


import radical.orbit
import os


def test_radical_orbit():
    '''
    ###FIXME### test stub
    '''

    assert (True)


# ---------------------------------------------------------------------------
# _resolve_plugin_names (pure function in plugin_host_base.py)
# ---------------------------------------------------------------------------

import pytest
from radical.orbit.plugin_host_base import _resolve_plugin_names


def test_resolve_plugin_names_exact():
    available = ["sysinfo", "psij", "queue_info"]
    result = _resolve_plugin_names(["psij", "sysinfo"], available)
    assert result == ["psij", "sysinfo"]


def test_resolve_plugin_names_prefix_dropped():
    # The prefix-match form was dropped: a partial name no longer resolves.
    available = ["sysinfo", "psij", "queue_info"]
    with pytest.raises(ValueError, match="No plugin matches 'sys'"):
        _resolve_plugin_names(["sys", "q"], available)


def test_resolve_plugin_names_partial_is_no_match():
    # What used to be an "ambiguous" prefix is now simply an unknown name.
    available = ["sysinfo", "syslog"]
    with pytest.raises(ValueError, match="No plugin matches 'sys'"):
        _resolve_plugin_names(["sys"], available)


def test_resolve_plugin_names_unknown_raises():
    available = ["sysinfo", "psij"]
    with pytest.raises(ValueError, match="No plugin matches"):
        _resolve_plugin_names(["rhapsody"], available)


def test_resolve_plugin_names_empty():
    assert _resolve_plugin_names([], ["sysinfo"]) == []


if __name__ == '__main__':

    test_radical_orbit()



