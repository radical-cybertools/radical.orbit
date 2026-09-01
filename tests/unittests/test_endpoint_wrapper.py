"""The endpoint wrapper resolves its venv at runtime, from its own path.

Regression tests for issue #121: the wrapper used to be a build-time
template, so the wheel baked the *builder's* venv paths into every
installation -- on any other host it silently fell back to plain python
and dragon-backed plugins died at runtime.
"""

import os
import stat
import subprocess
from pathlib import Path

import pytest

WRAPPER = (Path(__file__).resolve().parents[2]
           / "bin" / "radical-orbit-endpoint-wrapper.sh")

PY_STUB = """#!/bin/sh
# stands in for the venv python: answers the sysconfig probe, and marks
# an exec of the endpoint with its own location
case "$1" in
  -c) echo "/fake/site-packages" ;;
  *)  echo "EXEC_PYTHON:$0"; echo "EXEC_ARGS:$@" ;;
esac
"""


@pytest.fixture
def fake_venv(tmp_path):
    """A bin dir that looks like a venv's: wrapper + python3 + endpoint."""

    bindir = tmp_path / "venv" / "bin"
    bindir.mkdir(parents=True)

    wrapper = bindir / WRAPPER.name
    wrapper.write_text(WRAPPER.read_text())
    py = bindir / "python3"
    py.write_text(PY_STUB)
    (bindir / "radical-orbit-endpoint.py").write_text("# endpoint stub\n")
    for f in (wrapper, py):
        f.chmod(f.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    return bindir


def run(cmd, **env):
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=30,
        env={**os.environ, **env})


def test_wrapper_uses_its_own_venv(fake_venv):
    """Interpreter and endpoint come from the wrapper's install dir --
    never from a path captured somewhere else."""

    out = run([str(fake_venv / WRAPPER.name), "-n", "ep1"])

    assert f"EXEC_PYTHON:{fake_venv}/python3" in out.stdout
    assert f"EXEC_ARGS:{fake_venv}/radical-orbit-endpoint.py -n ep1" \
        in out.stdout
    # without a co-installed dragon the fallback says so, loudly
    assert "dragon not installed" in out.stderr


def test_wrapper_resolves_symlinked_invocations(fake_venv, tmp_path):
    """A symlink from elsewhere still lands in the real venv bin dir."""

    link = tmp_path / "elsewhere" / "wrapper"
    link.parent.mkdir()
    link.symlink_to(fake_venv / WRAPPER.name)

    out = run([str(link)])

    assert f"EXEC_PYTHON:{fake_venv}/python3" in out.stdout


def test_wrapper_puts_the_venv_on_path_and_pythonpath(fake_venv):
    """dragon's WLM-launched helpers do PATH lookups on the task side, and
    the service must import from this venv whatever a site hook does."""

    probe = fake_venv / "radical-orbit-endpoint.py"
    probe.write_text("")  # exec target; the python stub just echoes

    out = run([str(fake_venv / WRAPPER.name)], PYTHONPATH="/pre/existing")

    # the stub cannot show us the exports, so re-run through a shell that
    # execs a reporter instead of python3
    reporter = fake_venv / "python3"
    reporter.write_text(
        '#!/bin/sh\ncase "$1" in\n  -c) echo "/fake/site-packages" ;;\n'
        '  *) echo "PATH:$PATH"; echo "PYTHONPATH:$PYTHONPATH" ;;\nesac\n')

    out = run([str(fake_venv / WRAPPER.name)], PYTHONPATH="/pre/existing")

    path_line = [l for l in out.stdout.splitlines()
                 if l.startswith("PATH:")][0]
    ppath_line = [l for l in out.stdout.splitlines()
                  if l.startswith("PYTHONPATH:")][0]
    assert path_line.split(":", 1)[1].startswith(str(fake_venv))
    assert ppath_line == "PYTHONPATH:/fake/site-packages:/pre/existing"
