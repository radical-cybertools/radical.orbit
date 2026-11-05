"""The endpoint wrapper resolves its venv at runtime, from its own path.

Regression tests for issue #121: the wrapper used to be a build-time
template, so the wheel baked the *builder's* venv paths into every
installation -- on any other host it silently fell back to plain python
and dragon-backed plugins died at runtime.
"""

import os
import signal
import stat
import subprocess
import sys
import time
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
    """A symlink from elsewhere still lands in the real venv bin dir --
    absolute and relative targets both (the relative arm rebases the
    target onto the link's own directory)."""

    absolute = tmp_path / "elsewhere" / "wrapper"
    absolute.parent.mkdir()
    absolute.symlink_to(fake_venv / WRAPPER.name)

    relative = tmp_path / "elsewhere" / "rel-wrapper"
    relative.symlink_to(
        os.path.relpath(fake_venv / WRAPPER.name, relative.parent))

    for link in (absolute, relative):
        out = run([str(link)])
        assert f"EXEC_PYTHON:{fake_venv}/python3" in out.stdout


def test_wrapper_puts_the_venv_on_path_and_pythonpath(fake_venv):
    """dragon's WLM-launched helpers do PATH lookups on the task side, and
    the service must import from this venv whatever a site hook does."""

    # a python3 stand-in that reports the exports instead of executing
    (fake_venv / "python3").write_text(
        '#!/bin/sh\ncase "$1" in\n  -c) echo "/fake/site-packages" ;;\n'
        '  *) echo "PATH:$PATH"; echo "PYTHONPATH:$PYTHONPATH" ;;\nesac\n')

    out = run([str(fake_venv / WRAPPER.name)], PYTHONPATH="/pre/existing")

    path_line = [l for l in out.stdout.splitlines()
                 if l.startswith("PATH:")][0]
    ppath_line = [l for l in out.stdout.splitlines()
                  if l.startswith("PYTHONPATH:")][0]
    assert path_line.split(":", 1)[1].startswith(str(fake_venv))
    assert ppath_line == "PYTHONPATH:/fake/site-packages:/pre/existing"


def test_no_preexisting_pythonpath_means_no_trailing_colon(fake_venv):
    """An empty PYTHONPATH element is the current directory to CPython --
    an import-shadowing hazard on shared filesystems."""

    (fake_venv / "python3").write_text(
        '#!/bin/sh\ncase "$1" in\n  -c) echo "/fake/site-packages" ;;\n'
        '  *) echo "PYTHONPATH:$PYTHONPATH" ;;\nesac\n')

    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    out = subprocess.run(
        [str(fake_venv / WRAPPER.name)], capture_output=True, text=True,
        timeout=30, env=env)

    assert "PYTHONPATH:/fake/site-packages\n" in out.stdout


def test_a_wrapper_without_a_python_fails_loudly(fake_venv):
    """No usable python next to the wrapper means it is not installed
    into a venv: one clear FATAL line, nonzero exit, no half-configured
    environment marching on."""

    (fake_venv / "python3").unlink()

    out = run([str(fake_venv / WRAPPER.name)])

    assert out.returncode == 1
    assert "FATAL: no usable python3" in out.stderr
    assert "EXEC_PYTHON" not in out.stdout


# a launcher that, like ``dragon``, runs the endpoint as its child; it
# records any signal it gets itself -- the wrapper must not send it one
DRAGON_STUB = """#!/bin/sh
trap 'echo TERM >> "$MARKS/launcher"' TERM
trap 'echo INT  >> "$MARKS/launcher"' INT
"$(dirname "$0")/python3" "$@"
exit $?
"""

# the endpoint: writes its PID where the wrapper says (after a delay, to
# cover a signal that arrives before the endpoint is up), exits 7 on TERM
ENDPOINT_STUB = """import os, signal, sys, time
marks = os.environ['MARKS']
def on_term(signum, frame):
    with open(os.path.join(marks, 'endpoint'), 'w') as f:
        f.write(signal.Signals(signum).name)
    sys.exit(7)
signal.signal(signal.SIGTERM, on_term)
with open(os.path.join(marks, 'sigint'), 'w') as f:  # not inherited SIG_IGN
    f.write(str(signal.getsignal(signal.SIGINT) is signal.default_int_handler))
open(os.path.join(marks, 'started'), 'w').close()
time.sleep(float(os.environ['PID_DELAY']))
pidfile = os.environ['RADICAL_ORBIT_ENDPOINT_PIDFILE']
with open(pidfile, 'w') as f:
    f.write(f'{os.getpid()}\\n')
with open(os.path.join(marks, 'pidfile'), 'w') as f:
    f.write(pidfile)
time.sleep(60)
"""


@pytest.mark.parametrize("pid_delay", [0.0, 1.5])
def test_dragon_path_forwards_term_to_the_endpoint(fake_venv, tmp_path,
                                                   pid_delay):
    """Under a launcher, SIGTERM to the wrapper reaches the endpoint
    process alone, and the endpoint's exit code comes back out."""

    (fake_venv / "python3").write_text(
        f'#!/bin/sh\nexec {sys.executable} "$@"\n')
    (fake_venv / "dragon").write_text(DRAGON_STUB)
    (fake_venv / "dragon").chmod(0o755)
    (fake_venv / "radical-orbit-endpoint.py").write_text(ENDPOINT_STUB)
    marks = tmp_path / "marks"
    marks.mkdir()

    # an allocation, so the wrapper picks the launcher
    proc = subprocess.Popen(
        [str(fake_venv / WRAPPER.name)], stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True,
        env={**os.environ, "MARKS": str(marks), "PID_DELAY": str(pid_delay),
             "SLURM_JOB_ID": "1", "PBS_JOBID": "1", "TMPDIR": str(tmp_path)})
    try:
        deadline = time.time() + 20
        while not (marks / "started").exists():
            assert time.time() < deadline and proc.poll() is None
            time.sleep(0.05)
        proc.send_signal(signal.SIGTERM)
        rc = proc.wait(timeout=20)
    finally:
        if proc.poll() is None:
            proc.kill()
    err = proc.stderr.read()

    assert rc == 7, err
    assert (marks / "endpoint").read_text() == "SIGTERM"
    assert not (marks / "launcher").exists()
    assert "forwarded SIGTERM to the endpoint" in err
    assert not Path((marks / "pidfile").read_text()).exists()   # cleaned
    assert (marks / "sigint").read_text() == "True"   # INT not ignored
