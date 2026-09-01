#!/bin/sh
# Runs the ORBIT service using the Python interpreter and site-packages of
# the venv this wrapper is installed into.  When ``dragon`` is co-installed
# in the same venv, the service is launched via ``dragon`` so plugins that
# require the Dragon runtime (Rhapsody) initialise correctly.
#
# Everything resolves at RUNTIME from this script's own location: a wheel
# is built once and installed anywhere, so nothing about the installing
# venv may be captured at build time (issue #121 -- the old template baked
# the wheel builder's paths into every installation).
#
# Log level is controlled by RADICAL_ORBIT_LOG_LVL (default: INFO).
# Set to DEBUG before submitting to get verbose output in the job log:
#   export RADICAL_ORBIT_LOG_LVL=DEBUG

# The venv bin dir is wherever this script really lives, symlinks resolved.
SELF="$0"
while [ -L "$SELF" ]; do
    DIR="$(CDPATH= cd -- "$(dirname "$SELF")" && pwd)"
    SELF="$(readlink "$SELF")"
    case "$SELF" in /*) ;; *) SELF="$DIR/$SELF" ;; esac
done
BINDIR="$(CDPATH= cd -- "$(dirname "$SELF")" && pwd)"

# Belt for launch channels that scrub or replace the interpreter: the
# service must import from this venv even if a site hook swaps `python`.
# purelib and platlib both, where they differ (lib64 layouts) -- compiled
# packages live in the latter.  A wrapper without a usable python next to
# it is not installed into a venv: say so and stop, instead of exporting
# a broken environment and dying much later at the exec.
SITEPKGS="$("$BINDIR/python3" -c 'import sysconfig
p = sysconfig.get_paths()
out = p["purelib"] if p["purelib"] == p["platlib"] else \
    p["purelib"] + ":" + p["platlib"]
print(out)')" || {
    echo "[orbit] FATAL: no usable python3 in $BINDIR" >&2
    exit 1
}
export PYTHONPATH="$SITEPKGS${PYTHONPATH:+:$PYTHONPATH}"

# NOTE: the broker TLS cert is staged manually — copy broker_cert.pem
# from the broker host to ~/.radical/orbit/ on this host (or point
# $RADICAL_ORBIT_BROKER_CERT at it).  Endpoint startups happen through
# many channels (IRI, PsiJ, ssh, by hand); one staging procedure for
# all of them beats per-channel automation.

# The venv's bin dir must be on PATH: dragon's launcher resolves its
# helpers (``dragon-network-config-launch-helper``, ``dragon-backend``)
# BY NAME through the workload manager, and the WLM-spawned task does a
# PATH lookup on the target side.
export PATH="$BINDIR:$PATH"

# Under Slurm, force inner job steps to inherit this environment.  A
# submission made with ``--export=NONE`` (deliberate, or e.g. via a
# facility API) stamps SLURM_EXPORT_ENV=NONE into the job, and every
# inner ``srun`` — dragon's helper and backend launches — would scrub
# its task env, losing the PATH above.
if [ -n "${SLURM_JOB_ID:-}" ]; then
    export SLURM_EXPORT_ENV=ALL
fi

# Site-specific setup hook.  Evaluated *before* dragon-vs-python is
# decided so the chosen interpreter inherits any module loads / env
# tweaks the snippet performs.  The trust boundary is the same as the
# rest of the job submission: whoever can put strings into the job
# spec's environment can already control the exec target, so eval'ing
# the snippet adds no new attack surface.
#
# RADICAL_ORBIT_SETUP_B64 is the transport-safe form and takes
# precedence: some job APIs (e.g. IRI at NERSC) compose the batch
# script with unquoted ``export KEY=VALUE`` lines, truncating values at
# the first space — a multi-word snippet must travel as one base64
# token.
# Decode with the venv's python — the ``base64`` CLI diverges across
# systems (GNU ``-d`` vs BSD/macOS ``-D``) and may be absent entirely.
if [ -n "${RADICAL_ORBIT_SETUP_B64:-}" ]; then
    RADICAL_ORBIT_SETUP="$("$BINDIR/python3" -c \
        'import base64, os, sys
sys.stdout.write(base64.b64decode(os.environ["RADICAL_ORBIT_SETUP_B64"]).decode())')"
fi
if [ -n "${RADICAL_ORBIT_SETUP:-}" ]; then
    echo "[orbit] applying RADICAL_ORBIT_SETUP: $RADICAL_ORBIT_SETUP" >&2
    eval "$RADICAL_ORBIT_SETUP"
fi

# Anchor the dragon/python lookup to ``$BINDIR`` (= the venv this
# wrapper was installed into), NOT a PATH search.  RADICAL_ORBIT_SETUP
# may legitimately ``module load`` a different python (e.g. cray-python
# for headers / linking) and prepend it to PATH; that's for runtime
# libraries (LD_LIBRARY_PATH), not for picking the interpreter.  We
# always want the venv's own dragon + python.
DRAGON_PATH="$BINDIR/dragon"
PYTHON_PATH="$BINDIR/python3"

# Decide whether to launch via dragon.  Use it only when:
#   * dragon is installed in this venv,
#   * we are NOT on a SLURM/PBS login node without an allocation —
#     dragon's launch_selector raises in that case.
USE_DRAGON=no
REASON=""
if   [ ! -x "$DRAGON_PATH" ]; then
    REASON="dragon not installed at $DRAGON_PATH"
elif command -v sbatch >/dev/null 2>&1 && [ -z "$SLURM_JOB_ID" ]; then
    REASON="SLURM login node without allocation"
elif command -v qsub   >/dev/null 2>&1 && [ -z "$PBS_JOBID"    ]; then
    REASON="PBS login node without allocation"
else
    USE_DRAGON=yes
fi

# WARNING: do NOT enable dragon's channel-based logging in production.
# Passing e.g. ARGS="-l dragon_file=DEBUG -l stderr=DEBUG" routes dragon's
# logs through its own communication channels, which — verified live on
# Perlmutter with dragon 0.14 — can DEADLOCK backend bring-up right after
# BEIsUp/FENodeIdxBE, so the endpoint never comes up at all.  Use this ONLY
# for post-mortem debugging of an already-broken startup, never on a path you
# expect to succeed.
# ARGS="-l dragon_file=DEBUG -l stderr=DEBUG"

if [ "$USE_DRAGON" = "yes" ]; then
    echo "[orbit] starting with dragon ($DRAGON_PATH)" >&2
    exec "$DRAGON_PATH" $ARGS "$BINDIR/radical-orbit-endpoint.py" "$@"
else
    echo "[orbit] starting with python ($PYTHON_PATH) — $REASON" >&2
    exec "$PYTHON_PATH" "$BINDIR/radical-orbit-endpoint.py" "$@"
fi
