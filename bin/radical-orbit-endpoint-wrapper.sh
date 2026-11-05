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
#
# Stop signals: under dragon the wrapper does not ``exec`` but stays the
# parent of the launcher, traps TERM/INT/HUP and forwards them (HUP as
# TERM) to the endpoint python process alone -- never to ``dragon`` or its
# process group: ``dragon -s`` has no SIGTERM handler (the runtime dies
# under the endpoint, DDict managers orphan, /dev/shm leaks) and killing
# an srun step under ``dragon -m`` orphans its backend step.  The endpoint
# writes its PID to $RADICAL_ORBIT_ENDPOINT_PIDFILE (set here); on the
# signal it releases its resources (on_host_stop) and exits, then dragon
# tears down and the wrapper exits with dragon's exit code.  A signal
# before the endpoint wrote its PID is retried until it has, or dragon
# exits.  Without dragon the wrapper ``exec``s python as before.
#
# Batch jobs: let Slurm signal the batch shell only, with lead time
# before it kills everything at the walltime, and run the wrapper as the
# job's process (``exec`` it, or forward the signal to it):
#   #SBATCH --signal=B:TERM@120
# (without ``B:`` Slurm signals every job step -- dragon's sruns -- and
# not the batch shell; to stop early: ``scancel --batch --signal=TERM``).

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
    # stay dragon's parent and forward stop signals to the endpoint (see
    # the header)
    RADICAL_ORBIT_ENDPOINT_PIDFILE="$(mktemp \
        "${TMPDIR:-/tmp}/radical-orbit-endpoint.pid.XXXXXX")" || {
        echo "[orbit] FATAL: cannot create the endpoint pid file" >&2
        exit 1
    }
    export RADICAL_ORBIT_ENDPOINT_PIDFILE
    trap 'rm -f "$RADICAL_ORBIT_ENDPOINT_PIDFILE"' EXIT

    PENDING=""
    forward() {
        TRAPPED=1
        EP_PID="$(cat "$RADICAL_ORBIT_ENDPOINT_PIDFILE" 2>/dev/null)"
        if [ -n "$EP_PID" ] && kill -"$1" "$EP_PID" 2>/dev/null; then
            echo "[orbit] forwarded SIG$1 to the endpoint (pid $EP_PID)" >&2
            PENDING=""
        else
            PENDING="$1"        # endpoint not up (yet): retry below
        fi
    }
    trap 'forward TERM' TERM HUP
    trap 'forward INT'  INT

    # explicit stdin: an async list would otherwise get /dev/null
    "$DRAGON_PATH" $ARGS "$BINDIR/radical-orbit-endpoint.py" "$@" <&0 &
    CHILD=$!
    while :; do
        TRAPPED=""
        wait "$CHILD"; RC=$?
        [ -n "$TRAPPED" ] || break              # dragon exited
        if ! kill -0 "$CHILD" 2>/dev/null; then
            wait "$CHILD"; RC=$?                # exited while trapped
            break
        fi
        while [ -n "$PENDING" ] && kill -0 "$CHILD" 2>/dev/null; do
            sleep 1
            forward "$PENDING"
        done
    done
    exit "$RC"
else
    echo "[orbit] starting with python ($PYTHON_PATH) — $REASON" >&2
    exec "$PYTHON_PATH" "$BINDIR/radical-orbit-endpoint.py" "$@"
fi
