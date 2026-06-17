#!/usr/bin/env python3
"""
Analyze Rhapsody plugin profiling from radical.prof files.

Reads client.prof, bridge.prof, endpoint.prof from a directory, extracts
Rhapsody-specific profiling events, and reports per-phase latency
statistics with dedicated plots.

Usage:
    python examples/analyze_rhapsody.py [profile_dir]

    profile_dir defaults to the current working directory.
"""

import os
import sys
import glob
import statistics

import radical.prof as rprof


# -- Rhapsody phase definitions -----------------------------------------------
#
# Request-level phases (uid = batch id, i.e. first task UID per submit call)
REQUEST_PHASES = [
    ('rh_parse_body',           'rh_parse_body_done',      'rh_parse_body'),
    ('rh_template_expand',      'rh_template_expand_done', 'rh_template_expand'),
    ('rh_submit',               'rh_submit_done',          'rh_submit_total'),
    ('rh_deser',                'rh_deser_done',           'rh_deser'),
    ('rh_backend_submit',       'rh_backend_submit_done',  'rh_backend_submit'),
    ('rh_register',             'rh_register_done',        'rh_register'),
]

# Per-task end-to-end phases (uid = individual task UID)
TASK_PHASES = [
    ('task_submit',      'task_batch_flush', 'batch_queue'),
    ('task_batch_flush', 'rh_task_exec',     'submit_transport'),
    ('rh_task_exec',     'rh_task_done',     'execution'),
    ('rh_task_done',     'notify_queue',     'post_exec'),
    ('notify_queue',     'notify_flush',     'notify_queue'),
    ('notify_flush',     'task_complete',    'notify_transport'),
    ('task_submit',      'task_complete',    'total_e2e'),
]


# -- Helpers ------------------------------------------------------------------

def _load_profiles(prof_dir):
    """Find and load .prof files, return combined timeline."""
    patterns = ['client.prof', 'client.task.prof', 'bridge.prof', 'endpoint.prof']
    prof_files = []
    for pat in patterns:
        found = glob.glob(os.path.join(prof_dir, pat))
        prof_files.extend(found)

    if not prof_files:
        prof_files = sorted(glob.glob(os.path.join(prof_dir, '*.prof')))

    if not prof_files:
        print(f"No .prof files found in {prof_dir}")
        sys.exit(1)

    print(f"Loading {len(prof_files)} profile(s):")
    for f in prof_files:
        print(f"  {f}")
    print()

    profs = rprof.read_profiles(prof_files, sid='endpoint.benchmark')
    combined, _ = rprof.combine_profiles(profs)
    return combined


def _build_events(combined):
    """Index events by UID.

    Returns two dicts:
      - req_events:  batch_id -> {event_name: timestamp, '_msg': {event: msg}}
      - task_events: task_uid -> {event_name: timestamp}
    """
    # Events keyed by individual task UID (client + endpoint)
    TASK_EVENT_NAMES = {
        'task_submit', 'task_batch_flush', 'task_complete',
        'rh_task_exec', 'rh_task_done',
        'notify_queue', 'notify_flush',
    }

    req_events  = {}
    task_events = {}

    for row in combined:
        uid   = row[rprof.UID]
        event = row[rprof.EVENT]
        t     = row[rprof.TIME]
        msg   = row[rprof.MSG]

        if not uid:
            continue

        # Task-level events (client + endpoint)
        if event in TASK_EVENT_NAMES:
            if uid not in task_events:
                task_events[uid] = {}
            if event not in task_events[uid]:
                task_events[uid][event] = t
            continue

        # Request-level events (rh_* prefix, batch IDs)
        if not event.startswith('rh_'):
            continue

        if uid not in req_events:
            req_events[uid] = {'_msg': {}}
        if event not in req_events[uid]:
            req_events[uid][event] = t
        if msg and event not in req_events[uid]['_msg']:
            req_events[uid]['_msg'][event] = msg

    return req_events, task_events


def _compute_durations(events, phases):
    """Compute phase durations for each UID."""
    durations = {}
    for uid, evts in events.items():
        d = {}
        for start_evt, end_evt, label in phases:
            t0 = evts.get(start_evt)
            t1 = evts.get(end_evt)
            if t0 is not None and t1 is not None:
                d[label] = t1 - t0
        durations[uid] = d
    return durations


def _stats(values):
    """Compute summary statistics."""
    if not values:
        return None
    n   = len(values)
    avg = statistics.mean(values)
    med = statistics.median(values)
    mn  = min(values)
    mx  = max(values)
    std = statistics.stdev(values) if n > 1 else 0.0
    return {'n': n, 'avg': avg, 'med': med, 'min': mn,
            'max': mx, 'std': std}


def _aggregate_phases(uids, durations, phases):
    """Aggregate phase durations across a set of UIDs."""
    phase_values = {}
    for uid in uids:
        d = durations.get(uid, {})
        for phase, val in d.items():
            phase_values.setdefault(phase, []).append(val)
    return {phase: _stats(vals) for phase, vals in phase_values.items()}


def _print_phase_table(label, phase_stats, phases, indent=''):
    """Print a formatted table of phase statistics."""
    if not phase_stats:
        return

    print(f"{indent}{label}")
    print(f"{indent}{'phase':<25} {'avg':>8} {'med':>8} "
          f"{'min':>8} {'max':>8} {'std':>8}   n")
    print(f"{indent}{'-'*83}")

    for _, _, phase_label in phases:
        s = phase_stats.get(phase_label)
        if not s:
            continue
        print(f"{indent}{phase_label:<25} "
              f"{s['avg']*1000:>7.3f}ms "
              f"{s['med']*1000:>7.3f}ms "
              f"{s['min']*1000:>7.3f}ms "
              f"{s['max']*1000:>7.3f}ms "
              f"{s['std']*1000:>7.3f}ms "
              f"  {s['n']}")
    print()


def _cluster_into_rounds(req_events, req_durations):
    """Group request-level events into rounds by temporal gaps."""
    def _earliest(uid):
        evts = req_events[uid]
        for e in ('rh_parse_body', 'rh_submit', 'rh_deser'):
            if e in evts:
                return evts[e]
        return float('inf')

    sorted_ids = sorted(req_events.keys(), key=_earliest)
    if not sorted_ids:
        return []

    times = [_earliest(uid) for uid in sorted_ids]
    gaps  = [times[i+1] - times[i] for i in range(len(times)-1)]

    if gaps:
        med_gap   = statistics.median(gaps)
        threshold = max(med_gap * 5, 0.5)
    else:
        threshold = 0.5

    rounds    = []
    cur_round = [sorted_ids[0]]

    for i in range(1, len(sorted_ids)):
        gap = times[i] - times[i-1]
        if gap > threshold:
            rounds.append(cur_round)
            cur_round = []
        cur_round.append(sorted_ids[i])
    if cur_round:
        rounds.append(cur_round)

    return rounds


def _get_batch_size(uid, req_events):
    """Extract batch size from rh_submit message (stored as str(batch_n))."""
    msg = req_events[uid].get('_msg', {}).get('rh_submit', '')
    try:
        return int(msg)
    except (ValueError, TypeError):
        return 0


# -- Reporting ----------------------------------------------------------------

def main():
    prof_dir = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()

    # 1. Load profiles
    combined = _load_profiles(prof_dir)
    print(f"Combined timeline: {len(combined)} events\n")

    # 2. Build event indices
    req_events, task_events = _build_events(combined)
    req_durations  = _compute_durations(req_events,  REQUEST_PHASES)
    task_durations = _compute_durations(task_events, TASK_PHASES)

    print(f"Rhapsody submit requests: {len(req_events)}")
    print(f"Rhapsody tasks:           {len(task_events)}\n")

    if not req_events and not task_events:
        print("No Rhapsody profiling events found.")
        sys.exit(1)

    # 3. Request-level statistics
    if req_events:
        all_stats = _aggregate_phases(req_events.keys(), req_durations,
                                      REQUEST_PHASES)
        _print_phase_table("=== Rhapsody request-level phases (all) ===",
                           all_stats, REQUEST_PHASES)

    # 4. Task execution statistics
    if task_events:
        task_stats = _aggregate_phases(task_events.keys(), task_durations,
                                       TASK_PHASES)
        _print_phase_table("=== Per-task execution duration ===",
                           task_stats, TASK_PHASES)

    # 5. Cluster into rounds, report per-round
    rounds = _cluster_into_rounds(req_events, req_durations)
    print(f"Detected {len(rounds)} temporal rounds\n")

    for i, rnd in enumerate(rounds):
        batch_sizes = [_get_batch_size(uid, req_events) for uid in rnd]
        total_tasks = sum(batch_sizes)

        stats = _aggregate_phases(rnd, req_durations, REQUEST_PHASES)

        print(f"--- Round {i+1}: {len(rnd)} requests, "
              f"~{total_tasks} tasks ---")
        _print_phase_table("  Request phases:", stats, REQUEST_PHASES,
                           indent='  ')

    # 6. Per-task stats by round
    # Map tasks to rounds by checking if rh_task_exec falls within
    # the round's time window
    if task_events and rounds:
        print(f"\n{'='*70}")
        print("  Per-task execution by round")
        print(f"{'='*70}\n")

        for i, rnd in enumerate(rounds):
            # Find the time window of this round
            t_min = float('inf')
            t_max = float('-inf')
            for uid in rnd:
                for key, val in req_events[uid].items():
                    if key.startswith('_'):
                        continue
                    t_min = min(t_min, val)
                    t_max = max(t_max, val)

            # Allow some slack for task completion after request finishes
            t_max += 30.0  # tasks may complete well after submit returns

            round_tasks = []
            for tuid, tevts in task_events.items():
                t_exec = tevts.get('rh_task_exec', float('inf'))
                if t_min <= t_exec <= t_max:
                    round_tasks.append(tuid)

            if round_tasks:
                ts = _aggregate_phases(round_tasks, task_durations,
                                       TASK_PHASES)
                exec_s = ts.get('rh_task_exec')
                if exec_s:
                    print(f"  Round {i+1}: {exec_s['n']} tasks  "
                          f"avg={exec_s['avg']*1000:.3f}ms  "
                          f"med={exec_s['med']*1000:.3f}ms  "
                          f"min={exec_s['min']*1000:.3f}ms  "
                          f"max={exec_s['max']*1000:.3f}ms")

        print()



if __name__ == '__main__':
    main()
