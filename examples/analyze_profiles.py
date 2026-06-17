#!/usr/bin/env python3
"""
Analyze radical.prof profiles from an Endpoint throughput benchmark run.

Reads client.prof, bridge.prof, endpoint.prof from a directory, combines
them into a single timeline, and reports per-phase latency statistics
clustered by batch size and homogeneous/heterogeneous task mode.

Usage:
    python examples/analyze_profiles.py [profile_dir]

    profile_dir defaults to the current working directory.

The script expects the throughput benchmark to have been run with
RADICAL_ORBIT_PROFILE=True set in all three processes (client, bridge,
endpoint).
"""

import os
import sys
import glob
import statistics

import radical.prof as rprof


# -- Phase definitions --------------------------------------------------------
#
# Each phase is a (start_event, end_event) pair.  The analysis computes
# the duration of each phase per request and then aggregates.

PHASES = [
    # Client
    ('client_send',       'client_recv',        'client_rtt'),

    # Bridge inbound
    ('bridge_recv',       'bridge_body_prep',   'bridge_http_recv'),
    ('bridge_body_prep',  'bridge_ser',         'bridge_body_prep'),
    ('bridge_ser',        'bridge_ser_done',    'bridge_req_ser'),
    ('bridge_ser_done',   'bridge_ws_send',     'bridge_pre_ws_send'),
    ('bridge_ws_send',    'bridge_ws_sent',     'bridge_ws_send'),

    # Endpoint inbound
    ('endpoint_deser',        'endpoint_deser_done',    'endpoint_ws_deser'),
    ('endpoint_parse',        'endpoint_parse_done',    'endpoint_pydantic'),
    ('endpoint_recv',         'endpoint_route',         'endpoint_pre_route'),
    ('endpoint_route',        'endpoint_shim',          'endpoint_route_match'),
    ('endpoint_shim',         'endpoint_handler',       'endpoint_shim_build'),
    ('endpoint_handler',      'endpoint_handler_done',  'endpoint_handler'),

    # Endpoint outbound
    ('endpoint_body_ser',     'endpoint_body_ser_done', 'endpoint_body_ser'),
    ('endpoint_resp_ser',     'endpoint_resp_ser_done', 'endpoint_resp_model_ser'),
    ('endpoint_ws_send',      'endpoint_ws_sent',       'endpoint_ws_send'),

    # Bridge outbound
    ('bridge_deser',      'bridge_deser_done',  'bridge_resp_deser'),
    ('bridge_ws_recv',    'bridge_resp_ser',     'bridge_pre_resp_ser'),
    ('bridge_resp_ser',   'bridge_reply',       'bridge_resp_ser'),
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
        # try recursive
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


def _build_request_events(combined):
    """Index events by request UID.

    Returns dict: req_id -> {event_name: timestamp}
    """
    requests = {}
    for row in combined:
        uid   = row[rprof.UID]
        event = row[rprof.EVENT]
        t     = row[rprof.TIME]
        msg   = row[rprof.MSG]

        if not uid or not uid.startswith('req.'):
            continue

        if uid not in requests:
            requests[uid] = {'_events': {}, '_msg': {}}

        # keep earliest occurrence of each event per request
        if event not in requests[uid]['_events']:
            requests[uid]['_events'][event] = t
        if msg and event not in requests[uid]['_msg']:
            requests[uid]['_msg'][event] = msg

    return requests


def _classify_request(req_data):
    """Classify a request by its route (submit path, wait, etc.)

    Returns (route_type, batch_info) where batch_info is a dict with
    'batch_size' and 'homogeneous' if determinable.
    """
    msg = req_data['_msg']

    # The bridge_recv or endpoint_recv message contains "METHOD /path"
    route_msg = msg.get('endpoint_recv', msg.get('bridge_recv', ''))

    if 'submit/' in route_msg:
        # Determine batch size from body_ser_done message (byte count)
        # or from the request itself
        return 'submit', route_msg
    elif 'wait/' in route_msg:
        return 'wait', route_msg
    elif 'register_session' in route_msg:
        return 'session', route_msg
    elif 'list_tasks' in route_msg:
        return 'list_tasks', route_msg
    else:
        return 'other', route_msg


def _compute_phase_durations(requests):
    """Compute durations of each phase for each request.

    Returns dict: req_id -> {phase_label: duration_seconds}
    """
    durations = {}
    for req_id, rdata in requests.items():
        events = rdata['_events']
        d = {}
        for start_evt, end_evt, label in PHASES:
            t0 = events.get(start_evt)
            t1 = events.get(end_evt)
            if t0 is not None and t1 is not None:
                d[label] = t1 - t0
        durations[req_id] = d
    return durations


def _stats(values):
    """Compute summary statistics for a list of values."""
    if not values:
        return None
    n = len(values)
    avg = statistics.mean(values)
    med = statistics.median(values)
    mn  = min(values)
    mx  = max(values)
    std = statistics.stdev(values) if n > 1 else 0.0
    return {'n': n, 'avg': avg, 'med': med, 'min': mn,
            'max': mx, 'std': std}


# -- Batch clustering --------------------------------------------------------
#
# The throughput benchmark submits tasks in batches.  Each batch produces
# one or more submit HTTP requests (depending on payload chunking and
# template compression).  We cluster requests into "rounds" by looking at
# gaps in the client_send timestamps.

def _cluster_into_rounds(requests, durations):
    """Group requests into benchmark rounds by temporal gaps.

    A new round starts when the gap between consecutive client_send
    timestamps exceeds 2× the median inter-request gap.

    Returns list of round dicts, each with:
      - 'requests': list of req_ids
      - 'route_types': Counter of route types
      - 'durations': aggregated phase durations
    """
    # Sort requests by client_send (or earliest event)
    def _earliest(req_id):
        events = requests[req_id]['_events']
        return events.get('client_send',
               events.get('bridge_recv',
               events.get('endpoint_recv', float('inf'))))

    sorted_ids = sorted(requests.keys(), key=_earliest)
    if not sorted_ids:
        return []

    # Compute inter-request gaps
    times = [_earliest(rid) for rid in sorted_ids]
    gaps  = [times[i+1] - times[i] for i in range(len(times)-1)]

    if gaps:
        med_gap   = statistics.median(gaps)
        threshold = max(med_gap * 5, 0.5)  # at least 0.5s
    else:
        threshold = 0.5

    # Split into rounds
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


# -- Reporting ----------------------------------------------------------------

def _print_phase_table(label, phase_stats, indent=''):
    """Print a formatted table of phase statistics."""
    if not phase_stats:
        return

    print(f"{indent}{label}")
    print(f"{indent}{'phase':<25} {'avg':>8} {'med':>8} "
          f"{'min':>8} {'max':>8} {'std':>8}   n")
    print(f"{indent}{'-'*83}")

    for _, _, phase_label in PHASES:
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


def _aggregate_phases(req_ids, durations):
    """Aggregate phase durations across a set of requests."""
    phase_values = {}
    for rid in req_ids:
        d = durations.get(rid, {})
        for phase, val in d.items():
            phase_values.setdefault(phase, []).append(val)

    return {phase: _stats(vals) for phase, vals in phase_values.items()}


def _detect_benchmark_batches(rounds, requests):
    """Detect benchmark batch sizes and homo/hetero from round patterns.

    The throughput benchmark does: warmup(1), then batch sizes
    1,2,4,...  first homogeneous, then heterogeneous.

    We use heuristics:
    - Rounds with only submit+wait requests are benchmark rounds
    - The number of submit requests per round hints at batch size
    - The first half of benchmark rounds is homo, second half is hetero
    """
    batch_rounds = []
    for rnd in rounds:
        # Count submit requests in this round
        route_types = {}
        for rid in rnd:
            rtype, _ = _classify_request(requests[rid])
            route_types[rtype] = route_types.get(rtype, 0) + 1

        n_submit = route_types.get('submit', 0)
        n_wait   = route_types.get('wait', 0)
        n_total  = len(rnd)

        if n_submit > 0:
            batch_rounds.append({
                'req_ids':   rnd,
                'n_submit':  n_submit,
                'n_wait':    n_wait,
                'n_total':   n_total,
            })

    # Split into homo/hetero halves (skip warmup rounds)
    # The benchmark does: warmup(1 homo), batches homo, warmup(1 hetero), batches hetero
    # So we look for the pattern
    if len(batch_rounds) < 2:
        return batch_rounds, [], []

    # Find the likely split point: look for a round with 1 submit
    # after a run of increasing submits (that's the hetero warmup)
    homo_rounds  = []
    hetero_rounds = []
    saw_large     = False
    split_idx     = len(batch_rounds)

    for i, br in enumerate(batch_rounds):
        if br['n_submit'] >= 4:
            saw_large = True
        elif saw_large and br['n_submit'] <= 2 and i > 2:
            # This looks like the hetero warmup
            split_idx = i
            break

    homo_rounds   = batch_rounds[1:split_idx]   # skip homo warmup
    hetero_rounds = batch_rounds[split_idx+1:]   # skip hetero warmup

    return batch_rounds, homo_rounds, hetero_rounds


# -- Main --------------------------------------------------------------------

def main():
    prof_dir = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()

    # 1. Load profiles
    combined = _load_profiles(prof_dir)
    print(f"Combined timeline: {len(combined)} events\n")

    # 2. Build per-request event index
    requests  = _build_request_events(combined)
    durations = _compute_phase_durations(requests)
    print(f"Requests found: {len(requests)}\n")

    if not requests:
        print("No request events found.  Was RADICAL_ORBIT_PROFILE=True set?")
        sys.exit(1)

    # 3. Overall statistics
    all_phase_stats = _aggregate_phases(requests.keys(), durations)
    _print_phase_table("=== Overall phase statistics ===", all_phase_stats)

    # 4. Statistics by route type
    by_route = {}
    for rid in requests:
        rtype, _ = _classify_request(requests[rid])
        by_route.setdefault(rtype, []).append(rid)

    for rtype in sorted(by_route):
        rids = by_route[rtype]
        stats = _aggregate_phases(rids, durations)
        _print_phase_table(
            f"=== Route: {rtype} ({len(rids)} requests) ===", stats)

    # 5. Cluster into rounds and detect batch sizes
    rounds = _cluster_into_rounds(requests, durations)
    print(f"Detected {len(rounds)} temporal rounds\n")

    all_batch, homo_rounds, hetero_rounds = _detect_benchmark_batches(
        rounds, requests)

    # 6. Per-batch-size statistics
    def _report_batch_group(label, batch_group):
        if not batch_group:
            print(f"  (no {label} rounds detected)\n")
            return

        print(f"{'='*70}")
        print(f"  {label} — {len(batch_group)} rounds")
        print(f"{'='*70}\n")

        for i, br in enumerate(batch_group):
            rids = br['req_ids']
            _aggregate_phases(rids, durations)

            # Compute total round time
            t_vals = []
            for rid in rids:
                events = requests[rid]['_events']
                t0 = events.get('client_send', events.get('bridge_recv'))
                t1 = events.get('client_recv', events.get('bridge_reply'))
                if t0 is not None and t1 is not None:
                    t_vals.append(t1 - t0)

            round_stats = _stats(t_vals) if t_vals else None

            hdr = (f"  Round {i+1}: {br['n_total']} requests "
                   f"({br['n_submit']} submit, {br['n_wait']} wait)")
            if round_stats:
                hdr += (f"  |  avg RTT: {round_stats['avg']*1000:.1f}ms"
                        f"  med: {round_stats['med']*1000:.1f}ms")
            print(hdr)

            # Only show submit requests for phase breakdown
            submit_rids = [rid for rid in rids
                           if _classify_request(requests[rid])[0] == 'submit']
            if submit_rids:
                submit_stats = _aggregate_phases(submit_rids, durations)
                _print_phase_table(
                    f"    Submit phases ({len(submit_rids)} requests):",
                    submit_stats, indent='    ')

    _report_batch_group("Homogeneous (template)", homo_rounds)
    _report_batch_group("Heterogeneous (per-task args)", hetero_rounds)

    # 7. Summary comparison table
    if homo_rounds and hetero_rounds:
        print(f"\n{'='*70}")
        print("  Summary: avg client RTT by batch round")
        print(f"{'='*70}\n")
        print(f"  {'round':>5}  {'homo_n':>6}  {'homo_rtt':>10}  "
              f"{'hetero_n':>8}  {'hetero_rtt':>11}")
        print(f"  {'-'*50}")

        for i in range(max(len(homo_rounds), len(hetero_rounds))):
            def _round_rtt(batch_group, idx):
                if idx >= len(batch_group):
                    return None, 0
                rids = batch_group[idx]['req_ids']
                rtts = []
                for rid in rids:
                    events = requests[rid]['_events']
                    t0 = events.get('client_send')
                    t1 = events.get('client_recv')
                    if t0 is not None and t1 is not None:
                        rtts.append(t1 - t0)
                s = _stats(rtts) if rtts else None
                return s, len(rids)

            hs, hn = _round_rtt(homo_rounds, i)
            xs, xn = _round_rtt(hetero_rounds, i)

            h_str = f"{hs['avg']*1000:>8.1f}ms" if hs else f"{'—':>10}"
            x_str = f"{xs['avg']*1000:>8.1f}ms" if xs else f"{'—':>11}"

            print(f"  {i+1:>5}  {hn:>6}  {h_str:>10}  "
                  f"{xn:>8}  {x_str:>11}")

        print()



if __name__ == '__main__':
    main()
