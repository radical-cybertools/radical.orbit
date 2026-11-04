#!/usr/bin/env python3
"""Put-collapse study: join client results with the endpoint diag logs.

    python inv_analyze.py <runs dir> [data dir] [figs dir]

For every ``<data>/conc-inv-<stage>-<variant>/c<C>-b<B>-r*.json`` run, the
put and get windows (from the clients' ``t_start``) select the endpoint's
``diag.*.jsonl`` records (``<runs>/ep-ep-a<stage>/``).  Writes
``<data>/inv-summary.json`` and plots F6 (per-key put time per phase),
and F8 (native replay of the same put sequence).
"""

import glob
import json
import os
import re
import sys

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt                     # noqa: E402
from matplotlib.patches import Patch                # noqa: E402

RUNS = sys.argv[1]
DATA = sys.argv[2] if len(sys.argv) > 2 else 'data'
FIGS = sys.argv[3] if len(sys.argv) > 3 else 'figs'
BLUE, ORANGE, AQUA, YELLOW, MAGENTA, VIOLET = \
    '#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#6250d6'
GRAY = '#52514e'
plt.rcParams.update({'font.size': 13, 'axes.grid': True,
                     'grid.color': '#dddddd', 'axes.spines.top': False,
                     'axes.spines.right': False, 'lines.linewidth': 2,
                     'lines.markersize': 7})

_diag = {}


def diag(stage):
    if stage not in _diag:
        recs = []
        for f in glob.glob(f'{RUNS}/ep-ep-a{stage}/diag.*.jsonl'):
            recs += [json.loads(line) for line in open(f)]
        _diag[stage] = recs
    return _diag[stage]


def window(recs, op, t0, t1):
    rs = [r for r in recs if r['op'] == op and t0 <= r['t'] <= t1]
    if not rs:
        return None
    med = lambda f: float(np.median([r[f] for r in rs]))   # noqa: E731
    out = {'calls': len(rs)}
    if 'h' in rs[0]:                                # route handler record
        out['h_ms'] = med('h') * 1e3
        return out
    out.update(q_ms=med('q') * 1e3, x_ms=med('x') * 1e3,
               busy=sum(r['x'] for r in rs) / (t1 - t0))
    n = sum(r['n'] for r in rs)
    if n:
        out['key_us'] = sum(r['k_sum'] for r in rs) / n * 1e6
        out['kmax_ms'] = float(np.percentile([r['k_max'] for r in rs],
                                             90)) * 1e3
    return out


def runs():
    res = []
    for d in sorted(glob.glob(f'{DATA}/conc-inv-*')):
        m = re.match(r'conc-inv-([a-z0-9]+)-([a-z0-9]+)(?:-(\d))?$',
                     os.path.basename(d))
        stage, var, rep = m.group(1), m.group(2), m.group(3) or '1'
        for f in sorted(glob.glob(f'{d}/c*-b*-r0.json')):
            c, b = map(int, re.findall(r'c(\d+)-b(\d+)', f)[0])
            ranks = [json.load(open(x)) for x in
                     glob.glob(f.replace('-r0.json', '-r*.json'))]
            if len(ranks) < c:
                continue
            r0 = ranks[0]
            row = {'stage': stage, 'var': var, 'rep': rep, 'c': c, 'b': b,
                   't_put': r0['put']['t_start'],
                   'vsize': r0['vsize'], 'shared': r0.get('shared', False)}
            for ph in ('put', 'get'):
                row[ph] = sum(x[ph]['ops'] / x[ph]['elapsed'] for x in ranks)
                row[ph + '_call_ms'] = float(np.median(
                    [x[ph]['p50_us'] for x in ranks])) / 1e3
                t0 = r0[ph]['t_start']
                dur = max(x[ph]['elapsed'] for x in ranks)
                dop = '_put' if ph == 'put' else '_get'
                row[ph + '_dragon'] = window(diag(stage), dop, t0, t0 + dur)
                row[ph + '_handler'] = window(diag(stage),
                                              'r_' + ph, t0, t0 + dur)
            res.append(row)
    return res


def f6(rows):
    """Per-key put time on the Dragon thread, per phase in run order."""
    stages = [('m1', 'A-m1: endpoint under dragon -m -N 1 (variant sweep)'),
              ('qm', 'Q-m: fresh endpoint, dragon -m -N 1'),
              ('qs', 'Q-s: fresh endpoint, dragon -s')]
    cols = {1: BLUE, 2: AQUA, 4: ORANGE}
    fig, axs = plt.subplots(len(stages), 1, figsize=(14, 10), sharey=True)
    for ax, (st, title) in zip(axs, stages):
        rs = sorted((r for r in rows if r['stage'] == st and r['put_dragon']
                     and 'key_us' in r['put_dragon']),
                    key=lambda r: r['t_put'])
        x = np.arange(len(rs))
        ax.bar(x, [r['put_dragon']['key_us'] for r in rs], 0.8,
               color=[cols.get(r['c'], GRAY) for r in rs])
        ax.set_xticks(x, [f"{r['var']}{'' if r['rep'] == '1' else r['rep']}"
                          f"\nb{r['b']}" for r in rs], fontsize=8)
        ax.set_title(title, fontsize=12, loc='left')
        ax.set_ylabel('µs per key')
    axs[0].legend(handles=[Patch(color=col, label=f'{c} client'
                                 f'{"s" if c > 1 else ""}')
                           for c, col in cols.items()],
                  frameon=False, ncol=3, loc='upper right')
    fig.suptitle('F6  Put collapse = a persistent slow mode, not concurrency: '
                 'time per key in DDict.__setitem__, phases in run order',
                 fontsize=14)
    fig.tight_layout()
    for ext in ('png', 'svg'):
        fig.savefig(f'{FIGS}/F6_put_collapse.{ext}')
    plt.close(fig)


def f8():
    """Native replay of the A-m1 put sequence: one writer, no ORBIT."""
    fig, ax = plt.subplots(figsize=(12, 5.2))
    w = 0.4
    for i, (fl, lbl, col) in enumerate((('mN1', 'dragon -m -N 1', BLUE),
                                        ('s', 'dragon -s', ORANGE))):
        f = f'{DATA}/inv-native-replay-{fl}.json'
        if not os.path.exists(f):
            continue
        ph = json.load(open(f))['phases_us_per_put']
        x = np.arange(len(ph)) + (i - 0.5) * w
        ax.bar(x, [v for _, v in ph], w * 0.92, color=col, label=lbl)
        names = [n for n, _ in ph]
    ax.set_xticks(np.arange(len(names)), names, rotation=30, ha='right',
                  fontsize=11)
    ax.set_ylabel('time per put (µs)')
    ax.set_ylim(bottom=0)
    ax.set_title('F8  Native replay, ONE writer: 64-key sets of 1 KiB values '
                 'written in turn (cN = N sets)')
    ax.legend(frameon=False)
    fig.tight_layout()
    for ext in ('png', 'svg'):
        fig.savefig(f'{FIGS}/F8_native_replay.{ext}')
    plt.close(fig)


if __name__ == '__main__':
    rows = runs()
    with open(f'{DATA}/inv-summary.json', 'w') as f:
        json.dump(rows, f, indent=1)
    os.makedirs(FIGS, exist_ok=True)
    f6(rows)
    f8()
    fmt = lambda w, k: f"{w[k]:.0f}" if w and k in w else '-'   # noqa: E731
    print('| stage | variant | rep | c | b | put k/s | get k/s | put call ms '
          '| put handler ms | put q ms | put x ms | put key µs | get key µs '
          '| Dragon busy (put) |')
    print('|' + '---|' * 14)
    for r in rows:
        pd, gd = r['put_dragon'], r['get_dragon']
        print(f"| {r['stage']} | {r['var']} | {r['rep']} | {r['c']} | {r['b']}"
              f" | {r['put']:.0f} | {r['get']:.0f} | {r['put_call_ms']:.1f}"
              f" | {(r['put_handler'] or {}).get('h_ms', 0):.1f}"
              f" | {pd['q_ms'] if pd else 0:.1f}"
              f" | {pd['x_ms'] if pd else 0:.1f} | {fmt(pd, 'key_us')}"
              f" | {fmt(gd, 'key_us')}"
              f" | {pd['busy'] if pd else 0:.2f} |")
