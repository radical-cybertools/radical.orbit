#!/usr/bin/env python3
"""Reduce the raw benchmark JSON to tables and plots.

    python analyze.py [data_dir] [out_dir]

``data_dir`` (default ``./data``) holds what ``run_bench.sh`` wrote;
writes ``summary.json`` and Markdown tables (stdout) and figures
F1-F5 (PNG + SVG) to ``out_dir`` (default ``./figs``).
"""

import glob
import json
import os
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt                     # noqa: E402

DATA = sys.argv[1] if len(sys.argv) > 1 else 'data'
FIGS = sys.argv[2] if len(sys.argv) > 2 else 'figs'

# categorical slots, fixed order (dataviz reference palette, light mode)
BLUE, ORANGE, AQUA, YELLOW, MAGENTA, VIOLET = \
    '#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#6250d6'
GRAY = '#52514e'

plt.rcParams.update({'font.size': 14, 'axes.titlesize': 15,
                     'axes.labelsize': 14, 'legend.fontsize': 12,
                     'lines.linewidth': 2, 'lines.markersize': 8,
                     'axes.grid': True, 'grid.color': '#dddddd',
                     'grid.linewidth': 0.8, 'axes.spines.top': False,
                     'axes.spines.right': False, 'figure.dpi': 110})


def load(path):
    path = os.path.join(DATA, path)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def conc(tag, c, b):
    """Aggregate ops/s over the ranks of one concurrent run (None if
    missing or incomplete)."""
    files = glob.glob(os.path.join(DATA, f'conc-{tag}', f'c{c}-b{b}-r*.json'))
    if len(files) < c:
        return None
    ranks = [json.load(open(f)) for f in files]
    out = {'ranks': len(ranks),
           'late': max(r.get('put_late_s', 0) for r in ranks)}
    for ph in ('put', 'get'):
        out[ph] = sum(r[ph]['ops'] / r[ph]['elapsed'] for r in ranks)
        out[ph + '_p50_us'] = sorted(r[ph]['p50_us'] for r in ranks)[
            len(ranks) // 2]
    return out


def save(fig, name):
    os.makedirs(FIGS, exist_ok=True)
    if not fig.get_layout_engine():
        fig.tight_layout()
    for ext in ('png', 'svg'):
        fig.savefig(os.path.join(FIGS, f'{name}.{ext}'))
    plt.close(fig)


S = {}          # summary


# -- F1: latency vs value size ----------------------------------------------
def f1():
    orb, nat = load('lat-compute.json'), load('native-lat.json')
    nd = load('lat-nodeflate.json')
    if not orb:
        return
    sizes = sorted(int(s) for s in orb['sizes'])
    S['latency'] = {'orbit': orb['sizes'],
                    'native': nat['sizes'] if nat else None,
                    'orbit_nodeflate': nd['sizes'] if nd else None,
                    'noop_list': orb['noop_list'],
                    'noop_keys': orb['noop_keys']}
    fig, axs = plt.subplots(1, 2, figsize=(13, 5.2), sharey=True)
    for ax, op in zip(axs, ('put', 'get')):
        for d, lbl, col in ((orb, 'ORBIT (client → broker → endpoint)',
                             BLUE),
                            (nd, 'ORBIT, WebSocket deflate off (diag.)',
                             VIOLET),
                            (nat, 'native DDict (in runtime)', ORANGE)):
            if not d:
                continue
            p50 = [d['sizes'][str(s)][op]['p50_us'] / 1e3 for s in sizes]
            p90 = [d['sizes'][str(s)][op]['p90_us'] / 1e3 for s in sizes]
            p99 = [d['sizes'][str(s)][op]['p99_us'] / 1e3 for s in sizes]
            ax.fill_between(sizes, p50, p90, color=col, alpha=0.18, lw=0)
            ax.plot(sizes, p50, 'o-', color=col, label=f'{lbl}, p50')
            ax.plot(sizes, p99, ':', color=col, lw=1.5)
        for k, ls, lbl in (('noop_list', '--', "no-op 'ddict/list'"),
                           ('noop_keys', '-.', "'keys' (Dragon thread)")):
            y = orb[k]['p50_us'] / 1e3
            ax.axhline(y, color=GRAY, ls=ls, lw=1.5)
            ax.annotate(f'{lbl} p50 {y:.2f} ms', (sizes[0], y),
                        xytext=(0, 4), textcoords='offset points',
                        fontsize=11, color=GRAY)
        ax.set_xscale('log', base=2)
        ax.set_yscale('log')
        ax.set_xticks(sizes, ['64 B', '1 KiB', '16 KiB', '256 KiB',
                              '1 MiB'])
        ax.set_xlabel('value size (bytes, one key per call)')
        ax.set_title(f"sequential {'update({k: v})' if op == 'put' else 'get(k)'}")
    axs[0].set_ylabel('latency per call (ms, log)')
    axs[0].legend(loc='upper left', frameon=False)
    fig.suptitle('F1  DDict latency, 1 client, 1-node DDict (line: p50, '
                 'band: p50-p90, dotted: p99)', fontsize=15)
    save(fig, 'F1_latency')


# -- F1b: latency vs value size, WebSocket compression off / on / auto --------
def f1b():
    cfgs = [('off', 'off', BLUE, 'o-'),
            ('on', 'on (deflate level 1)', VIOLET, 's-'),
            ('auto', 'auto (resolved: off)', AQUA, 'D--')]
    runs = {c: load(f'lat-wscomp-{c}.json') for c, *_ in cfgs}
    # first run (2026-11-04): hard-wired default deflate, and the nodeflate
    # diagnostic patch; thin reference lines
    refs = [(load('lat-compute.json'), 'first run: old default deflate',
             '-.'), (load('lat-nodeflate.json'), 'first run: nodeflate patch',
                     ':')]
    nat = load('native-lat.json')
    if not runs['off']:
        return
    sizes = sorted(int(x) for x in runs['off']['sizes'])
    S['latency_wscomp'] = {c: (r['sizes'] if r else None)
                           for c, r in runs.items()}
    fig, axs = plt.subplots(1, 2, figsize=(13, 5.4), sharey=True)
    for ax, op in zip(axs, ('put', 'get')):
        for c, lbl, col, st in cfgs:
            r = runs[c]
            if r:
                ax.plot(sizes, [r['sizes'][str(x)][op]['p50_us'] / 1e3
                                for x in sizes], st, color=col, label=lbl,
                        lw=1.5 if c == 'nodeflate' else 2)
        for r, lbl, st in refs:
            if r:
                ax.plot(sizes, [r['sizes'][str(x)][op]['p50_us'] / 1e3
                                for x in sizes], st, color=GRAY, lw=1.3,
                        label=lbl)
        if nat:
            ax.plot(sizes, [nat['sizes'][str(x)][op]['p50_us'] / 1e3
                            for x in sizes], 'o-', color=ORANGE,
                    label='native DDict (first run)')
        ax.set_xscale('log', base=2)
        ax.set_yscale('log')
        ax.set_xticks(sizes, ['64 B', '1 KiB', '16 KiB', '256 KiB', '1 MiB'])
        ax.set_xlabel('value size (bytes, one key per call)')
        ax.set_title(f"sequential {'update({k: v})' if op == 'put' else 'get(k)'}")
    axs[0].set_ylabel('p50 latency per call (ms, log)')
    axs[0].legend(loc='upper left', frameon=False, fontsize=10)
    fig.suptitle('F1b  ORBIT DDict p50 latency vs WebSocket compression '
                 '(RADICAL_ORBIT_WS_COMPRESSION, PR #159)', fontsize=15)
    save(fig, 'F1b_latency_compression')
    print('| value | ' + ' | '.join(f'{c} put / get (ms)' for c, *_ in cfgs)
          + ' |')
    print('|---' * (len(cfgs) + 1) + '|')
    for x in sizes:
        cells = []
        for c, *_ in cfgs:
            r = runs[c]
            cells.append('-' if not r else
                         f"{r['sizes'][str(x)]['put']['p50_us']/1e3:.2f} / "
                         f"{r['sizes'][str(x)]['get']['p50_us']/1e3:.2f}")
        print(f'| {x:,} B | ' + ' | '.join(cells) + ' |')
    print()


# -- F2: throughput vs batch size --------------------------------------------
def f2():
    d = load('batch-compute.json')
    if not d:
        return
    bs = sorted(int(b) for b in d if b.isdigit())
    S['batch'] = {b: d[str(b)] for b in bs}
    fig, ax = plt.subplots(figsize=(8.5, 5.5))
    for op, col in (('put', BLUE), ('get', AQUA)):
        y = [d[str(b)][op]['ops_s'] for b in bs]
        ax.plot(bs, y, 'o-', color=col,
                label='update(batch)' if op == 'put' else 'get_many(batch)')
        ax.annotate(f'{y[-1]:,.0f}/s', (bs[-1], y[-1]), xytext=(6, 0),
                    textcoords='offset points', fontsize=11, va='center')
    ax.set_xscale('log', base=2)
    ax.set_yscale('log')
    ax.set_xticks(bs, [str(b) for b in bs])
    ax.set_xlabel('keys per call (batch size)')
    ax.set_ylabel('keys/s (1 KiB values; MB/s = keys/s ÷ 977)')
    ax.set_title('F2  ORBIT DDict throughput vs batch size (1 client)')
    ax.legend(frameon=False)
    save(fig, 'F2_batch')


# -- F3: throughput vs concurrent clients ------------------------------------
def f3():
    cs = [1, 2, 4, 8, 16]
    fig, axs = plt.subplots(1, 2, figsize=(13, 5.2))
    S['conc_k1'] = {}
    for ax, b in zip(axs, (1, 64)):
        pts = [(c, conc('k1', c, b)) for c in cs]
        pts = [(c, r) for c, r in pts if r]
        S['conc_k1'][b] = {c: r for c, r in pts}
        for op, col in (('put', BLUE), ('get', AQUA)):
            ax.plot([c for c, _ in pts], [r[op] for _, r in pts], 'o-',
                    color=col, label=f'ORBIT {op}')
        for p in (4, 16):
            n = load(f'native-conc-k1-p{p}.json')
            if n and b == 1:
                for op, m in (('put', 's'), ('get', 'D')):
                    ax.plot(p, n[f'{op}_ops_s'], m, color=ORANGE,
                            label=f'native {op}' if p == 4 else None)
        ax.set_xscale('log', base=2)
        ax.set_yscale('log')
        ax.set_xticks(cs, [str(c) for c in cs])
        ax.set_xlabel('concurrent client processes (over 4 nodes)')
        ax.set_title(f'{b} key{"s" if b > 1 else ""} per call')
        ax.legend(frameon=False)
    axs[0].set_ylabel('aggregate keys/s (1 KiB values, log)')
    fig.suptitle('F3  Throughput vs concurrent clients, one endpoint, '
                 '1-node DDict', fontsize=15)
    save(fig, 'F3_clients')


# -- F4: scaling vs nodes ----------------------------------------------------
def f4():
    ks = [1, 2, 4]
    fig, axs = plt.subplots(1, 2, figsize=(13, 6.6), sharey=True)
    S['scaling'] = {}
    for ax, b in zip(axs, (1, 64)):
        S['scaling'][b] = {}
        for lbl, cfn, ls in (('16 clients', lambda k: 16, '-'),
                             ('4 clients/node', lambda k: 4 * k, '--')):
            pts = [(k, conc(f'k{k}', cfn(k), b)) for k in ks]
            pts = [(k, r) for k, r in pts if r]
            S['scaling'][b][lbl] = {k: r for k, r in pts}
            for op, col in (('put', BLUE), ('get', AQUA)):
                ax.plot([k for k, _ in pts], [r[op] for _, r in pts],
                        'o' + ls, color=col, label=f'{op}, {lbl}')
        vb = conc('varb', 16, b)
        S['scaling'][b]['varb'] = vb
        if vb:
            for op, m in (('put', 's'), ('get', 'D')):
                ax.plot(4, vb[op], m, color=MAGENTA, ms=10,
                        label=f'{op}, 4 endpoints × 1-node DDict')
        if b == 1:
            for op, m in (('put', 's'), ('get', 'D')):
                pts = [(k, load(f'native-conc-k{k}-p16.json')) for k in ks]
                pts = [(k, n) for k, n in pts if n]
                if pts:
                    ax.plot([k for k, _ in pts],
                            [n[f'{op}_ops_s'] for _, n in pts], m + ':',
                            color=ORANGE,
                            label=f'native {op}, 16 procs')
        ax.set_yscale('log')
        ax.set_xticks(ks, [str(k) for k in ks])
        ax.set_xlabel('nodes in the Dragon runtime = DDict n_nodes')
        ax.set_title(f'{b} key{"s" if b > 1 else ""} per call')
    axs[0].set_ylabel('aggregate keys/s (1 KiB values, log)')
    hs, ls = axs[0].get_legend_handles_labels()
    fig.legend(hs, ls, loc='lower center', ncol=4, frameon=False,
               fontsize=11)
    fig.suptitle('F4  Scaling: one ORBIT endpoint on a 1/2/4-node Dragon '
                 'runtime', fontsize=15)
    fig.tight_layout(rect=(0, 0.16, 1, 1))
    save(fig, 'F4_scaling')


# -- F5: broker placement ----------------------------------------------------
def f5():
    cmp, lgn = load('lat-compute.json'), load('lat-login.json')
    if not (cmp and lgn):
        return
    sizes = sorted(int(s) for s in cmp['sizes'])
    names = ['64B', '1K', '16K', '256K', '1M']
    fig, axs = plt.subplots(1, 2, figsize=(13, 5.2))
    ax, w = axs[0], 0.2
    for i, (d, lbl, col) in enumerate(((cmp, 'broker on compute node',
                                        BLUE), (lgn, 'broker on login node',
                                                VIOLET))):
        for j, op in enumerate(('put', 'get')):
            x = [n + (2 * i + j - 1.5) * w for n in range(len(sizes))]
            y = [d['sizes'][str(s)][op]['p50_us'] / 1e3 for s in sizes]
            ax.bar(x, y, w * 0.9, color=col, alpha=1.0 if op == 'put' else
                   0.55, label=f'{lbl}, {op}')
    ax.set_yscale('log')
    ax.set_xticks(range(len(sizes)), names)
    ax.set_xlabel('value size')
    ax.set_ylabel('p50 latency per call (ms, log)')
    ax.set_title('latency (1 client, 1-node DDict)')
    ax.legend(frameon=False, fontsize=10)

    ax = axs[1]
    S['placement'] = {}
    groups = [(k, b) for k in (1, 4) for b in (1, 64)]
    for i, (tag, lbl, col) in enumerate((('k', 'compute', BLUE),
                                         ('login-k', 'login', VIOLET))):
        for j, op in enumerate(('put', 'get')):
            x, y = [], []
            for n, (k, b) in enumerate(groups):
                r = conc(f'{tag}{k}', 16, b)
                S['placement'][f'{lbl}-k{k}-b{b}'] = r
                if r:
                    x.append(n + (2 * i + j - 1.5) * w)
                    y.append(r[op])
            ax.bar(x, y, w * 0.9, color=col, alpha=1.0 if op == 'put' else
                   0.55, label=f'broker on {lbl}, {op}')
    ax.set_xticks(range(len(groups)),
                  [f'{k} node{"s" if k > 1 else ""}\nbatch {b}'
                   for k, b in groups])
    ax.set_ylabel('aggregate keys/s, 16 clients')
    ax.set_title('throughput (16 clients, 1 KiB values)')
    ax.legend(frameon=False, fontsize=10)
    fig.suptitle('F5  Broker placement: compute node vs login node',
                 fontsize=15)
    save(fig, 'F5_broker_placement')


def tables():
    lat = S.get('latency')
    if lat:
        print('| value | ORBIT put p50/p99 ms | ORBIT get p50/p99 ms | '
              'native put p50 ms | native get p50 ms |')
        print('|---|---|---|---|---|')
        for s, v in lat['orbit'].items():
            n = (lat['native'] or {}).get(s)
            print(f"| {int(s):,} B | {v['put']['p50_us']/1e3:.2f} / "
                  f"{v['put']['p99_us']/1e3:.2f} | {v['get']['p50_us']/1e3:.2f}"
                  f" / {v['get']['p99_us']/1e3:.2f} | "
                  f"{n['put']['p50_us']/1e3:.3f} | {n['get']['p50_us']/1e3:.3f} |"
                  if n else '')
        for k in ('noop_list', 'noop_keys'):
            print(f"\n{k}: p50 {lat[k]['p50_us']/1e3:.2f} ms, "
                  f"p99 {lat[k]['p99_us']/1e3:.2f} ms")
    print()
    for name in ('batch', 'conc_k1', 'scaling', 'placement'):
        if name in S:
            print(f'## {name}\n```\n{json.dumps(S[name], indent=1, default=str)[:4000]}\n```')


if __name__ == '__main__':
    for f in (f1, f1b, f2, f3, f4, f5):
        try:
            f()
        except Exception as e:                      # keep going: partial data
            print(f'{f.__name__}: {e!r}', file=sys.stderr)
    with open(os.path.join(DATA, 'summary.json'), 'w') as fh:
        json.dump(S, fh, indent=1, default=str)
    tables()
