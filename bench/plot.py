#!/usr/bin/env python3
"""Turn benchmark results into the graphs in docs/img and the tables in docs/BENCHMARKS.md.

usage: plot.py [OUT_DIR]   (default: docs/img next to this script)
Results come from $BENCH_ROOT/results. For each (suite, client, variant) the runs tagged
with $TAGS (comma-separated tag prefixes, first match wins; default "rel,r") are used,
and the median run is reported.
"""
import glob, json, os, re, statistics, sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.environ.get("BENCH_ROOT", "/root/bench")
RES = f"{ROOT}/results"
OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "docs", "img")
TAGS = os.environ.get("TAGS", "rel,r").split(",")

CLIENTS = [("nzbfast", "default", "nzbfast"), ("sab", "default", "SABnzbd"), ("nzbget", "tuned", "NZBGet")]
COLOR = {"nzbfast": "#e8590c", "sab": "#4c6ef5", "nzbget": "#2f9e44"}
SUITES = [("movies", "Movies\n6 × 4 GB"), ("tv", "TV\n40 × 400 MB"), ("pp", "Unpack mix\n11 releases"), ("repair", "Repair\n3 damaged + 1 dead")]
SCENARIOS = [
    ("takedown", "Takedowns on main\n(slow 430s, backup has them)"),
    ("slowprov", "One slow provider\n(2 MB/s per connection)"),
    ("connlimit", "Connection limit\n(20 allowed, 50 configured)"),
    ("deadprov", "Dead provider\n(logins hang)"),
    ("flaky", "Flaky provider\n(drops, 412 replies)"),
    ("outage", "Outage\n(only provider down 1–6 s)"),
]

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
    "axes.edgecolor": "#888", "axes.labelcolor": "#333", "xtick.color": "#444", "ytick.color": "#444",
    "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white",
    "axes.grid": True, "grid.color": "#e6e6e6", "grid.linewidth": 0.8, "axes.axisbelow": True,
})


def runs(suite, client, variant, prefix=""):
    """Results of one configuration, for the first tag prefix that has any."""
    for t in TAGS:
        pat = re.compile(rf"^{re.escape(suite)}\.{client}\.{variant}\.{re.escape(prefix)}{t}\d+\.json$")
        found = [json.load(open(f)) for f in sorted(glob.glob(f"{RES}/{suite}.{client}.{variant}.*.json")) if pat.match(os.path.basename(f))]
        if found:
            return found
    return []


def median_run(rs):
    if not rs:
        return None
    rs = sorted(rs, key=lambda r: r["wall_s"])
    return rs[(len(rs) - 1) // 2]


def wall(suite, client, variant, prefix=""):
    rs = runs(suite, client, variant, prefix)
    return statistics.median(r["wall_s"] for r in rs) if rs else None


def unfinished(suite, client, variant):
    """The median run hit the deadline with jobs still in the queue."""
    r = median_run(runs(suite, client, variant))
    return bool(r and r.get("timed_out"))


def label_bars(ax, bars, fmt="{:.1f} s", horizontal=False):
    for b in bars:
        v = b.get_width() if horizontal else b.get_height()
        if v <= 0:
            continue
        if horizontal:
            ax.text(b.get_x() + v * 1.04, b.get_y() + b.get_height() / 2, fmt.format(v), va="center", ha="left", fontsize=8.5, color="#333")
        else:
            ax.text(b.get_x() + b.get_width() / 2, v * 1.03, fmt.format(v), va="bottom", ha="center", fontsize=8.5, color="#333")


def grouped(ax, groups, values, log=False, xlabel="Seconds to finish (lower is better)", dnf=(), legend="lower right"):
    """Horizontal grouped bars: groups top to bottom, one bar per client. `dnf` holds
    (group index, client) pairs that did not finish before the deadline."""
    n = len(CLIENTS)
    h = 0.8 / n
    for k, (c, v, name) in enumerate(CLIENTS):
        ys = [i + (k - (n - 1) / 2) * h for i in range(len(groups))]
        xs = [values[g][c] or 0 for g in range(len(groups))]
        bars = ax.barh(ys, xs, height=h * 0.92, color=COLOR[c], label=name)
        for g, b in enumerate(bars):
            if (g, c) in dnf:
                b.set_hatch("///")
                b.set_alpha(0.55)
                ax.text(b.get_width() * 1.04, b.get_y() + b.get_height() / 2, f"did not finish in {b.get_width():.0f} s", va="center", fontsize=8.5, color="#333")
            elif b.get_width() > 0:
                label_bars(ax, [b], horizontal=True)
    ax.set_yticks(range(len(groups)), [g for g in groups])
    ax.invert_yaxis()
    ax.grid(axis="y", visible=False)
    top = max([v for g in values for v in g.values() if v] or [1])
    if log:
        ax.set_xscale("log")
        ax.set_xlim(1, top * 12)
    else:
        ax.set_xlim(0, top * 1.18)
    ax.set_xlabel(xlabel)
    ax.legend(loc=legend, frameon=False)


def rate_series(r, step=1.0):
    """(seconds, Gbit/s) from the bytes-received samples of one run."""
    s = r["series"]
    out, j = [], 0
    for i in range(1, len(s)):
        if s[i][0] - s[j][0] >= step or i == len(s) - 1:
            dt = s[i][0] - s[j][0]
            if dt > 0:
                out.append(((s[i][0] + s[j][0]) / 2, (s[i][1] - s[j][1]) * 8 / dt / 1e9))
            j = i
    return out


def save(fig, name):
    os.makedirs(OUT, exist_ok=True)
    fig.savefig(f"{OUT}/{name}", bbox_inches="tight", metadata={"Date": None})
    if os.environ.get("PNG"):
        fig.savefig(f"{OUT}/{name[:-4]}.png", bbox_inches="tight", dpi=110)
    plt.close(fig)
    print("wrote", f"{OUT}/{name}")


def fig_suites():
    values = [{c: wall(s, c, v) for c, v, _ in CLIENTS} for s, _ in SUITES]
    fig, ax = plt.subplots(figsize=(8.5, 4.6))
    grouped(ax, [l for _, l in SUITES], values)
    ax.set_title("NZB to finished files: 71 GB corpus, 50 connections", loc="left", fontsize=11.5, color="#222")
    save(fig, "suites.svg")
    return values


def fig_providers():
    values = [{c: wall(s, c, v) for c, v, _ in CLIENTS} for s, _ in SCENARIOS]
    dnf = {(g, c) for g, (s, _) in enumerate(SCENARIOS) for c, v, _ in CLIENTS if unfinished(s, c, v)}
    fig, ax = plt.subplots(figsize=(8.5, 6.4))
    grouped(ax, [l for _, l in SCENARIOS], values, log=True, xlabel="Seconds to finish, log scale (lower is better)", dnf=dnf)
    ax.set_title("Troublesome providers", loc="left", fontsize=11.5, color="#222")
    save(fig, "providers.svg")
    return values


def fig_timelines(scenarios, name, title, cols=3, cap=40):
    """Throughput per client over time; runs longer than `cap` seconds are cut off with a note."""
    rows = (len(scenarios) + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(4.0 * cols, 2.9 * rows), squeeze=False)
    for ax, (s, label) in zip(axes.flat, scenarios):
        tmax, notes = 0, []
        for c, v, cname in CLIENTS:
            r = median_run(runs(s, c, v))
            if not r:
                continue
            pts = rate_series(r, 0.5)
            pts = [(0, 0)] + [p for p in pts if p[0] <= min(r["wall_s"] + 1, cap)] + ([(r["wall_s"] + 0.5, 0)] if r["wall_s"] < cap else [])
            ax.plot([p[0] for p in pts], [p[1] for p in pts], color=COLOR[c], lw=1.6, label=cname)
            if r["wall_s"] < cap:
                ax.axvline(r["wall_s"], color=COLOR[c], lw=0.8, ls=":")
            else:
                notes.append((c, f"{cname}: {'did not finish in' if r.get('timed_out') else 'done at'} {r['wall_s']:.0f} s →"))
            tmax = max(tmax, min(r["wall_s"], cap))
        for k, (c, text) in enumerate(notes):
            ax.text(0.98, 0.93 - 0.11 * k, text, transform=ax.transAxes, ha="right", va="top", fontsize=8, color=COLOR[c])
        ax.set_title(label.replace("\n", " "), fontsize=9.5, loc="left", color="#222")
        ax.set_xlim(0, tmax * 1.03 + 0.5)
        ax.set_ylim(bottom=0)
        ax.set_xlabel("seconds", fontsize=8.5)
        ax.set_ylabel("Gbit/s", fontsize=8.5)
        ax.tick_params(labelsize=8)
    for ax in list(axes.flat)[len(scenarios):]:
        ax.set_visible(False)
    axes.flat[0].legend(frameon=False, fontsize=8.5)
    fig.suptitle(title, x=0.01, ha="left", fontsize=11.5, color="#222")
    fig.tight_layout()
    save(fig, name)


def fig_routing():
    """nzbfast's traffic per provider in the multi-provider scenarios (from the test server)."""
    picks = [("takedown", {"5571": "main (lacks half the episodes)", "5570": "backup"}), ("slowprov", {"5563": "fast provider", "5572": "slow provider"})]
    fig, axes = plt.subplots(1, len(picks), figsize=(5.0 * len(picks), 3.0), squeeze=False)
    palette = ["#e8590c", "#868e96"]
    for ax, (s, names) in zip(axes.flat, picks):
        r = median_run(runs(s, "nzbfast", "default"))
        if not r or not r.get("mock"):
            ax.set_visible(False)
            continue
        ports, series = r["mock"]["ports"], r["mock"]["series"]
        idx = [ports.index(p) for p in names]
        ts = [(series[i][0] + series[i - 1][0]) / 2 for i in range(1, len(series))]
        ys = [[(series[i][1][k] - series[i - 1][1][k]) * 8 / (series[i][0] - series[i - 1][0]) / 1e9 for i in range(1, len(series))] for k in idx]
        ax.stackplot(ts, *ys, labels=list(names.values()), colors=palette[: len(idx)], alpha=0.9)
        ax.set_title(dict(SCENARIOS)[s].split("\n")[0], fontsize=9.5, loc="left", color="#222")
        ax.set_xlim(0, r["wall_s"] + 0.5)
        total = [r["mock"]["series"][-1][1][k] - r["mock"]["series"][0][1][k] for k in idx]
        share = ", ".join(f"{n.split(' (')[0]} {t / sum(total) * 100:.0f}%" for n, t in zip(names.values(), total))
        ax.text(0.98, 0.62, f"share of data: {share}", transform=ax.transAxes, ha="right", fontsize=8, color="#444")
        ax.set_xlabel("seconds", fontsize=8.5)
        ax.set_ylabel("Gbit/s", fontsize=8.5)
        ax.tick_params(labelsize=8)
        ax.legend(frameon=False, fontsize=8, loc="upper right")
    fig.suptitle("nzbfast: traffic per provider", x=0.01, ha="left", fontsize=11.5, color="#222")
    fig.tight_layout()
    save(fig, "routing.svg")


def fig_rtt():
    groups = [(0, "", "Same data center\n(< 1 ms)"), (30, "rtt30-", "Same continent\n(30 ms)"), (100, "rtt100-", "Across an ocean\n(100 ms)")]
    values = [{c: wall("movies", c, v, prefix) for c, v, _ in CLIENTS} for _, prefix, _ in groups]
    fig, ax = plt.subplots(figsize=(8.5, 3.9))
    grouped(ax, [l for _, _, l in groups], values, legend="upper right")
    ax.set_title("Movies suite (26 GB) vs round-trip time to the provider", loc="left", fontsize=11.5, color="#222")
    save(fig, "rtt.svg")
    return {c: {rtt: values[k][c] for k, (rtt, _, _) in enumerate(groups)} for c, _, _ in CLIENTS}


def fig_resources():
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(9.5, 3.6))
    cpu, mem = {}, {}
    for s, _ in SUITES[:3]:
        cpu[s], mem[s] = {}, {}
        for c, v, _ in CLIENTS:
            r = median_run(runs(s, c, v))
            if r:
                cpu[s][c] = r["cpu_s"] / (r["payload_bytes"] / 1e9)
                mem[s][c] = r["peak_rss"] / 2**30
    n = len(CLIENTS)
    w = 0.8 / n
    for ax, data, ylabel, fmt in [(a1, cpu, "CPU seconds per GB", "{:.2f}"), (a2, mem, "Peak memory (GiB)", "{:.1f}")]:
        for k, (c, v, name) in enumerate(CLIENTS):
            xs = [i + (k - (n - 1) / 2) * w for i in range(len(data))]
            bars = ax.bar(xs, [data[s].get(c, 0) for s in data], width=w * 0.92, color=COLOR[c], label=name)
            label_bars(ax, bars, fmt)
        ax.set_xticks(range(len(data)), [dict(SUITES)[s].split("\n")[0] for s in data])
        ax.set_ylabel(ylabel)
        ax.grid(axis="x", visible=False)
    a1.legend(frameon=False, fontsize=8.5)
    a1.set_title("CPU per GB downloaded", loc="left", fontsize=11, color="#222")
    a2.set_title("Peak memory", loc="left", fontsize=11, color="#222")
    fig.tight_layout()
    save(fig, "resources.svg")
    return cpu, mem


def fig_real():
    """Live providers: throughput of each client's median run."""
    if not runs("real", "nzbfast", "default"):
        return
    fig, ax = plt.subplots(figsize=(8.5, 3.6))
    for c, v, name in CLIENTS:
        r = median_run(runs("real", c, v))
        if not r:
            continue
        pts = [(0, 0)] + rate_series(r, 2.0)
        ax.plot([p[0] for p in pts], [p[1] for p in pts], color=COLOR[c], lw=1.6, label=f"{name}: done at {r['wall_s']:.0f} s")
        ax.axvline(r["wall_s"], color=COLOR[c], lw=0.8, ls=":")
    ax.set_xlabel("seconds")
    ax.set_ylabel("Gbit/s")
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False)
    ax.set_title("24 real releases (88.5 GB) from live Usenet providers", loc="left", fontsize=11.5, color="#222")
    save(fig, "real.svg")


def table(rows, header):
    print("| " + " | ".join(header) + " |")
    print("| " + " | ".join("---" for _ in header) + " |")
    for r in rows:
        print("| " + " | ".join(r) + " |")
    print()


def fmt(x, unit=" s"):
    return "–" if x is None else f"{x:.1f}{unit}"


if __name__ == "__main__":
    sv = fig_suites()
    pv = fig_providers()
    fig_timelines([("movies", "Movies: 6 × 4 GB"), ("tv", "TV: 40 × 400 MB"), ("pp", "Unpack mix: 11 releases")], "timeline.svg",
                  "Throughput while downloading (median run)")
    fig_timelines(SCENARIOS, "providers-timeline.svg", "Throughput with troublesome providers (median run)")
    fig_routing()
    rv = fig_rtt()
    cpu, mem = fig_resources()
    fig_real()
    names = [n for _, _, n in CLIENTS]
    table([[l.replace("\n", ": ")] + [fmt(v[c]) for c, _, _ in CLIENTS] for (s, l), v in zip(SUITES, sv)], ["Suite"] + names)
    table([[l.replace("\n", " ")] + [f"did not finish in {v[c]:.0f} s" if unfinished(s, c, var) else fmt(v[c]) for c, var, _ in CLIENTS]
           for (s, l), v in zip(SCENARIOS, pv)], ["Scenario"] + names)
    table([[f"{rtt} ms"] + [fmt(rv[c].get(rtt)) for c, _, _ in CLIENTS] for rtt in (0, 30, 100)], ["RTT"] + names)
    for s, _ in SUITES + SCENARIOS:
        for c, v, _ in CLIENTS:
            rs = runs(s, c, v)
            if rs:
                okn = [sum(j["ok"] for j in r["jobs"].values()) for r in rs]
                md5 = [sum(bool(j.get("md5_ok")) for j in r["jobs"].values()) for r in rs]
                print(f"{s:9} {c:8} runs={len(rs)} walls={[r['wall_s'] for r in rs]} ok={okn} md5={md5} cpu={[r['cpu_s'] for r in rs]} peak_rss_gib={[round(r['peak_rss'] / 2**30, 1) for r in rs]}")
