#!/usr/bin/env python3
"""Plot latency percentiles recorded in results/history.csv.

Each run of hft_lob / engine_subscriber appends one row (see
include/results.hpp). This turns that log into a chart:

  - one run recorded  -> bar chart of that run's percentiles
  - multiple runs      -> trend line of each percentile across commits

Usage:
    python3 scripts/plot_latency.py
    python3 scripts/plot_latency.py --latest-only
    python3 scripts/plot_latency.py --csv results/history.csv --out results/latency.png
"""
import argparse
import csv

import matplotlib.pyplot as plt

PERCENTILE_COLS = ["p50_ns", "p90_ns", "p99_ns", "p99_9_ns", "p99_99_ns"]
LABELS = ["p50", "p90", "p99", "p99.9", "p99.99"]


def plot_single_run(row, out_path):
    values = [float(row[c]) for c in PERCENTILE_COLS]
    plt.figure(figsize=(7, 4))
    bars = plt.bar(LABELS, values, color="#3b82f6")
    plt.yscale("log")
    plt.ylabel("latency (ns, log scale)")
    plt.title(f"{row['run_label']}  —  n={row['n']}  —  commit {row['git_hash']}")
    for bar, v in zip(bars, values):
        plt.text(bar.get_x() + bar.get_width() / 2, v, f"{v:.0f}", ha="center", va="bottom")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)


def plot_trend(rows, out_path):
    plt.figure(figsize=(9, 5))
    x = range(len(rows))
    for col, label in zip(PERCENTILE_COLS, LABELS):
        plt.plot(x, [float(r[col]) for r in rows], marker="o", label=label)
    plt.yscale("log")
    plt.xticks(list(x), [r["git_hash"] for r in rows], rotation=45, ha="right")
    plt.ylabel("latency (ns, log scale)")
    plt.xlabel("commit")
    plt.title("Latency percentiles across runs")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="results/history.csv")
    ap.add_argument("--out", default="results/latency.png")
    ap.add_argument("--latest-only", action="store_true",
                     help="Only plot the most recent run, as a bar chart")
    args = ap.parse_args()

    with open(args.csv) as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print(f"no data in {args.csv} — run the harness at least once first")
        return

    if args.latest_only or len(rows) == 1:
        plot_single_run(rows[-1], args.out)
    else:
        plot_trend(rows, args.out)

    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()