#!/usr/bin/env python3
"""
fiobench.py - fio-based random read/write benchmark

Usage:
    python fiobench.py -f fio.tmp --mode both
    python fiobench.py -f fio.tmp --mode write
    python fiobench.py -f fio.tmp --mode read --size 10G
"""

import argparse
import json
import subprocess
import sys


FIO_BIN = "fio"

COMMON_ARGS = [
    "--bs=4k",
    "--ioengine=posixaio",
    "--direct=1",
    "--iodepth=64",
    "--numjobs=8",
    "--group_reporting",
    "--runtime=60s",
    "--time_based",
    "--output-format=json",
]


def _run_fio(name, rw, filename, size):
    cmd = [
        FIO_BIN,
        f"--name={name}",
        f"--rw={rw}",
        f"--size={size}",
        f"--filename={filename}",
    ] + COMMON_ARGS

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"fio error:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)

    data = json.loads(result.stdout)
    job = data["jobs"][0]
    return job


def _extract(job, rw_key):
    stats = job[rw_key]
    return {
        "iops": stats["iops"],
        "iops_mean": stats["iops_mean"],
        "bw_kb_s": stats["bw"],
        "bw_mean_kb_s": stats["bw_mean"],
        "lat_ns_mean": stats["lat_ns"]["mean"],
        "lat_ns_stddev": stats["lat_ns"]["stddev"],
    }


def fio_bench(filename, mode="both", size="50G"):
    """
    Run fio benchmark(s) and return results as a dict.

    Parameters
    ----------
    filename : str
        Path to the file fio will use for I/O.
    mode : str
        One of 'read', 'write', or 'both'.
    size : str
        File size passed to fio (e.g. '50G', '10G').

    Returns
    -------
    dict with keys 'read' and/or 'write', each containing:
        iops, iops_mean, bw_kb_s, bw_mean_kb_s, lat_ns_mean, lat_ns_stddev
    """
    if mode not in ("read", "write", "both"):
        raise ValueError(f"mode must be 'read', 'write', or 'both', got {mode!r}")

    results = {}

    if mode in ("write", "both"):
        print("Running random write benchmark...")
        job = _run_fio("rand-write", "randwrite", filename, size)
        results["write"] = _extract(job, "write")

    if mode in ("read", "both"):
        print("Running random read benchmark...")
        job = _run_fio("rand-read", "randread", filename, size)
        results["read"] = _extract(job, "read")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="fio random read/write benchmark",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-f", "--filename", default="fio.tmp", help="Path to fio test file"
    )
    parser.add_argument(
        "-m",
        "--mode",
        choices=["read", "write", "both"],
        default="both",
        help="Benchmark mode",
    )
    parser.add_argument("-s", "--size", default="50G", help="File size (e.g. 50G, 10G)")
    parser.add_argument(
        "-j", "--json", metavar="FILE", help="Write results to JSON file"
    )
    args = parser.parse_args()

    results = fio_bench(args.filename, mode=args.mode, size=args.size)

    print()
    for rw, stats in results.items():
        print(f"[{rw.upper()}]")
        print(f"  IOPS:          {stats['iops']:.0f}  (mean: {stats['iops_mean']:.0f})")
        print(
            f"  Bandwidth:     {stats['bw_kb_s'] / 1024:.1f} MB/s  (mean: {stats['bw_mean_kb_s'] / 1024:.1f} MB/s)"
        )
        print(
            f"  Latency mean:  {stats['lat_ns_mean'] / 1000:.1f} µs  (stddev: {stats['lat_ns_stddev'] / 1000:.1f} µs)"
        )
        print()

    if args.json:
        with open(args.json, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results written to {args.json}")


if __name__ == "__main__":
    main()
