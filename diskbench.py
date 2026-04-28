#!/usr/bin/env python3
"""
diskbench.py - simple sequential read/write disk benchmark

Usage:
    python diskbench.py -f /scratch/test.tmp -s 4096 -b 1024
    python diskbench.py -f /scratch/test.tmp -s 4096 -b 1024 --zero
"""

import os
import sys
import argparse
import json
import ctypes
import time


def aligned_buffer(size, alignment=4096):
    """Allocate a buffer aligned to `alignment` bytes, required by O_DIRECT."""
    buf = (ctypes.c_char * (size + alignment))()
    offset = alignment - (ctypes.addressof(buf) % alignment)
    return (ctypes.c_char * size).from_buffer(buf, offset)


def open_direct(path, flags):
    """Open file with O_DIRECT if available, fallback to normal open with warning."""
    try:
        return os.open(path, flags | os.O_DIRECT, 0o600)
    except AttributeError:
        print("Warning: O_DIRECT not available on this platform, results may include cache effects",
              file=sys.stderr)
        return os.open(path, flags, 0o600)


def write_test(path, total_mb, block_kb, zero=False, show_progress=True):
    """
    Sequential write test.
    Returns (throughput_mb_s, elapsed_s).
    block_kb: block size in KB (must be multiple of 512 for O_DIRECT alignment).
    zero: fill buffer with zeros instead of random data.
    """
    block_size = block_kb * 1024
    total_bytes = total_mb * 1024 * 1024
    n_blocks = total_bytes // block_size

    buf = aligned_buffer(block_size)
    if not zero:
        ctypes.memmove(buf, os.urandom(block_size), block_size)
    # zero buffer is already zeroed by ctypes

    fd = open_direct(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
    start = time.perf_counter()
    written = 0
    for i in range(n_blocks):
        os.write(fd, buf)
        written += block_size
        if show_progress:
            sys.stdout.write(f"\rWrite: {(i + 1) * 100 // n_blocks:3d}%")
            sys.stdout.flush()
    os.fsync(fd)  # single fsync at the end
    elapsed = time.perf_counter() - start
    os.close(fd)

    if show_progress:
        print()

    return written / 1024 / 1024 / elapsed, elapsed


def read_test(path, total_mb, block_kb, show_progress=True):
    """
    Sequential read test.
    Returns (throughput_mb_s, elapsed_s).
    """
    block_size = block_kb * 1024
    total_bytes = total_mb * 1024 * 1024
    n_blocks = total_bytes // block_size

    buf = aligned_buffer(block_size)
    fd = open_direct(path, os.O_RDONLY)
    start = time.perf_counter()
    read_bytes = 0
    for i in range(n_blocks):
        n = os.readv(fd, [buf])
        if not n:
            break
        read_bytes += n
        if show_progress:
            sys.stdout.write(f"\rRead:  {(i + 1) * 100 // n_blocks:3d}%")
            sys.stdout.flush()
    elapsed = time.perf_counter() - start
    os.close(fd)

    if show_progress:
        print()

    return read_bytes / 1024 / 1024 / elapsed, elapsed


def get_args():
    parser = argparse.ArgumentParser(
        description="Sequential disk read/write benchmark",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-f", "--file", default="/tmp/diskbench.tmp",
                        help="Path to test file (will be overwritten)")
    parser.add_argument("-s", "--size", type=int, default=1024,
                        help="Total data size to write in MB")
    parser.add_argument("-b", "--block-size", type=int, default=1024,
                        help="Block size in KB (must be multiple of 512 for O_DIRECT)")
    parser.add_argument("-j", "--json", metavar="FILE",
                        help="Write results to JSON file")
    parser.add_argument("--zero", action="store_true",
                        help="Fill write buffer with zeros instead of random data")
    parser.add_argument("--no-cleanup", action="store_true",
                        help="Keep test file after benchmark")
    return parser.parse_args()


def main():
    args = get_args()

    if (args.block_size * 1024) % 512 != 0:
        print("Error: block size must be a multiple of 512 bytes for O_DIRECT alignment",
              file=sys.stderr)
        sys.exit(1)

    actual_size = (args.size * 1024 // args.block_size) * args.block_size // 1024
    buf_type = "zeros" if args.zero else "random"
    print(f"File:       {args.file}")
    print(f"Size:       {actual_size} MB  (block size: {args.block_size} KB, buffer: {buf_type})")
    print()

    os.sync()
    write_mbps, write_s = write_test(args.file, actual_size, args.block_size, zero=args.zero)
    os.sync()
    read_mbps, read_s   = read_test(args.file,  actual_size, args.block_size)

    print(f"\nWrite: {write_mbps:8.1f} MB/s  ({write_s:.2f} s)")
    print(f"Read:  {read_mbps:8.1f} MB/s  ({read_s:.2f} s)")

    if args.json:
        result = {
            "file": args.file,
            "size_mb": actual_size,
            "block_size_kb": args.block_size,
            "buffer": buf_type,
            "write_mb_s": round(write_mbps, 2),
            "write_elapsed_s": round(write_s, 2),
            "read_mb_s": round(read_mbps, 2),
            "read_elapsed_s": round(read_s, 2),
        }
        with open(args.json, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\nResults written to {args.json}")

    if not args.no_cleanup:
        os.remove(args.file)


if __name__ == "__main__":
    main()
