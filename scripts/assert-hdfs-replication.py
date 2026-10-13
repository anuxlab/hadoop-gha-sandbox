#!/usr/bin/env python3
"""Assert live HDFS replica count and distinct DataNode locations from `hdfs fsck`."""
from __future__ import annotations

import re
import sys


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} EXPECTED_REPLICAS < fsck-output.txt", file=sys.stderr)
        return 2
    try:
        expected = int(sys.argv[1])
    except ValueError:
        print("EXPECTED_REPLICAS must be an integer", file=sys.stderr)
        return 2
    if expected < 1:
        print("EXPECTED_REPLICAS must be >= 1", file=sys.stderr)
        return 2

    block_lines = [line.rstrip() for line in sys.stdin if "Live_repl=" in line]
    if not block_lines:
        print("FAIL: fsck output contained no Live_repl block records", file=sys.stderr)
        return 1

    failed = False
    for index, line in enumerate(block_lines, start=1):
        match = re.search(r"Live_repl=(\d+)", line)
        if not match:
            print(f"FAIL: could not parse live replica count from block line {index}: {line}", file=sys.stderr)
            failed = True
            continue
        live_replicas = int(match.group(1))
        endpoints = re.findall(r"DatanodeInfoWithStorage\[([^,\]]+)", line)
        unique_endpoints = sorted(set(endpoints))
        print(
            f"[HDFS] block {index}: live_replicas={live_replicas}, "
            f"distinct_DataNodes={len(unique_endpoints)}, expected={expected}"
        )
        if live_replicas != expected:
            print(
                f"FAIL: block {index} has {live_replicas} live replicas; expected exactly {expected}",
                file=sys.stderr,
            )
            failed = True
        if len(unique_endpoints) != expected:
            print(
                f"FAIL: block {index} has {len(unique_endpoints)} distinct DataNode endpoints; "
                f"expected exactly {expected}: {unique_endpoints}",
                file=sys.stderr,
            )
            failed = True

    if failed:
        return 1
    print(f"[PASS] All {len(block_lines)} block(s) have exactly {expected} live replicas on distinct DataNodes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
