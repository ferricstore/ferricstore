#!/usr/bin/env python3
"""Placeholder for local FerricStore regression baselines.

The previous runner targeted the removed standalone wire protocol and no longer
produces valid KV results. Replace this file with a native-protocol benchmark
runner before using it as a regression gate.

FerricFlow baseline workloads should continue to run from the Python SDK
repository until this repo owns a native benchmark client.
"""

from __future__ import annotations

import sys


def main() -> int:
    sys.stderr.write(
        "bench/local_regression_baseline.py is obsolete. "
        "Use or build a native-protocol benchmark runner instead.\n"
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
