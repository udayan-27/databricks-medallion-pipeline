"""
Bronze orchestrator: ingest customers, orders, and products.

Calls ingest_core.ingest_all so the three datasets share one ingest_id,
one preflight, and one implementation of the read/write/metadata path.

Rerun: entity tables are overwritten from the current CSVs; ingest_metadata
is append-only. Do not run overlapping jobs. Not a streaming pipeline.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ingest_core import cli_main, ingest_all

__all__ = ["ingest_all"]


if __name__ == "__main__":
    cli_main("all")
