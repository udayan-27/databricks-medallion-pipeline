"""
Bronze ingest: orders.csv -> bronze.orders.

Raw ingest only. Source values are not cleaned, filled, deduplicated, or repaired.
Shared implementation: ingest_core.ingest_orders.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ingest_core import cli_main, ingest_orders

__all__ = ["ingest_orders"]


if __name__ == "__main__":
    cli_main("orders")
