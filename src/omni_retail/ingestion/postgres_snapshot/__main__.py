"""Entry point: ``python -m omni_retail.ingestion.postgres_snapshot``."""

import sys

from omni_retail.ingestion.postgres_snapshot.cli import main

if __name__ == "__main__":
    sys.exit(main())
