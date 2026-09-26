"""Entry point: ``python -m omni_retail.serving.clickhouse``."""

import sys

from omni_retail.serving.clickhouse.cli import main

if __name__ == "__main__":
    sys.exit(main())
