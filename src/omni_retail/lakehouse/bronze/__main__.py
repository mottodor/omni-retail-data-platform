"""``python -m omni_retail.lakehouse.bronze`` entry point."""

import sys

from omni_retail.lakehouse.bronze.cli import main

if __name__ == "__main__":
    sys.exit(main())
