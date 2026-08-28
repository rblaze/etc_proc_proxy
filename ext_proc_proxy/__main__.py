"""Main entry point for running the proxy CLI."""

import asyncio
import sys

from ext_proc_proxy.config import parse_args
from ext_proc_proxy.proxy import run_proxy


def main():
    """Main entry point function."""
    config = parse_args()
    try:
        asyncio.run(run_proxy(config))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()

