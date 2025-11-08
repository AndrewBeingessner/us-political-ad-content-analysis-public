#!/usr/bin/env python3
"""CLI shim for the TEXT creative scraper."""
from __future__ import annotations

import asyncio

from gatc_scraper.logging import configure_logging, logging_context, set_global_context
from gatc_scraper.text import CliArgs, get_scraper_version, parse_args, run

SCRIPT_NAME = "text"


def main() -> None:
    configure_logging()
    set_global_context(app="gatc_scraper", pipeline=SCRIPT_NAME)
    version = get_scraper_version()
    with logging_context(script=SCRIPT_NAME, scraper_version=version):
        args: CliArgs = parse_args()
        asyncio.run(run(args))


if __name__ == "__main__":
    main()
