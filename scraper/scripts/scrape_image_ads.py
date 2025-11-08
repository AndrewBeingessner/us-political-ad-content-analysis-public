#!/usr/bin/env python3
"""CLI shim for the IMAGE creative scraper.

This module preserves the legacy entrypoint while delegating to the
``gatc_scraper.image`` package for the actual implementation.  Keeping the
shim allows existing automation that imports or executes
``scripts/scrape_image_ads.py`` directly to continue working unchanged.
"""
from __future__ import annotations

import asyncio

from gatc_scraper.image import CliArgs, get_scraper_version, parse_args, run
from gatc_scraper.logging import configure_logging, logging_context, set_global_context

SCRIPT_NAME = "image"


def main() -> None:
    """Parse CLI arguments and execute the IMAGE capture pipeline."""
    configure_logging()
    set_global_context(app="gatc_scraper", pipeline=SCRIPT_NAME)
    version = get_scraper_version()
    with logging_context(script=SCRIPT_NAME, scraper_version=version):
        args: CliArgs = parse_args()
        asyncio.run(run(args))


if __name__ == "__main__":
    main()
