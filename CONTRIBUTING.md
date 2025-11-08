# Contributing guide

Thank you for improving the GATC scraper! This document collects the conventions and local workflows we rely on to keep the project stable.

## Environment setup
1. Install Python 3.12.
2. Clone the repository and install dependencies:
   ```bash
   make bootstrap
   ```
   This installs runtime + dev packages and downloads the Chromium browser for Playwright.
3. Ensure the following environment variables are available when exercising the scrapers manually:
   - `DB_PASSWORD` (required)
   - Optional: `DB_NAME`, `DB_USER`, `DB_SSLMODE`, `AD_SCRAPER_VERSION`

## Development loop
- Run unit tests and static checks before pushing:
  ```bash
  make lint
  make typecheck
  make test
  ```
- Use `make fmt` to apply Ruff autofixes and Black formatting. CI will enforce the same formatting rules.
- Tests live under `tests/` and must remain network-free. Favor fast unit tests that cover invariants such as hashing, metadata contracts, and URL parsing.
- Avoid changing CLI flag names or removing log fields; preserve the operational contract documented in `README.md`.

## Playwright tips
- `--trace` and `--debug-*` CLI flags write artifacts under `media/debug/`, which is gitignored.
- When running on CI or ephemeral environments, rely on the compatibility shims (`scripts/scraper_common.py`, `scripts/scraper_db.py`) to keep existing automation working.

## Commit & PR guidelines
- Group logically-related changes together and include high-level context in commit messages.
- Update documentation whenever behavior or operational guidance changes.
- CI (GitHub Actions) runs Ruff, mypy, and pytest on every PR. Please ensure these commands succeed locally before requesting review.
