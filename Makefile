.PHONY: bootstrap fmt lint typecheck test check hooks

PYTHON ?= python3

PYTHON_VERSION := $(shell $(PYTHON) -c 'import sys; print(".".join(map(str, sys.version_info[:2])))' 2>/dev/null)
ifeq ($(PYTHON_VERSION),)
$(error Could not determine Python version via "$(PYTHON)". Please install Python 3.10-3.12 and/or set PYTHON=python3.12)
endif
ifeq ($(filter $(PYTHON_VERSION),3.10 3.11 3.12),)
$(error Expected Python 3.10-3.12, but "$(PYTHON)" reports $(PYTHON_VERSION). Install a supported interpreter or set PYTHON=python3.12)
endif

bootstrap:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt -r requirements-dev.txt
	$(PYTHON) -m pip install -e .
	$(PYTHON) -m playwright install chromium

fmt:
	$(PYTHON) -m ruff check --fix scraper/src scraper/scripts scraper/tests
	$(PYTHON) -m black scraper/src scraper/scripts scraper/tests

lint:
	$(PYTHON) -m ruff check scraper/src scraper/scripts scraper/tests

typecheck:
	$(PYTHON) -m mypy -p gatc_scraper

test:
	$(PYTHON) -m pytest

check: lint typecheck test

hooks:
	$(PYTHON) -m pre_commit install
