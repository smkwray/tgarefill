PYTHON ?= python
VENV ?= .venv
BIN := $(VENV)/bin

.PHONY: venv install sync-raw staging panel events attribution local-projections figures mvp paper test clean

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(BIN)/python -m pip install -U pip
	$(BIN)/python -m pip install -e ".[dev]"

sync-raw:
	$(BIN)/python scripts/download_all.py

staging:
	$(BIN)/python scripts/build_staging.py

panel:
	$(BIN)/python scripts/build_master_panel.py

events:
	$(BIN)/python scripts/build_event_candidates.py

attribution:
	$(BIN)/python scripts/build_attribution_baseline.py

local-projections:
	$(BIN)/python scripts/build_local_projections.py

figures:
	$(BIN)/python scripts/build_figures.py

mvp: sync-raw staging panel events attribution

paper: mvp local-projections figures

test:
	$(BIN)/python -m pytest -q

clean:
	rm -rf data/staging/*
	rm -rf data/processed/*
	rm -rf outputs/tables/*
	rm -rf outputs/figures/*
	touch data/staging/.gitkeep data/processed/.gitkeep outputs/tables/.gitkeep outputs/figures/.gitkeep
