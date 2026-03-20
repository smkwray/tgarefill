PYTHON ?= $(shell \
	if [ -n "$$VIRTUAL_ENV" ] && [ -x "$$VIRTUAL_ENV/bin/python" ]; then \
		printf '%s\n' "$$VIRTUAL_ENV/bin/python"; \
	elif [ -n "$$UV_PROJECT_ENVIRONMENT" ] && [ -x "$$UV_PROJECT_ENVIRONMENT/bin/python" ]; then \
		printf '%s\n' "$$UV_PROJECT_ENVIRONMENT/bin/python"; \
	else \
		command -v python3 || command -v python; \
	fi)

.PHONY: venv install sync-raw staging panel events attribution local-projections auction-shock-lp figures site-data mvp site paper test clean

venv:
	$(PYTHON) -m venv .venv

install:
	$(PYTHON) -m pip install -U pip
	$(PYTHON) -m pip install -e ".[dev]"

sync-raw:
	$(PYTHON) scripts/download_all.py

staging:
	$(PYTHON) scripts/build_staging.py

panel:
	$(PYTHON) scripts/build_master_panel.py

events:
	$(PYTHON) scripts/build_event_candidates.py

attribution:
	$(PYTHON) scripts/build_attribution_baseline.py

local-projections:
	$(PYTHON) scripts/build_local_projections.py

auction-shock-lp:
	$(PYTHON) scripts/build_auction_shock_lp.py

figures:
	$(PYTHON) scripts/build_figures.py

site-data:
	$(PYTHON) scripts/build_site_data.py

mvp: sync-raw staging panel events attribution

site: mvp local-projections auction-shock-lp figures site-data

paper: site

test:
	$(PYTHON) -B -m pytest -q

clean:
	rm -rf data/staging/*
	rm -rf data/processed/*
	rm -rf outputs/tables/*
	rm -rf outputs/figures/*
	touch data/staging/.gitkeep data/processed/.gitkeep outputs/tables/.gitkeep outputs/figures/.gitkeep
