# tgarefill

`tgarefill` builds a weekly balance-sheet panel around Treasury General Account (TGA) rebuilds and estimates how funding channels move around rapid rebuild episodes and Treasury bill offering-size innovations. It downloads public Treasury, Federal Reserve (Fed), Office of Financial Research (OFR), and Securities and Exchange Commission (SEC) datasets, aligns them to Wednesday-ending weeks in USD millions, and writes event tables, Jordà local projection estimates, charts, and static site data.

## Data

All inputs come from public US government sources:

- FiscalData API: Daily Treasury Statement (DTS) operating cash balance (Wednesday close TGA, primary), DTS deposits and withdrawals (for the tax-receipt surprise control), Monthly Treasury Statement (MTS), and Treasury security auctions.
- FRED (Federal Reserve Economic Data, St. Louis Fed; public CSV downloads, no API key required): Wednesday balance-sheet levels from the Fed's H.4.1 release (TGA, bank reserves, reverse repos), commercial bank deposits and Treasury and agency securities from the H.8 release on bank balance sheets (not seasonally adjusted, seasonally adjusted, and deposit breakdowns), daily take-up at the Fed's overnight reverse repurchase (ON RRP) facility, and quarterly foreign Treasury holdings.
- OFR Short-Term Funding Monitor (STFM): monthly money market fund (MMF) Treasury holdings and repo, plus weekly primary dealer Treasury repo.
- Treasury.gov: investor-class auction allotments and Treasury International Capital (TIC) files.
- SEC Form N-MFP monthly zip archives of money market fund holdings (used only by the optional `mmfalloc` extension).

The pipeline converts every series to USD millions: H.8, ON RRP, and quarterly foreign debt series from billions, and OFR series from dollars.

## Install

Requires Python 3.11 or later. From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

`Makefile` uses the active virtual environment (`VIRTUAL_ENV` or `UV_PROJECT_ENVIRONMENT`), falling back to `$(HOME)/venvs/tgarefill` or `python3`.

## Configuration

Optional environment variables (see `.env.example`):

- `TGAREFILL_USER_AGENT` — HTTP User-Agent string for data downloads (default `tgarefill/0.1 (+research)`).
- `TGAREFILL_TIMEOUT` — request timeout in seconds (default `120`).
- `LOG_LEVEL` — logging level (default `INFO`).

Required only when downloading SEC Form N-MFP files via `make mmfalloc`:

- `TGAREFILL_SEC_USER_AGENT` — contact-bearing User-Agent required by SEC servers. Not needed if running `scripts/build_mmfalloc.py --skip-download` against cached zips under `data/raw/sec_nmfp/`.
- `MMFALLOC_START_MONTH` and `MMFALLOC_END_MONTH` — optional `YYYY-MM` bounds for `build_mmfalloc.py` (default `2022-12` to `2025-11`).

## Usage

Run the core build, which downloads the data, builds the weekly panel, detects rapid rebuild episodes, and computes funding-channel changes over each rebuild window:

```bash
make mvp
```

Run the full pipeline, including Jordà local projections, bill offering-size surprise regressions, charts, and static site data:

```bash
make site
```

Run the optional SEC Form N-MFP fund-level allocation extension (after setting `TGAREFILL_SEC_USER_AGENT`):

```bash
make mmfalloc
```

Individual pipeline stages are exposed as `Makefile` targets: `sync-raw`, `staging`, `panel`, `events`, `attribution`, `local-projections`, `auction-shock-lp`, `figures`, `site-data`, and `clean`.

## Method

- Rapid rebuild events flag weeks where the weekly change in the TGA, or its 4-week or 8-week cumulative change, exceeds the 90th percentile of positive changes, then merge flagged weeks separated by at most 9 days.
- Bill offering-size innovations subtract, for each non-cash-management bill auction, the trailing median of the previous 8 auctions (minimum 4) in the same term and reopening group, summed to Wednesday-ending weeks. A term-only grouping is computed alongside for comparison.
- Jordà (2005) local projections run at horizons from -4 to 12 weeks (excluding 0) with 2 shock lags, 1 response lag, 11 month dummies, and Newey-West standard errors with bandwidth `max(h, 1)` at horizon `h`. The bill-surprise specification adds a DTS tax-receipt surprise control. Its main sample ends November 30, 2025, before the Federal Reserve began reserve management purchases of Treasury bills in December 2025; a sample through the latest week and an announcement-week timing check run alongside it.
- Regime splits classify weeks with ON RRP balances of $100 billion or more as abundant and the rest as scarce; pre-facility weeks with missing ON RRP are excluded from regime regressions.

Episode rules are set in `configs/episode_rules.yaml` and local projection settings in `configs/analysis.yaml`.

## Output

Raw downloads (`data/raw/`), staged tables (`data/staging/`), processed panels (`data/processed/`), and analysis outputs (`outputs/`) are gitignored. Dollar series are in USD millions.

| Path | Description |
| --- | --- |
| `data/processed/master_weekly_panel.parquet` | Weekly Wednesday-ending balance-sheet panel (USD millions) |
| `data/processed/master_monthly_panel.parquet` | Monthly balance-sheet panel (USD millions) |
| `data/processed/event_candidates.parquet` | Candidate Treasury-cash rebuild events |
| `outputs/tables/attribution_baseline.csv` | Funding-channel changes over each rebuild window |
| `outputs/tables/` | Other rebuild, local projection and bill offering-size surprise tables |
| `outputs/figures/` | PNG charts of TGA rebuilds, funding channels, and local projection impulse responses |
| `site/` | Static site JSON data and figures updated by `make site` |

`make mmfalloc` writes monthly fund-level N-MFP tables under `data/processed/mmfalloc/` and summary CSVs under `outputs/tables/`.

## Limits

- Estimates are conditional associations around bill offering-size deviations and rebuild episodes; they do not identify structural causal effects. Bill-size innovations come from trailing auction medians and show serial persistence across weeks.
- Monthly OFR MMF Treasury holdings are forward-filled into the weekly panel via backward as-of merge, so weekly MMF changes reflect monthly observations carried forward.
- Proxy funding channels (reserves, bank deposits, ON RRP, bank Treasury and agency holdings, MMF Treasury holdings, and dealer repo) overlap across balance sheets, so coefficient ratios cannot be read as mutually exclusive funding shares.
- Primary dealer Treasury repo ends before the most recent sample years, and H.8 series combine Treasury and agency securities.

## Tests

```bash
python -B -m pytest -q
ruff check src tests scripts
```

## License

MIT. See [LICENSE](LICENSE).
