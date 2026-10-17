# Who Funds TGA Rebuilds?

**[Live Site](https://smkwray.github.io/tgarefill/)** | Empirical decomposition of Treasury General Account rebuild funding channels using bill offering-size innovations.

When the US Treasury rebuilds its cash balance by issuing securities, where does the money come from? This project identifies rapid rebuild episodes and studies aggregate funding associations around bill offering-size innovations.

## Central Finding

Larger bill offering-size innovations are associated with increases in aggregate money-market fund (MMF) Treasury holdings and declines in overnight reverse repurchase agreement (ON RRP) balances. The channels overlap: their coefficient ratio is not a funding share, and statistically insignificant bank outcomes do not establish an absence of bank involvement.

The canonical pre-December-2025 specification estimates an h=4 MMF association of +$44.1B (t=3.82) and an ON RRP association of -$30.4B (t=-4.99), per one-standard-deviation bill-size innovation. Monthly MMF observations are carried into a weekly panel; this does not create independently observed weekly MMF flows. These are conditional associations, without household, final-owner, bank-deposit pass-through, or unrestricted causal identification.

Fewer significant placebo coefficients in the bill-surprise comparison are a diagnostic, not proof of exogeneity. SEC N-MFP data provide a separate descriptive view of MMF allocation changes around selected positive bill-surprise months during December 2022–November 2025.

## Method

### Data Pipeline

| Source | Series | Frequency | Role |
|--------|--------|-----------|------|
| FiscalData DTS | Operating cash balance | Daily | TGA Wednesday close (primary) |
| FiscalData Auctions | Bill/coupon results | Per auction | Bill-size innovation |
| FRED H.4.1 | WTREGEN, WRBWFRBL, WLRRAL | Weekly | TGA (reference), reserves, reverse repos |
| FRED H.8 | DPSACBW027NBOG, TASACBW027NBOG | Weekly | Bank deposits, Treasury holdings |
| FRED | RRPONTSYD | Daily | ON RRP facility usage |
| OFR STFM | MMF-MMF_T_TOT-M | Monthly | MMF Treasury holdings |
| OFR STFM | NYPD-PD_RP_T_TOT-A | Weekly | Primary dealer Treasury repo (ended 2021) |
| SEC DERA N-MFP | Monthly flat-file zips | Monthly | Descriptive fund-level MMF allocation |
| FRED | FDHBFIN | Quarterly | Foreign Treasury holdings (reference only) |

All values normalized to millions of USD. H.8 series converted from billions. OFR series from raw USD.

FiscalData downloads retain normalized paginated JSON payloads in a compressed SQLite
snapshot, not original HTTP response bytes. Each fresh snapshot and its manifest share
a run ID and are selected together through an atomic current-snapshot pointer.
Staging accepts only payload hashes marked downloaded in that manifest; failed optional
endpoints are absent from the new snapshot.

### Event Detection

Weeks are flagged as rapid rebuilds when the weekly ΔTGA or rolling 4w/8w cumulative change exceeds the 90th percentile **of positive changes only**. Contiguous flagged weeks (max 9-day gap) are grouped into events. The positive-only threshold is critical — computing quantiles on the full distribution inflates the event count by ~50%.

### Bill Offering-Size Innovation

For each non-CMB bill auction:
```
surprise = offering_amount − trailing_median(last 8 same-term × reopening-status auctions, min 4)
```

Aggregated to weekly (Wednesday-ending). This is a deviation from a trailing same-term/reopening median; it is not a directly observed market-expectations surprise or an instrument establishing exogeneity. A tax-receipt surprise control (from DTS deposit categories) absorbs non-issuance TGA movements. A same-term-only grouping is tracked separately as a robustness comparison.

### Local Projections

Jordà (2005) specification:
```
Δy_{t+h} = α + β(h) · shock_t + γ · controls_t + ε_{t+h}
```

Baseline (binary shock): 2 shock lags, 1 response lag, 11 month dummies. Bill-surprise specification adds a tax-receipt surprise control (from DTS deposit categories) to absorb non-issuance TGA movements. Newey-West HAC standard errors (bandwidth = horizon). Placebo tests at h=-4 to h=-1.

### Regime Classification

ON RRP abundant (≥$100B) vs. scarce (<$100B). Pre-facility weeks (before 2013, ON RRP = NaN) are excluded from regime estimation, not lumped into "scarce."

## Descriptive MMF Allocation Extension

The `mmfalloc` extension parses SEC N-MFP monthly files and compares observed fund allocations around selected months. Requested source months must match observed coverage; events are ranked only within that observed window. Failed source gates stop the build before replacing any outputs.

This separate descriptive extension neither validates the canonical estimator nor independently establishes its mechanism. Exact extension values and its figure are withheld from this release pending a separate receipt binding inputs, selected events, gates and outputs. The aggregate estimator’s evidence receipt does not cover this extension.

## Caveats

1. **Issuance-specific.** The regressor measures bill offering-size deviations, not all TGA changes.
2. **Placebos.** A remaining h=-1 ON RRP association (t=-2.1) limits a clean timing interpretation.
3. **Shock persistence.** Bill-size innovations are persistent; the horizon coefficients need not describe a single isolated financing event.
4. **NSA baseline.** Month dummies control for seasonality. Seasonally adjusted bank series are a sensitivity comparison, not an identification test.
5. **Accounting overlap.** Proxy channels are not mutually exclusive.

## Quick Start

Activate the environment you want `make` to use, install the package, then run the canonical MVP pipeline. `make` will use the active shell environment, or `VIRTUAL_ENV` / `UV_PROJECT_ENVIRONMENT` if those are set.

```bash
# Python 3.11 or later
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
python -B -m pytest -q
ruff check .

# Acquire data and build the weekly accounting panel
make mvp

# Extended site build
make site

# Descriptive MMF allocation extension
make mmfalloc
```

Raw inputs under `data/raw/`, generated panels under `data/processed/`, and
analysis under `outputs/` are local and ignored by Git. The tracked `site/` assets
are a curated exploratory presentation with their own sample and normalization;
they are not the canonical pre-December-2025 result.

SEC downloads require `TGAREFILL_SEC_USER_AGENT` with your own contact information.
Cached N-MFP parsing with `--skip-download` does not require a network identity.

## Outputs

`make mvp` creates the weekly accounting artifacts:

```
data/processed/master_weekly_panel.parquet
data/processed/event_candidates.parquet
outputs/tables/attribution_baseline.csv
```

The extended site build extends that with additional tables, figures, and the repo-root `site/` bundle used for GitHub Pages:

```
outputs/
├── figures/
│   ├── tga_timeline_events.png        # TGA with detected events
│   ├── attribution_stacked_top20.png   # Top 20 event decomposition
│   ├── era_dominant_source.png         # Funding channel evolution
│   ├── onrrp_era.png                   # TGA + ON RRP dual panel
│   ├── event_size_over_time.png        # Event scatter
│   ├── irf_pooled.png                  # Binary shock IRFs
│   ├── irf_by_regime.png              # Regime-split IRFs
│   ├── irf_continuous_regime.png      # Continuous shock regime IRFs
├── tables/
│   ├── event_candidates.csv
│   ├── attribution_baseline.csv
│   ├── local_projections.csv
│   ├── auction_shock_lp.csv
│   ├── auction_shock_grouping_comparison.csv
│   └── mmfalloc_downstream_summary.csv
site/
├── data/                              # GitHub Pages JSON
├── img/                               # GitHub Pages figures
└── index.html                         # GitHub Pages entrypoint
```

## Data Sources

All data from free, official US government sources:
- [FiscalData API](https://fiscaldata.treasury.gov/api-documentation/) — DTS, MTS, Auctions
- [FRED](https://fred.stlouisfed.org/) — H.4.1, H.8, ON RRP
- [OFR STFM](https://www.financialresearch.gov/short-term-funding-monitor/) — MMF, Primary Dealer, Repo
- [SEC DERA Form N-MFP Data Sets](https://www.sec.gov/data-research/sec-markets-data/dera-form-n-mfp-data-sets) — fund-level MMF holdings
- [Treasury.gov](https://home.treasury.gov/) — TIC, Investor-Class, Refunding

## License

MIT
