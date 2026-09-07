# Who Funds TGA Rebuilds?

**[Live Site](https://smkwray.github.io/tgarefill/)** | Empirical decomposition of Treasury General Account rebuild funding channels using bill offering-size innovations.

When the U.S. Treasury rebuilds its cash balance by issuing securities, where does the money come from? This project identifies 51 rapid rebuild episodes (2005-2026) and studies aggregate funding associations around bill offering-size innovations.

## Central Finding

Larger bill offering-size innovations are associated with increases in aggregate MMF Treasury holdings and declines in ON RRP balances. The channels overlap: their coefficient ratio is not a funding share, and statistically insignificant bank outcomes do not establish an absence of bank involvement.

The numerical comparison below and the interactive site retain an exploratory specification. The separately retained canonical pre-December-2025 specification estimates an h=4 MMF association of +$44.1B (t=3.82) and an ON RRP association of -$30.4B (t=-4.99), per one-standard-deviation bill-size innovation. Monthly MMF observations are carried into a weekly panel; this does not create independently observed weekly MMF flows. These are conditional associations, without household, final-owner, bank-deposit pass-through, or unrestricted causal identification.

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
| SEC DERA N-MFP | Monthly flat-file zips | Monthly | Fund-level MMF allocation validation |
| FRED | FDHBFIN | Quarterly | Foreign Treasury holdings (reference only) |

All values normalized to millions of USD. H.8 series converted from billions. OFR series from raw USD.

Raw FiscalData responses are retained as exact JSON bytes inside one
Zstandard-compressed `data/raw/fiscaldata/fiscaldata.sqlite` store. The adjacent
small manifest remains human-readable, and staging reads the compact store
directly into the same per-endpoint Parquet tables.

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

## Exploratory Comparison

| Channel | Binary Pre-trends | Bill-Surprise Pre-trends | Bill-Surprise h=4 (1-std) |
|---------|:-:|:-:|:-:|
| Reserves | 2/4 | 0/4 | -$2B, t=-0.1 |
| Bank Deposits | 2/4 | 0/4 | +$8B, t=0.7 |
| ON RRP | 3/4 | 1/4 | **-$31B, t=-5.0*** |
| Bank T&A | 1/4 | 0/4 | +$4B, t=1.5 |
| MMF Treasury | 2/4 | 0/4 | **+$41B, t=3.4*** |
| Dealer Repo | 2/4 | 0/4 | -$1B, t=-0.2 |

\* Significant at 5% with Newey-West HAC standard errors.

## Fund-Level MMF Validation

The `mmfalloc` extension parses official SEC DERA Form N-MFP monthly flat-file zips without extracting TSVs into the repository. It builds fund/security positions, a fund-month panel, category aggregates, source gates, benchmark reconciliation, event-window deltas, placebos, leave-one-family-out checks, and a compact downstream summary.

Source gates pass on the canonical validation window:

- Sample: 2022-12-31 through 2025-11-30
- Coverage: 36 contiguous monthly observations
- Mapping: minimum mapped portfolio-security value share = 1.000
- Reconciliation: median absolute pct diff = 0.0421 against OFR/Fed benchmark checks

Mean event-month allocation deltas across selected positive bill-surprise months:

- Treasury bills + coupons: +$229B
- Fed ON RRP: -$141B
- Repo ex-Fed: -$11B

Interpretation: aggregate changes across observed MMF portfolios show rotation toward Treasuries/T-bills and away from Fed ON RRP/repo around the selected bill-surprise months. Do not use this table to infer household behavior, final ownership, bank-deposit pass-through, or broad bill-deposit substitution.

## Caveats

1. **Issuance-specific.** The regressor measures bill offering-size deviations, not all TGA changes.
2. **Placebos.** A remaining h=-1 ON RRP association (t=-2.1) limits a clean timing interpretation.
3. **Shock persistence.** Bill-size innovations are persistent; the horizon coefficients need not describe a single isolated financing event.
4. **NSA baseline.** Month dummies control for seasonality. Seasonally adjusted bank series are a sensitivity comparison, not an identification test.
5. **Accounting overlap.** Proxy channels are not mutually exclusive.

## Quick Start

Activate the environment you want `make` to use, install the package, then run the canonical MVP pipeline. `make` will use the active shell environment, or `VIRTUAL_ENV` / `UV_PROJECT_ENVIRONMENT` if those are set.

```bash
# Example with uv
uv venv ~/venvs/tgarefill --python 3.11
source ~/venvs/tgarefill/bin/activate
uv pip install -e ".[dev]"

# Canonical MVP path
make mvp

# Extended site build
make site

# Fund-level MMF validation
make mmfalloc
```

## Outputs

The MVP command creates the canonical artifacts:

```
data/processed/master_weekly_panel.parquet
data/processed/event_candidates.parquet
outputs/tables/attribution_baseline.csv
```

The extended site build extends that with additional tables, figures, and the repo-root `site/` bundle used for GitHub Pages:

```
outputs/
├── figures/
│   ├── tga_timeline_events.png        # TGA with 51 events highlighted
│   ├── irf_binary_vs_bill_surprise.png # Central comparison figure
│   ├── attribution_stacked_top20.png   # Top 20 event decomposition
│   ├── era_dominant_source.png         # Funding channel evolution
│   ├── onrrp_era.png                   # TGA + ON RRP dual panel
│   ├── event_size_over_time.png        # Event scatter
│   ├── irf_pooled.png                  # Binary shock IRFs
│   ├── irf_by_regime.png              # Regime-split IRFs
│   ├── irf_continuous_regime.png      # Continuous shock regime IRFs
│   └── mmfalloc_eventstudy.svg        # Fund-level MMF validation inset
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

All data from free, official U.S. government sources:
- [FiscalData API](https://fiscaldata.treasury.gov/api-documentation/) — DTS, MTS, Auctions
- [FRED](https://fred.stlouisfed.org/) — H.4.1, H.8, ON RRP
- [OFR STFM](https://www.financialresearch.gov/short-term-funding-monitor/) — MMF, Primary Dealer, Repo
- [SEC DERA Form N-MFP Data Sets](https://www.sec.gov/data-research/sec-markets-data/dera-form-n-mfp-data-sets) — fund-level MMF holdings
- [Treasury.gov](https://home.treasury.gov/) — TIC, Investor-Class, Refunding

## License

MIT
