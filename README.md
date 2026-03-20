# Who Funds TGA Rebuilds?

**[Live Site](https://smkwray.github.io/tgarefill/)** | Empirical decomposition of Treasury General Account rebuild funding channels using auction-schedule surprise identification.

When the U.S. Treasury rebuilds its cash balance by issuing securities, where does the money come from? This project identifies 51 rapid rebuild episodes (2005-2026) and uses a bill-size surprise instrument to trace the causal funding channels.

## Central Finding

Unexpected bill issuance that rebuilds the TGA is primarily funded by **money market funds** (+$41B per 1-std shock, t=3.4), with ~75% of that absorption sourced from **ON RRP runoff** (-$31B, t=-5.0). Bank deposits, reserves, and bank Treasury holdings show no significant causal response once the predictable component of auction supply is removed.

The bill-size surprise eliminates most pre-trends (12 significant placebo coefficients → 1), confirming that the binary event indicator used in prior analysis was contaminated by predictable auction-schedule repositioning.

## Method

### Data Pipeline

| Source | Series | Frequency | Role |
|--------|--------|-----------|------|
| FiscalData DTS | Operating cash balance | Daily | TGA Wednesday close (primary) |
| FiscalData Auctions | Bill/coupon results | Per auction | Bill-size surprise instrument |
| FRED H.4.1 | WTREGEN, WRBWFRBL, WLRRAL | Weekly | TGA (reference), reserves, reverse repos |
| FRED H.8 | DPSACBW027NBOG, TASACBW027NBOG | Weekly | Bank deposits, Treasury holdings |
| FRED | RRPONTSYD | Daily | ON RRP facility usage |
| OFR STFM | MMF-MMF_T_TOT-M | Monthly | MMF Treasury holdings |
| OFR STFM | NYPD-PD_RP_T_TOT-A | Weekly | Primary dealer Treasury repo (ended 2021) |
| FRED | FDHBFIN | Quarterly | Foreign Treasury holdings (reference only) |

All values normalized to millions of USD. H.8 series converted from billions. OFR series from raw USD.

### Event Detection

Weeks are flagged as rapid rebuilds when the weekly ΔTGA or rolling 4w/8w cumulative change exceeds the 90th percentile **of positive changes only**. Contiguous flagged weeks (max 9-day gap) are grouped into events. The positive-only threshold is critical — computing quantiles on the full distribution inflates the event count by ~50%.

### Bill-Size Surprise Instrument

For each non-CMB bill auction:
```
surprise = offering_amount − trailing_median(last 8 same-term × reopening-status auctions, min 4)
```

Aggregated to weekly (Wednesday-ending). This strips ~94% of weekly auction supply that was already announced by the prior Wednesday, isolating unexpected variation in Treasury financing. A tax-receipt surprise control (from DTS deposit categories) absorbs non-issuance TGA movements. A same-term-only grouping is tracked separately as a robustness comparison.

### Local Projections

Jordà (2005) specification:
```
Δy_{t+h} = α + β(h) · shock_t + γ · controls_t + ε_{t+h}
```

Baseline (binary shock): 2 shock lags, 1 response lag, 11 month dummies. Bill-surprise specification adds a tax-receipt surprise control (from DTS deposit categories) to absorb non-issuance TGA movements. Newey-West HAC standard errors (bandwidth = horizon). Placebo tests at h=-4 to h=-1.

### Regime Classification

ON RRP abundant (≥$100B) vs. scarce (<$100B). Pre-facility weeks (before 2013, ON RRP = NaN) are excluded from regime estimation, not lumped into "scarce."

## Key Results

| Channel | Binary Pre-trends | Bill-Surprise Pre-trends | Bill-Surprise h=4 (1-std) |
|---------|:-:|:-:|:-:|
| Reserves | 2/4 | 0/4 | -$2B, t=-0.1 |
| Bank Deposits | 2/4 | 0/4 | +$8B, t=0.7 |
| ON RRP | 3/4 | 1/4 | **-$31B, t=-5.0*** |
| Bank T&A | 1/4 | 0/4 | +$4B, t=1.5 |
| MMF Treasury | 2/4 | 0/4 | **+$41B, t=3.4*** |
| Dealer Repo | 2/4 | 0/4 | -$1B, t=-0.2 |

\* Significant at 5% with Newey-West HAC standard errors.

## Caveats

1. **Issuance-specific.** The shock identifies unexpected bill supply, not all TGA changes.
2. **h=-1 ON RRP.** The one remaining pre-trend (t=-2.1) likely reflects same-week announcement effects.
3. **Shock persistence.** Bill surprise autocorrelation = 0.86. Interpret h=4-8 as primary; h>8 is contaminated.
4. **NSA baseline.** Month dummies control for seasonality. SA sensitivity confirms: swapping NSA for SA deposits and bank T&A leaves all results unchanged.
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
│   └── irf_continuous_regime.png      # Continuous shock regime IRFs
├── tables/
│   ├── event_candidates.csv
│   ├── attribution_baseline.csv
│   ├── local_projections.csv
│   ├── auction_shock_lp.csv
│   └── auction_shock_grouping_comparison.csv
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
- [Treasury.gov](https://home.treasury.gov/) — TIC, Investor-Class, Refunding

## License

MIT
