"""Jordà-style local projections for TGA rebuild impulse responses.

For each horizon h = 0, 1, ..., H weeks:
    Δy_{t+h} = α + β * shock_t + γ * controls_t + ε_{t+h}

shock_t = rapid_rebuild_flag (binary) or delta_tga (continuous)
y = each proxy channel (reserves, deposits, ON RRP, etc.)
β(h) traces out the impulse response of y to a TGA rebuild shock.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import statsmodels.api as sm


@dataclass
class LPResult:
    """Result of a single local projection regression."""

    response_var: str
    horizon: int
    beta: float
    se: float
    se_nw: float
    t_stat: float
    t_stat_nw: float
    n_obs: int
    r_squared: float
    regime: str | None = None


def _ols_hac(y: np.ndarray, X: np.ndarray, horizon: int) -> tuple[np.ndarray, np.ndarray, np.ndarray, float]:
    """OLS with both standard and Newey-West HAC standard errors.

    Returns (coefficients, ols_se, nw_se, r_squared).
    Newey-West bandwidth = max(horizon, 1) following Jordà (2005).
    """
    model = sm.OLS(y, X)
    res = model.fit()
    beta = res.params
    se_ols = res.bse
    r2 = res.rsquared

    nw_lags = max(horizon, 1)
    try:
        res_nw = res.get_robustcov_results(cov_type="HAC", maxlags=nw_lags)
        se_nw = res_nw.bse
    except Exception:
        se_nw = se_ols

    return beta, se_ols, se_nw, r2


def _horizon_change(series: pd.Series, horizon: int) -> pd.Series:
    if horizon > 0:
        return series.shift(-horizon) - series
    if horizon < 0:
        return series - series.shift(abs(horizon))
    return pd.Series(np.nan, index=series.index)


def estimate_local_projections(
    panel: pd.DataFrame,
    shock_col: str,
    response_cols: list[str],
    min_horizon: int = 1,
    max_horizon: int = 12,
    control_cols: list[str] | None = None,
    lags: int = 2,
    response_lags: int = 0,
    add_month_dummies: bool = False,
    regime_col: str | None = None,
) -> list[LPResult]:
    """Estimate Jordà local projections.

    Parameters
    ----------
    panel : DataFrame with 'date' column sorted chronologically.
    shock_col : Column name for the shock variable (e.g. 'rapid_rebuild_flag').
    response_cols : Columns to project onto the shock.
    max_horizon : Maximum weeks ahead to project.
    control_cols : Additional control variables.
    lags : Number of lags of the shock to include as controls.
    regime_col : If provided, estimate separate coefficients per regime.

    Returns
    -------
    List of LPResult, one per (response_var, horizon, regime).
    """
    df = panel.sort_values("date").reset_index(drop=True).copy()

    # Require the shock column to be non-null (drops pre-TGA junk rows)
    df = df.dropna(subset=[shock_col]).reset_index(drop=True)

    control_cols = control_cols or []
    results: list[LPResult] = []

    # Build lag columns on the FULL contiguous panel BEFORE any regime subsetting.
    # This ensures shift(1) always refers to the previous calendar week, not the
    # previous row within a non-contiguous regime subset.
    for lag in range(1, lags + 1):
        df[f"_shock_lag{lag}"] = df[shock_col].shift(lag)

    month_dummy_cols: list[str] = []
    if add_month_dummies:
        month_dummies = pd.get_dummies(df["date"].dt.month, prefix="_month", drop_first=True, dtype=float)
        df = pd.concat([df, month_dummies], axis=1)
        month_dummy_cols = list(month_dummies.columns)

    regimes = [None]
    if regime_col and regime_col in df.columns:
        regimes = sorted(df[regime_col].dropna().unique())

    for response_var in response_cols:
        if response_var not in df.columns:
            continue

        for lag in range(1, response_lags + 1):
            df[f"_response_lag{lag}_{response_var}"] = df[response_var].shift(lag)

        horizons = [h for h in range(min_horizon, max_horizon + 1) if h != 0]
        for h in horizons:
            df[f"_dy_h{h}"] = _horizon_change(df[response_var], h)

            for regime in regimes:
                if regime is not None:
                    mask = df[regime_col] == regime
                    sub = df[mask].dropna(subset=[f"_dy_h{h}", shock_col]).copy()
                else:
                    sub = df.dropna(subset=[f"_dy_h{h}", shock_col]).copy()

                if len(sub) < 20:
                    continue

                # Build X matrix: constant + shock + pre-built lags + controls
                X_parts = [np.ones(len(sub))]  # constant
                X_parts.append(sub[shock_col].values)  # shock

                for lag in range(1, lags + 1):
                    X_parts.append(sub[f"_shock_lag{lag}"].fillna(0).values)

                for lag in range(1, response_lags + 1):
                    X_parts.append(sub[f"_response_lag{lag}_{response_var}"].fillna(0).values)

                for ctrl in control_cols:
                    if ctrl in sub.columns:
                        X_parts.append(sub[ctrl].fillna(0).values)

                for ctrl in month_dummy_cols:
                    X_parts.append(sub[ctrl].values)

                X = np.column_stack(X_parts)
                y = sub[f"_dy_h{h}"].values

                # Drop rows with NaN/Inf
                valid = np.isfinite(y) & np.all(np.isfinite(X), axis=1)
                if valid.sum() < 20:
                    continue

                X, y = X[valid], y[valid]

                try:
                    beta, se_ols, se_nw, r2 = _ols_hac(y, X, horizon=h)
                except Exception:
                    continue

                # beta[1] is the shock coefficient
                b = float(beta[1])
                s_ols = float(se_ols[1])
                s_nw = float(se_nw[1])
                results.append(LPResult(
                    response_var=response_var,
                    horizon=h,
                    beta=b,
                    se=s_ols,
                    se_nw=s_nw,
                    t_stat=b / s_ols if s_ols > 0 else 0.0,
                    t_stat_nw=b / s_nw if s_nw > 0 else 0.0,
                    n_obs=int(valid.sum()),
                    r_squared=float(r2),
                    regime=str(regime) if regime is not None else None,
                ))

            df.drop(columns=[f"_dy_h{h}"], inplace=True)

        if response_lags:
            df.drop(
                columns=[f"_response_lag{lag}_{response_var}" for lag in range(1, response_lags + 1)],
                inplace=True,
            )

    return sorted(results, key=lambda r: (r.response_var, r.regime or "", r.horizon))


def results_to_dataframe(results: list[LPResult]) -> pd.DataFrame:
    """Convert LP results to a DataFrame."""
    rows = []
    for r in results:
        rows.append({
            "response_var": r.response_var,
            "horizon": r.horizon,
            "beta": r.beta,
            "se": r.se,
            "se_nw": r.se_nw,
            "t_stat": r.t_stat,
            "t_stat_nw": r.t_stat_nw,
            "ci_lower": r.beta - 1.96 * r.se_nw,
            "ci_upper": r.beta + 1.96 * r.se_nw,
            "n_obs": r.n_obs,
            "r_squared": r.r_squared,
            "regime": r.regime,
            "significant_5pct": abs(r.t_stat_nw) >= 1.96,
        })
    return pd.DataFrame(rows)


def classify_regime(
    panel: pd.DataFrame,
    on_rrp_col: str = "on_rrp_daily_total",
    threshold_mn: float = 100_000,
) -> pd.Series:
    """Classify weeks into ON RRP regimes.

    Parameters
    ----------
    threshold_mn : ON RRP level in millions above which = 'abundant'.
                   Default 100,000 million = $100B.

    Pre-facility weeks (ON RRP is NaN) are set to NaN (excluded from
    regime-split estimation) rather than lumped into 'scarce'.
    """
    vals = panel[on_rrp_col]
    result = pd.Series(np.nan, index=panel.index, dtype=object)
    has_data = vals.notna()
    result[has_data & (vals >= threshold_mn)] = "on_rrp_abundant"
    result[has_data & (vals < threshold_mn)] = "on_rrp_scarce"
    return result
