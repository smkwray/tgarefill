from __future__ import annotations

from typing import Iterable

import pandas as pd

DEFAULT_PROXY_MAP: dict[str, tuple[str, int]] = {
    "reserve_drain_proxy": ("reserve_balances_weekly_wednesday", -1),
    "deposit_drawdown_proxy": ("commercial_bank_deposits_weekly_nsa", -1),
    "on_rrp_runoff_proxy": ("on_rrp_daily_total", -1),
    "bank_treasury_absorption_proxy": ("bank_treasury_and_agency_securities_weekly_nsa", 1),
    "mmf_treasury_absorption_proxy": ("mmf_treasury_holdings", 1),
    "dealer_repo_proxy": ("dealer_treasury_repo", 1),
    # foreign_treasury_holdings_quarterly excluded: quarterly forward-fill
    # into weekly panel produces invalid event-window deltas.
    # Use only in matched-frequency (quarterly/monthly) analysis.
}


def normalize_proxy_map(proxy_map: dict[str, tuple[str, int] | dict[str, object]]) -> dict[str, tuple[str, int]]:
    normalized: dict[str, tuple[str, int]] = {}
    for proxy_name, spec in proxy_map.items():
        if isinstance(spec, tuple):
            normalized[proxy_name] = spec
            continue
        column = str(spec.get("column", ""))
        if not column:
            raise ValueError(f"Proxy '{proxy_name}' is missing a column")
        normalized[proxy_name] = (column, int(spec.get("sign", 1)))
    return normalized


def _last_value_on_or_before(
    panel: pd.DataFrame,
    date: pd.Timestamp,
    column: str,
    max_lookback_days: int | None = None,
) -> float | None:
    subset = panel.loc[panel["date"] <= date, ["date", column]].dropna()
    if subset.empty:
        return None
    last_row = subset.sort_values("date").iloc[-1]
    if max_lookback_days is not None:
        lookback_days = int((pd.Timestamp(date) - pd.Timestamp(last_row["date"])).days)
        if lookback_days > max_lookback_days:
            return None
    return last_row[column]


def build_baseline_attribution(
    events_df: pd.DataFrame,
    weekly_panel: pd.DataFrame,
    tga_col: str = "tga_weekly_wednesday",
    proxy_map: dict[str, tuple[str, int] | dict[str, object]] | None = None,
    max_lookback_days: int | None = 14,
) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame()

    panel = weekly_panel.copy().sort_values("date").reset_index(drop=True)
    panel["date"] = pd.to_datetime(panel["date"])
    proxies = normalize_proxy_map(proxy_map or DEFAULT_PROXY_MAP)

    rows: list[dict[str, object]] = []

    for event in events_df.itertuples(index=False):
        baseline_date = pd.to_datetime(getattr(event, "baseline_date"))
        end_date = pd.to_datetime(getattr(event, "end_date"))

        tga_start = _last_value_on_or_before(panel, baseline_date, tga_col, max_lookback_days)
        tga_end = _last_value_on_or_before(panel, end_date, tga_col, max_lookback_days)
        if tga_start is None or tga_end is None:
            continue

        delta_tga = tga_end - tga_start
        row: dict[str, object] = {
            "event_id": getattr(event, "event_id"),
            "baseline_date": baseline_date,
            "end_date": end_date,
            "delta_tga": delta_tga,
            "issuance_mix": getattr(event, "issuance_mix", None),
        }

        proxy_amounts: dict[str, float] = {}
        for proxy_name, (column, sign) in proxies.items():
            if column not in panel.columns:
                row[f"{proxy_name}_source_column"] = column
                row[f"{proxy_name}_delta"] = None
                row[f"{proxy_name}_positive_proxy_amount"] = None
                continue

            start_val = _last_value_on_or_before(panel, baseline_date, column, max_lookback_days)
            end_val = _last_value_on_or_before(panel, end_date, column, max_lookback_days)
            if start_val is None or end_val is None:
                row[f"{proxy_name}_source_column"] = column
                row[f"{proxy_name}_delta"] = None
                row[f"{proxy_name}_positive_proxy_amount"] = None
                continue

            raw_delta = end_val - start_val
            proxy_amount = max(float(sign) * float(raw_delta), 0.0)

            row[f"{proxy_name}_source_column"] = column
            row[f"{proxy_name}_delta"] = raw_delta
            row[f"{proxy_name}_positive_proxy_amount"] = proxy_amount
            proxy_amounts[proxy_name] = proxy_amount

        total_positive = sum(proxy_amounts.values())
        row["positive_proxy_total"] = total_positive
        row["unexplained_residual_against_tga"] = (
            delta_tga - total_positive if pd.notna(delta_tga) else None
        )

        for proxy_name, amount in proxy_amounts.items():
            row[f"{proxy_name}_share_of_positive_proxy_total"] = (
                amount / total_positive if total_positive > 0 else None
            )
            row[f"{proxy_name}_share_of_delta_tga"] = (
                amount / delta_tga if delta_tga not in (None, 0) else None
            )

        rows.append(row)

    return pd.DataFrame(rows)
