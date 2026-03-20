from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from tgarefill.utils.files import coerce_numeric, normalize_columns, snake_case


def _first_matching_column(columns: list[str], patterns: list[str]) -> str | None:
    lower = [col.lower() for col in columns]
    for pattern in patterns:
        for original, lowered in zip(columns, lower):
            if pattern in lowered:
                return original
    return None


def load_staged_fred_long(staging_dir: Path) -> pd.DataFrame:
    path = staging_dir / "fred" / "fred_long.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    return df


def build_dts_wednesday_tga(staging_dir: Path) -> pd.DataFrame:
    """Build Wednesday-close TGA series from DTS operating cash balance.

    DTS reports daily closing balances. We filter to the Federal Reserve
    Account / TGA rows and select Wednesday observations. This gives a
    point-in-time Wednesday level, unlike WTREGEN which is a week-average.
    Values are already in millions of USD.
    """
    path = staging_dir / "fiscaldata" / "dts_operating_cash_balance.parquet"
    if not path.exists():
        return pd.DataFrame(columns=["date", "tga_dts_wednesday"])

    df = pd.read_parquet(path)
    df = normalize_columns(df)

    # Find the account type column and balance column
    acct_col = None
    for candidate in ("account_type", "account_typ"):
        if candidate in df.columns:
            acct_col = candidate
            break
    if acct_col is None:
        return pd.DataFrame(columns=["date", "tga_dts_wednesday"])

    # Filter to TGA-relevant accounts.
    # Pre-April 2022: account_type = "Federal Reserve Account" or
    #   "Treasury General Account (TGA)", balance in close_today_bal.
    # Post-April 2022: separate "Closing Balance" / "Opening Balance" rows,
    #   balance in open_today_bal (close_today_bal is null).
    mask = df[acct_col].str.contains(
        "Federal Reserve|Treasury General Account", case=False, na=False
    )
    # Exclude opening-balance and deposit/withdrawal subtotal rows
    exclude = df[acct_col].str.contains(
        "Opening Balance|Deposits|Withdrawals", case=False, na=False
    )
    tga = df[mask & ~exclude].copy()

    # Parse date and balance
    date_col = "record_date" if "record_date" in tga.columns else tga.columns[0]
    tga["date"] = pd.to_datetime(tga[date_col], errors="coerce")

    # Unify balance column across schemas
    if "close_today_bal" in tga.columns and "open_today_bal" in tga.columns:
        close_vals = coerce_numeric(tga["close_today_bal"])
        open_vals = coerce_numeric(tga["open_today_bal"])
        tga["tga_dts_wednesday"] = close_vals.fillna(open_vals)
    elif "close_today_bal" in tga.columns:
        tga["tga_dts_wednesday"] = coerce_numeric(tga["close_today_bal"])
    elif "open_today_bal" in tga.columns:
        tga["tga_dts_wednesday"] = coerce_numeric(tga["open_today_bal"])
    else:
        return pd.DataFrame(columns=["date", "tga_dts_wednesday"])

    tga = tga.dropna(subset=["date", "tga_dts_wednesday"])

    # Anchor each week to Wednesday, falling back to the latest prior business day
    # when the Wednesday DTS entry is missing (for example, on market holidays).
    tga["week_end_date"] = tga["date"].dt.to_period("W-WED").dt.end_time.dt.normalize()
    tga = (
        tga.sort_values(["week_end_date", "date"])
        .groupby("week_end_date", as_index=False)
        .last()[["week_end_date", "date", "tga_dts_wednesday"]]
        .rename(columns={"week_end_date": "date", "date": "tga_dts_source_date"})
    )
    return tga.sort_values("date").reset_index(drop=True)


# FRED H.8 and ON RRP series are in billions; H.4.1 series are in millions.
# Convert everything to millions for consistent attribution math.
_BILLIONS_TO_MILLIONS_SERIES: set[str] = {
    "commercial_bank_deposits_weekly_nsa",
    "commercial_bank_deposits_weekly_sa",
    "bank_treasury_and_agency_securities_weekly_nsa",
    "bank_treasury_and_agency_securities_weekly_sa",
    "other_deposits_weekly_nsa",
    "other_deposits_weekly_sa",
    "large_time_deposits_weekly_nsa",
    "large_time_deposits_weekly_sa",
    "on_rrp_daily_total",
    "foreign_treasury_holdings_quarterly",
}


def build_weekly_panel_from_fred(
    fred_long: pd.DataFrame,
    fred_series_config: dict[str, Any],
) -> pd.DataFrame:
    series_cfg = fred_series_config.get("series", {})
    frames: list[pd.DataFrame] = []

    for series_key, meta in series_cfg.items():
        series_df = fred_long.loc[fred_long["series_key"] == series_key, ["date", "value"]].copy()
        if series_df.empty:
            continue
        series_df = series_df.dropna(subset=["date"]).sort_values("date")

        # Convert billions to millions for H.8 and ON RRP series
        if series_key in _BILLIONS_TO_MILLIONS_SERIES:
            series_df["value"] = series_df["value"] * 1000

        frequency = str(meta.get("frequency", "weekly")).lower()
        if frequency in ("daily", "quarterly"):
            # Resample to weekly (Wednesday) — forward-fills quarterly gaps
            series_df = (
                series_df.set_index("date")
                .resample("W-WED")
                .last()
                .ffill()
                .rename(columns={"value": series_key})
            )
        else:
            series_df = (
                series_df.drop_duplicates(subset=["date"], keep="last")
                .set_index("date")
                .rename(columns={"value": series_key})
            )
        frames.append(series_df)

    if not frames:
        return pd.DataFrame(columns=["date"])

    weekly = pd.concat(frames, axis=1).sort_index().reset_index().rename(columns={"index": "date"})
    weekly["date"] = pd.to_datetime(weekly["date"]).dt.normalize()
    return weekly


def load_staged_fiscal_table(staging_dir: Path, key: str) -> pd.DataFrame | None:
    path = staging_dir / "fiscaldata" / f"{key}.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    df = normalize_columns(df)
    return df


def build_weekly_auction_mix(auctions_df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(auctions_df)
    date_col = _first_matching_column(
        list(df.columns),
        ["issue_date", "auction_date", "record_date", "security_issue_date"],
    )
    amount_col = _first_matching_column(
        list(df.columns),
        ["offering_amount", "accepted_amount", "issue_amount", "announced_amount", "total_accepted"],
    )
    type_col = _first_matching_column(
        list(df.columns),
        ["security_type", "security_type_desc", "security_term", "security_term_week_year", "security_class"],
    )

    if not date_col or not amount_col or not type_col:
        return pd.DataFrame(columns=["date", "bill", "coupon", "other", "auction_total", "bill_share", "coupon_share"])

    out = df[[date_col, amount_col, type_col]].copy()
    out["date"] = pd.to_datetime(out[date_col], errors="coerce")
    out["amount"] = coerce_numeric(out[amount_col])
    out["security_text"] = out[type_col].astype(str).str.lower()
    out = out.dropna(subset=["date", "amount"])

    out["bucket"] = "other"
    out.loc[out["security_text"].str.contains("bill", na=False), "bucket"] = "bill"
    out.loc[
        out["security_text"].str.contains("note|bond|frn|tips|coupon", regex=True, na=False),
        "bucket",
    ] = "coupon"

    out["date"] = out["date"].dt.to_period("W-WED").dt.end_time.dt.normalize()

    grouped = (
        out.pivot_table(index="date", columns="bucket", values="amount", aggfunc="sum", fill_value=0.0)
        .reset_index()
    )
    grouped.columns = [snake_case(str(col)) for col in grouped.columns]

    for required in ("bill", "coupon", "other"):
        if required not in grouped.columns:
            grouped[required] = 0.0

    grouped["auction_total"] = grouped["bill"] + grouped["coupon"] + grouped["other"]
    grouped["bill_share"] = grouped["bill"] / grouped["auction_total"]
    grouped["coupon_share"] = grouped["coupon"] / grouped["auction_total"]
    return grouped.sort_values("date").reset_index(drop=True)


def load_staged_ofr_long(staging_dir: Path, dataset: str) -> pd.DataFrame | None:
    path = staging_dir / "ofr" / f"{dataset}_long.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    return df


def build_ofr_weekly_columns(staging_dir: Path) -> pd.DataFrame | None:
    """Extract weekly OFR series and align to Wednesday dates.

    Returns a DataFrame with date + OFR columns, or None if no data.
    """
    # NYPD dealer Treasury repo (weekly)
    nypd = load_staged_ofr_long(staging_dir, "nypd")
    if nypd is None:
        return None

    dealer_repo = nypd.loc[
        nypd["series_key"] == "NYPD-PD_RP_T_TOT-A", ["date", "value"]
    ].copy()
    if dealer_repo.empty:
        return None

    # OFR values are in raw USD; convert to millions to match FRED H.4.1
    dealer_repo["dealer_treasury_repo"] = dealer_repo["value"] / 1e6
    dealer_repo = (
        dealer_repo.drop(columns=["value"])
        .set_index("date")
        .resample("W-WED")
        .last()
        .reset_index()
    )
    return dealer_repo


def build_ofr_monthly_columns(staging_dir: Path) -> pd.DataFrame | None:
    """Extract monthly OFR series for the monthly panel.

    Returns a DataFrame with date + OFR columns, or None if no data.
    """
    mmf = load_staged_ofr_long(staging_dir, "mmf")
    if mmf is None:
        return None

    frames: list[pd.DataFrame] = []

    # MMF total Treasury holdings
    treas = mmf.loc[mmf["series_key"] == "MMF-MMF_T_TOT-M", ["date", "value"]].copy()
    if not treas.empty:
        treas["mmf_treasury_holdings"] = treas["value"] / 1e6  # to millions
        frames.append(treas[["date", "mmf_treasury_holdings"]].set_index("date"))

    # MMF total repo
    repo = mmf.loc[mmf["series_key"] == "MMF-MMF_RP_TOT-M", ["date", "value"]].copy()
    if not repo.empty:
        repo["mmf_repo_total"] = repo["value"] / 1e6
        frames.append(repo[["date", "mmf_repo_total"]].set_index("date"))

    if not frames:
        return None

    result = pd.concat(frames, axis=1).sort_index().reset_index()
    result["date"] = pd.to_datetime(result["date"]).dt.normalize()
    return result


def merge_weekly_panel(
    weekly_fred: pd.DataFrame,
    weekly_auction_mix: pd.DataFrame | None = None,
    weekly_ofr: pd.DataFrame | None = None,
    monthly_ofr: pd.DataFrame | None = None,
) -> pd.DataFrame:
    out = weekly_fred.copy()
    if weekly_auction_mix is not None and not weekly_auction_mix.empty:
        out = out.merge(weekly_auction_mix, on="date", how="left")
    if weekly_ofr is not None and not weekly_ofr.empty:
        out = out.merge(weekly_ofr, on="date", how="left")
    # Forward-fill monthly OFR data into the weekly panel
    if monthly_ofr is not None and not monthly_ofr.empty:
        out = out.sort_values("date")
        monthly_sorted = monthly_ofr.sort_values("date")
        out = pd.merge_asof(out, monthly_sorted, on="date", direction="backward")
    return out.sort_values("date").reset_index(drop=True)


def build_monthly_panel_from_weekly(weekly_df: pd.DataFrame) -> pd.DataFrame:
    if weekly_df.empty:
        return pd.DataFrame(columns=["date"])
    monthly = (
        weekly_df.set_index("date")
        .sort_index()
        .resample("ME")
        .last()
        .reset_index()
    )
    return monthly
