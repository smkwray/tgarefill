"""Auction-schedule surprise shocks for TGA rebuild identification.

Constructs weekly surprise measures from FiscalData auction results and
DTS tax receipts to replace the partially predictable binary rebuild flag.
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from tgarefill.utils.files import coerce_numeric, normalize_columns


_TAX_RECEIPT_PATTERN = (
    r"\b(?:"
    r"tax(?:es)?|customs|withheld|withholding|employment taxes|corporate income|"
    r"estate|excise|futa|seca|fica|irs"
    r")\b"
)


def build_bill_size_surprise(
    auctions: pd.DataFrame,
    min_date: str = "2005-01-01",
    lookback: int = 8,
    min_history: int = 4,
    exclude_cmb: bool = True,
) -> pd.DataFrame:
    """Weekly bill-size surprise: offering_amt minus trailing median for same term.

    For each non-CMB bill auction, expected size = trailing median of the
    last `lookback` auctions with the same (security_term, reopening) group.
    Surprise = offering_amt - expected. Aggregated to weekly (Wednesday).

    Returns DataFrame with columns: date, bill_size_surprise.
    """
    df = normalize_columns(auctions).copy()
    df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")
    df["offering_amt"] = coerce_numeric(df["offering_amt"])
    df = df.dropna(subset=["issue_date", "offering_amt"])
    df = df[df["issue_date"] >= min_date]

    # Filter to bills
    mask = df["security_type"].str.lower().str.strip() == "bill"
    if exclude_cmb:
        mask &= df["cash_management_bill_cmb"].str.lower().str.strip() != "yes"
    bills = df[mask].sort_values("issue_date").copy()

    if bills.empty:
        return pd.DataFrame(columns=["date", "bill_size_surprise"])

    # Group key for trailing median
    bills["group"] = bills["security_term"].str.strip() + "_" + bills["reopening"].str.strip()

    # Compute trailing median per group
    surprises = []
    for _, group_df in bills.groupby("group"):
        g = group_df.sort_values("issue_date").copy()
        g["expected"] = (
            g["offering_amt"]
            .shift(1)
            .rolling(window=lookback, min_periods=min_history)
            .median()
        )
        g["surprise"] = g["offering_amt"] - g["expected"]
        surprises.append(g[["issue_date", "surprise"]].dropna())

    if not surprises:
        return pd.DataFrame(columns=["date", "bill_size_surprise"])

    all_surprises = pd.concat(surprises, ignore_index=True)
    # Aggregate to weekly (Wednesday)
    all_surprises["date"] = (
        all_surprises["issue_date"].dt.to_period("W-WED").dt.end_time.dt.normalize()
    )
    weekly = all_surprises.groupby("date", as_index=False)["surprise"].sum()
    weekly = weekly.rename(columns={"surprise": "bill_size_surprise"})
    # Convert from raw USD to millions
    weekly["bill_size_surprise"] = weekly["bill_size_surprise"] / 1e6
    return weekly.sort_values("date").reset_index(drop=True)


def build_short_notice_cmb(
    auctions: pd.DataFrame,
    min_date: str = "2005-01-01",
) -> pd.DataFrame:
    """Weekly short-notice CMB issuance: auctions announced after prior Wednesday.

    Captures cash-management bills announced too late to be in the prior
    week's information set.

    Returns DataFrame with columns: date, short_notice_issuance.
    """
    df = normalize_columns(auctions).copy()
    df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")
    df["announcemt_date"] = pd.to_datetime(df["announcemt_date"], errors="coerce")
    df["offering_amt"] = coerce_numeric(df["offering_amt"])
    df = df.dropna(subset=["issue_date", "announcemt_date", "offering_amt"])
    df = df[df["issue_date"] >= min_date]
    if "cash_management_bill_cmb" not in df.columns:
        return pd.DataFrame(columns=["date", "short_notice_issuance"])

    # Issue week (Wednesday-ending)
    df["issue_week"] = df["issue_date"].dt.to_period("W-WED").dt.end_time.dt.normalize()
    # Prior Wednesday = issue_week - 7 days
    df["prior_wednesday"] = df["issue_week"] - pd.Timedelta(days=7)
    # Short notice: announced after the prior Wednesday
    short = df[
        (df["announcemt_date"] > df["prior_wednesday"])
        & (df["cash_management_bill_cmb"].astype(str).str.lower().str.strip() == "yes")
    ].copy()

    if short.empty:
        return pd.DataFrame(columns=["date", "short_notice_issuance"])

    weekly = short.groupby("issue_week", as_index=False)["offering_amt"].sum()
    weekly = weekly.rename(columns={"issue_week": "date", "offering_amt": "short_notice_issuance"})
    weekly["short_notice_issuance"] = weekly["short_notice_issuance"] / 1e6
    return weekly.sort_values("date").reset_index(drop=True)


def build_tax_receipt_surprise(
    dts_deposits: pd.DataFrame,
    min_date: str = "2005-01-01",
    min_history: int = 1,
) -> pd.DataFrame:
    """Weekly tax-receipt surprise: actual deposits minus same-week-of-year trailing mean.

    Uses DTS deposit flows to construct a non-issuance TGA surprise control.

    Returns DataFrame with columns: date, tax_receipt_surprise.
    """
    df = normalize_columns(dts_deposits).copy()
    df["record_date"] = pd.to_datetime(df["record_date"], errors="coerce")
    df["transaction_today_amt"] = coerce_numeric(df["transaction_today_amt"])
    df = df.dropna(subset=["record_date", "transaction_today_amt"])
    df = df[df["record_date"] >= min_date]

    # Filter to deposits only
    if "transaction_type" in df.columns:
        df = df[df["transaction_type"].str.lower().str.strip() == "deposits"]
    if "transaction_catg" in df.columns:
        df = df[df["transaction_catg"].fillna("").str.contains(_TAX_RECEIPT_PATTERN, case=False, regex=True)]

    # Aggregate daily deposits to weekly
    df["date"] = df["record_date"].dt.to_period("W-WED").dt.end_time.dt.normalize()
    weekly = df.groupby("date", as_index=False)["transaction_today_amt"].sum()
    weekly = weekly.rename(columns={"transaction_today_amt": "deposits_total"})
    weekly = weekly.sort_values("date").reset_index(drop=True)

    # Week-of-year seasonal expectation (trailing mean of same ISO week)
    weekly["iso_week"] = weekly["date"].dt.isocalendar().week.astype(int)
    weekly["expected"] = (
        weekly.groupby("iso_week")["deposits_total"]
        .transform(lambda x: x.shift(1).expanding(min_periods=min_history).mean())
    )
    weekly["tax_receipt_surprise"] = (weekly["deposits_total"] - weekly["expected"]) / 1e6
    return weekly[["date", "tax_receipt_surprise"]].dropna().reset_index(drop=True)
