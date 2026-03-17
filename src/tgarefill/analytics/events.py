from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


def classify_issuance_mix(
    bill_share: float | None,
    bill_heavy_threshold: float = 0.70,
    coupon_heavy_threshold: float = 0.30,
) -> str:
    if bill_share is None or pd.isna(bill_share):
        return "unknown"
    if bill_share >= bill_heavy_threshold:
        return "bill-heavy"
    if bill_share <= coupon_heavy_threshold:
        return "coupon-heavy"
    return "mixed"


def _positive_quantile(series: pd.Series, quantile: float) -> float:
    positive = series[series > 0].dropna()
    if positive.empty:
        return float("inf")
    return float(positive.quantile(quantile))


def _trailing_zscore(series: pd.Series, window: int) -> pd.Series:
    history = series.shift(1)
    rolling = history.rolling(window=window, min_periods=max(window // 2, 8))
    mean = rolling.mean()
    std = rolling.std().replace(0, pd.NA)
    return (series - mean) / std


def compute_event_flags(
    weekly_df: pd.DataFrame,
    tga_col: str,
    delta_tga_quantile: float = 0.90,
    rolling_4w_quantile: float = 0.90,
    rolling_8w_quantile: float = 0.90,
    rolling_z_window_weeks: int | None = None,
    delta_tga_zscore_threshold: float | None = None,
    rolling_4w_zscore_threshold: float | None = None,
    rolling_8w_zscore_threshold: float | None = None,
    min_auction_total: float | None = None,
) -> pd.DataFrame:
    df = weekly_df.copy().sort_values("date").reset_index(drop=True)
    if tga_col not in df.columns:
        raise KeyError(f"Missing TGA column: {tga_col}")

    df["date"] = pd.to_datetime(df["date"])
    df["delta_tga"] = df[tga_col].diff()
    df["rolling_4w_tga_change"] = df[tga_col].diff(4)
    df["rolling_8w_tga_change"] = df[tga_col].diff(8)

    positive_delta = df["delta_tga"] > 0
    delta_cut = _positive_quantile(df["delta_tga"], delta_tga_quantile)
    roll4_cut = _positive_quantile(df["rolling_4w_tga_change"], rolling_4w_quantile)
    roll8_cut = _positive_quantile(df["rolling_8w_tga_change"], rolling_8w_quantile)
    df["delta_tga_threshold"] = delta_cut
    df["rolling_4w_tga_change_threshold"] = roll4_cut
    df["rolling_8w_tga_change_threshold"] = roll8_cut

    quantile_flag = (
        (df["delta_tga"] >= delta_cut)
        | (df["rolling_4w_tga_change"] >= roll4_cut)
        | (df["rolling_8w_tga_change"] >= roll8_cut)
    )

    if rolling_z_window_weeks:
        df["delta_tga_zscore"] = _trailing_zscore(df["delta_tga"], int(rolling_z_window_weeks))
        df["rolling_4w_tga_change_zscore"] = _trailing_zscore(
            df["rolling_4w_tga_change"],
            int(rolling_z_window_weeks),
        )
        df["rolling_8w_tga_change_zscore"] = _trailing_zscore(
            df["rolling_8w_tga_change"],
            int(rolling_z_window_weeks),
        )
        zscore_flag = pd.Series(False, index=df.index)
        if delta_tga_zscore_threshold is not None:
            zscore_flag |= df["delta_tga_zscore"] >= float(delta_tga_zscore_threshold)
        if rolling_4w_zscore_threshold is not None:
            zscore_flag |= df["rolling_4w_tga_change_zscore"] >= float(rolling_4w_zscore_threshold)
        if rolling_8w_zscore_threshold is not None:
            zscore_flag |= df["rolling_8w_tga_change_zscore"] >= float(rolling_8w_zscore_threshold)
    else:
        zscore_flag = pd.Series(False, index=df.index)

    issuance_filter = pd.Series(True, index=df.index)
    if min_auction_total is not None and "auction_total" in df.columns:
        issuance_filter = df["auction_total"].fillna(0) >= float(min_auction_total)

    df["rapid_rebuild_flag"] = positive_delta & issuance_filter & (quantile_flag | zscore_flag)
    return df


def _group_contiguous_flags(
    df: pd.DataFrame,
    flag_col: str,
    max_gap_days_within_event: int = 9,
) -> pd.Series:
    is_flag = df[flag_col].fillna(False)
    gaps = df["date"].diff().dt.days.fillna(max_gap_days_within_event + 1)
    new_group = is_flag & ((~is_flag.shift(fill_value=False)) | (gaps > max_gap_days_within_event))
    return new_group.cumsum().where(is_flag)


def detect_rebuild_events(
    weekly_df: pd.DataFrame,
    tga_col: str,
    bill_share_col: str | None = "bill_share",
    rules: dict[str, Any] | None = None,
) -> pd.DataFrame:
    rules = dict(rules or {})
    flagged = compute_event_flags(
        weekly_df=weekly_df,
        tga_col=tga_col,
        delta_tga_quantile=rules.get("delta_tga_quantile", 0.90),
        rolling_4w_quantile=rules.get("rolling_4w_quantile", 0.90),
        rolling_8w_quantile=rules.get("rolling_8w_quantile", 0.90),
        rolling_z_window_weeks=rules.get("rolling_z_window_weeks"),
        delta_tga_zscore_threshold=rules.get("delta_tga_zscore_threshold"),
        rolling_4w_zscore_threshold=rules.get("rolling_4w_zscore_threshold"),
        rolling_8w_zscore_threshold=rules.get("rolling_8w_zscore_threshold"),
        min_auction_total=rules.get("min_auction_total"),
    )
    flagged["event_group"] = _group_contiguous_flags(
        flagged,
        flag_col="rapid_rebuild_flag",
        max_gap_days_within_event=int(rules.get("max_gap_days_within_event", 9)),
    )

    events: list[dict[str, Any]] = []
    for group_id, group in flagged.dropna(subset=["event_group"]).groupby("event_group"):
        group = group.sort_values("date")
        start_idx = group.index.min()
        baseline_idx = max(start_idx - 1, 0)
        baseline_row = flagged.loc[baseline_idx]
        end_row = group.iloc[-1]

        delta_tga_event = end_row[tga_col] - baseline_row[tga_col]
        mean_bill_share = group[bill_share_col].mean() if bill_share_col in group.columns else None

        events.append(
            {
                "event_id": f"event_{int(group_id):03d}",
                "baseline_date": baseline_row["date"],
                "start_date": group["date"].min(),
                "end_date": group["date"].max(),
                "duration_weeks": int(len(group)),
                "delta_tga_event": delta_tga_event,
                "max_weekly_delta_tga": group["delta_tga"].max(),
                "mean_bill_share": mean_bill_share,
                "issuance_mix": classify_issuance_mix(
                    mean_bill_share,
                    bill_heavy_threshold=float(rules.get("bill_heavy_threshold", 0.70)),
                    coupon_heavy_threshold=float(rules.get("coupon_heavy_threshold", 0.30)),
                ),
            }
        )

    return pd.DataFrame(events).sort_values("start_date").reset_index(drop=True)


def attach_manual_labels(
    events_df: pd.DataFrame,
    manual_labels: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if manual_labels is None or manual_labels.empty:
        return events_df

    out = events_df.copy()
    out["manual_tags"] = None
    out["manual_notes"] = None

    labels = manual_labels.copy()
    labels["start_date"] = pd.to_datetime(labels["start_date"])
    labels["end_date"] = pd.to_datetime(labels["end_date"])

    for i, event in out.iterrows():
        overlaps = labels[
            (labels["start_date"] <= event["end_date"])
            & (labels["end_date"] >= event["start_date"])
        ]
        if overlaps.empty:
            continue
        out.at[i, "manual_tags"] = ";".join(
            overlaps["tag"].dropna().astype(str).unique().tolist()
        ) or None
        out.at[i, "manual_notes"] = " | ".join(
            overlaps["notes"].dropna().astype(str).tolist()
        ) or None

    return out
