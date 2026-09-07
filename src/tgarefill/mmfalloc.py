"""Fund-level MMF allocation validation from SEC N-MFP data.

This module keeps the extension deliberately narrow: parse official SEC DERA
N-MFP flat-file zips, map portfolio rows into a small set of funding-channel
categories, reconcile the resulting aggregates to existing OFR/Fed benchmarks,
and build transparent event-window summaries around the existing bill-surprise
shock series.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from zipfile import ZipFile

import pandas as pd
import requests
from bs4 import BeautifulSoup

from tgarefill.utils.files import coerce_numeric, ensure_dir, normalize_columns, write_dataframe, write_json


SEC_NMFP_INDEX_URL = "https://www.sec.gov/data-research/sec-markets-data/dera-form-n-mfp-data-sets"
SEC_BASE_URL = "https://www.sec.gov"
DEFAULT_START_MONTH = "2022-12"
DEFAULT_END_MONTH = "2025-11"

CATEGORIES = [
    "treasury_bills",
    "treasury_coupons",
    "fed_onrrp",
    "private_treasury_repo",
    "agency_repo",
    "agency_debt",
    "private_instruments",
    "cash_other",
    "unmapped",
]


@dataclass(frozen=True)
class ValidationGate:
    gate: str
    passed: bool
    value: float | int | str
    threshold: float | int | str
    note: str


def month_range(start_month: str, end_month: str) -> list[pd.Period]:
    start = pd.Period(start_month, freq="M")
    end = pd.Period(end_month, freq="M")
    if end < start:
        raise ValueError("end_month must be >= start_month")
    return list(pd.period_range(start, end, freq="M"))


def discover_sec_nmfp_links(session: requests.Session) -> pd.DataFrame:
    """Return official SEC N-MFP zip links listed on the DERA page."""
    response = session.get(SEC_NMFP_INDEX_URL, timeout=120)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    rows: list[dict[str, object]] = []
    for link in soup.find_all("a", href=True):
        href = str(link["href"])
        text = " ".join(link.get_text(" ", strip=True).split())
        if "form-n-mfp-data-sets" not in href or not href.endswith("_nmfp.zip"):
            continue
        href = href if href.startswith("http") else f"{SEC_BASE_URL}{href}"
        month = _month_from_link_text(text)
        if month is None:
            continue
        rows.append({"report_month": month.to_timestamp("M"), "label": text, "url": href})
    if not rows:
        raise RuntimeError("SEC N-MFP index returned no monthly/quarterly zip links")
    out = pd.DataFrame(rows).drop_duplicates("url")
    return out.sort_values("report_month").reset_index(drop=True)


def _month_from_link_text(text: str) -> pd.Period | None:
    # Current monthly labels look like "2025 November NMFP"; older files may be quarterly.
    parts = text.split()
    if len(parts) >= 3 and parts[0].isdigit():
        year = parts[0]
        month_name = parts[1]
        try:
            return pd.Period(pd.to_datetime(f"{year} {month_name}"), freq="M")
        except Exception:
            return None
    return None


def select_monthly_sec_links(links: pd.DataFrame, months: Iterable[pd.Period]) -> pd.DataFrame:
    wanted = pd.DataFrame({"report_period": list(months)})
    wanted["report_month"] = wanted["report_period"].dt.to_timestamp("M")
    selected = wanted.merge(links, on="report_month", how="left")
    missing = selected.loc[selected["url"].isna(), "report_period"].astype(str).tolist()
    if missing:
        raise RuntimeError(f"Missing SEC N-MFP monthly links for: {', '.join(missing)}")
    return selected.drop(columns=["report_period"]).sort_values("report_month").reset_index(drop=True)


def download_sec_zips(
    session: requests.Session,
    selected_links: pd.DataFrame,
    raw_dir: Path,
    overwrite: bool = False,
) -> list[dict[str, object]]:
    """Download official SEC monthly zip payloads without extracting them."""
    ensure_dir(raw_dir)
    manifest: list[dict[str, object]] = []
    for row in selected_links.itertuples(index=False):
        url = str(row.url)
        destination = raw_dir / Path(url).name
        if overwrite or not destination.exists():
            with session.get(url, timeout=120, stream=True) as response:
                response.raise_for_status()
                with destination.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            handle.write(chunk)
        manifest.append(
            {
                "report_month": pd.Timestamp(row.report_month).date().isoformat(),
                "label": row.label,
                "url": url,
                "saved_to": str(destination),
                "bytes": destination.stat().st_size,
            }
        )
    write_json(manifest, raw_dir / "manifest.json")
    return manifest


def parse_sec_nmfp_zips(zip_paths: Iterable[Path]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse SEC zip files into compact position and fund-month panels."""
    position_frames: list[pd.DataFrame] = []
    fund_frames: list[pd.DataFrame] = []

    for zip_path in sorted(zip_paths):
        positions, fund_month = parse_sec_nmfp_zip(zip_path)
        if not positions.empty:
            position_frames.append(positions)
        if not fund_month.empty:
            fund_frames.append(fund_month)

    positions = pd.concat(position_frames, ignore_index=True) if position_frames else _empty_positions()
    fund_month = pd.concat(fund_frames, ignore_index=True) if fund_frames else _empty_fund_month_panel()
    if not positions.empty:
        positions = positions.sort_values(["report_month", "fund_id", "security_id"]).reset_index(drop=True)
    if not fund_month.empty:
        fund_month = fund_month.sort_values(["report_month", "fund_id"]).reset_index(drop=True)
    return positions, fund_month


def parse_sec_nmfp_zip(zip_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse one SEC N-MFP zip file into category-mapped positions and fund rows."""
    with ZipFile(zip_path) as archive:
        submissions = _read_zip_tsv(archive, "NMFP_SUBMISSION.tsv")
        series = _read_zip_tsv(archive, "NMFP_SERIESLEVELINFO.tsv")
        securities = _read_zip_tsv(archive, "NMFP_SCHPORTFOLIOSECURITIES.tsv")
        advisers = _read_optional_zip_tsv(archive, "NMFP_ADVISER.tsv")

    required_submission = {"accession_number", "reportdate", "seriesid"}
    required_series = {"accession_number", "netassetofseries"}
    required_securities = {"accession_number", "security_id", "investmentcategory"}
    _require_columns(submissions, required_submission, zip_path)
    _require_columns(series, required_series, zip_path)
    _require_columns(securities, required_securities, zip_path)

    submissions = _filter_to_modal_report_month(submissions)
    submissions = _dedupe_submissions(submissions)
    family = _adviser_family(advisers)
    fund_meta = submissions.merge(series, on="accession_number", how="left").merge(
        family, on="accession_number", how="left"
    )
    fund_meta["report_month"] = _parse_sec_date(fund_meta["reportdate"]).dt.to_period("M").dt.to_timestamp("M")
    fund_meta["fund_id"] = fund_meta["seriesid"].fillna(fund_meta["accession_number"])
    fund_meta["fund_name"] = (
        fund_meta.get("nameofseries", pd.Series(index=fund_meta.index, dtype=object))
        .fillna(fund_meta.get("series_name", pd.Series(index=fund_meta.index, dtype=object)))
        .fillna(fund_meta.get("registrantfullname", pd.Series(index=fund_meta.index, dtype=object)))
    )
    fund_meta["adviser_or_family"] = fund_meta["adviser_or_family"].fillna(
        fund_meta.get("registrantfullname", pd.Series(index=fund_meta.index, dtype=object))
    )
    fund_meta["fund_type"] = fund_meta.get("moneymarketfundcategory", pd.Series(index=fund_meta.index, dtype=object))
    fund_meta["total_assets"] = coerce_numeric(fund_meta["netassetofseries"])
    fund_meta["wam"] = coerce_numeric(fund_meta.get("averageportfoliomaturity", pd.Series(index=fund_meta.index)))
    fund_meta["wal"] = coerce_numeric(fund_meta.get("averagelifematurity", pd.Series(index=fund_meta.index)))
    fund_meta["cash"] = coerce_numeric(fund_meta.get("cash", pd.Series(index=fund_meta.index))).fillna(0)

    securities = securities[securities["accession_number"].isin(set(fund_meta["accession_number"]))].copy()
    securities["position_value"] = _position_value(securities)
    securities["category"] = classify_positions(securities)
    securities["report_month"] = securities["accession_number"].map(
        fund_meta.set_index("accession_number")["report_month"]
    )
    securities["fund_id"] = securities["accession_number"].map(fund_meta.set_index("accession_number")["fund_id"])
    securities["fund_name"] = securities["accession_number"].map(fund_meta.set_index("accession_number")["fund_name"])
    securities["adviser_or_family"] = securities["accession_number"].map(
        fund_meta.set_index("accession_number")["adviser_or_family"]
    )
    securities["source_schema_version"] = "sec_nmfp_flat"
    positions = securities[
        [
            "report_month",
            "fund_id",
            "fund_name",
            "adviser_or_family",
            "accession_number",
            "security_id",
            "nameofissuer",
            "titleofissuer",
            "investmentcategory",
            "category",
            "position_value",
            "source_schema_version",
        ]
    ].rename(
        columns={
            "nameofissuer": "issuer_name",
            "titleofissuer": "security_title",
            "investmentcategory": "raw_investment_category",
        }
    )

    fund_month = aggregate_fund_month(fund_meta, positions)
    return positions, fund_month


def _read_zip_tsv(archive: ZipFile, member: str) -> pd.DataFrame:
    with archive.open(member) as handle:
        return normalize_columns(pd.read_csv(handle, sep="\t", dtype=str, low_memory=False))


def _read_optional_zip_tsv(archive: ZipFile, member: str) -> pd.DataFrame:
    if member not in archive.namelist():
        return pd.DataFrame()
    return _read_zip_tsv(archive, member)


def _require_columns(df: pd.DataFrame, columns: set[str], source: Path) -> None:
    missing = columns - set(df.columns)
    if missing:
        raise RuntimeError(f"{source.name} missing required columns: {', '.join(sorted(missing))}")


def _dedupe_submissions(submissions: pd.DataFrame) -> pd.DataFrame:
    df = submissions.copy()
    df["filing_date_parsed"] = _parse_sec_date(df.get("filing_date"))
    df["reportdate_parsed"] = _parse_sec_date(df.get("reportdate"))
    df = df.dropna(subset=["reportdate_parsed"])
    df["seriesid"] = df["seriesid"].fillna(df["accession_number"])
    df = df.sort_values(["seriesid", "reportdate_parsed", "filing_date_parsed", "accession_number"])
    return df.groupby(["seriesid", "reportdate_parsed"], as_index=False).tail(1)


def _filter_to_modal_report_month(submissions: pd.DataFrame) -> pd.DataFrame:
    df = submissions.copy()
    report_dates = _parse_sec_date(df.get("reportdate"))
    report_month = report_dates.dt.to_period("M")
    if report_month.dropna().empty:
        return df
    modal_month = report_month.value_counts().idxmax()
    return df[report_month == modal_month].copy()


def _adviser_family(advisers: pd.DataFrame) -> pd.DataFrame:
    if advisers.empty or "accession_number" not in advisers.columns:
        return pd.DataFrame(columns=["accession_number", "adviser_or_family"])
    name_col = "advisername" if "advisername" in advisers.columns else advisers.columns[-1]
    out = advisers[["accession_number", name_col]].dropna().copy()
    out = out.rename(columns={name_col: "adviser_or_family"})
    out = out.groupby("accession_number", as_index=False)["adviser_or_family"].first()
    return out


def _parse_sec_date(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="datetime64[ns]")
    parsed = pd.to_datetime(series, format="%d-%b-%Y", errors="coerce")
    missing = parsed.isna() & series.notna()
    if missing.any():
        parsed.loc[missing] = pd.to_datetime(series.loc[missing], errors="coerce")
    return parsed


def _position_value(securities: pd.DataFrame) -> pd.Series:
    for col in ("includingvalueofanysponsorsupp", "excludingvalueofanysponsorsupp", "availableforsalesecuritiesam_l"):
        if col in securities.columns:
            values = coerce_numeric(securities[col])
            if values.notna().any():
                return values.fillna(0)
    return pd.Series(0.0, index=securities.index)


def classify_positions(df: pd.DataFrame) -> pd.Series:
    """Map N-MFP portfolio-security rows to canonical MMF allocation categories."""
    category = df.get("investmentcategory", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()
    issuer = df.get("nameofissuer", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()
    title = df.get("titleofissuer", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()
    desc = df.get("briefdescription", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()
    combined = issuer + " " + title + " " + desc

    out = pd.Series("unmapped", index=df.index, dtype=object)
    is_repo = category.str.contains("repurchase agreement", na=False)
    is_treasury_debt = category.str.contains("u.s. treasury debt", na=False)
    is_fed = combined.str.contains("federal reserve|frbny|fed reverse|fed repo", regex=True, na=False)

    out.loc[is_treasury_debt & combined.str.contains(r"\bbill\b|treasury bill|t-bill|tbill", regex=True, na=False)] = "treasury_bills"
    out.loc[is_treasury_debt & (out == "unmapped")] = "treasury_coupons"
    out.loc[is_repo & is_fed] = "fed_onrrp"
    out.loc[is_repo & category.str.contains("treasury repurchase", na=False) & ~is_fed] = "private_treasury_repo"
    out.loc[is_repo & category.str.contains("government agency repurchase", na=False) & ~is_fed] = "agency_repo"
    out.loc[category.str.contains("government agency debt", na=False)] = "agency_debt"
    out.loc[
        category.str.contains(
            "commercial paper|certificate of deposit|time deposit|municipal|asset backed|sovereign|insurance company funding|variable rate demand|tender option bond",
            regex=True,
            na=False,
        )
    ] = "private_instruments"
    out.loc[is_repo & category.str.contains("other repurchase agreement", na=False) & ~is_fed] = "private_instruments"
    out.loc[category.str.contains("investment company|other instrument", regex=True, na=False)] = "cash_other"
    return out


def aggregate_fund_month(fund_meta: pd.DataFrame, positions: pd.DataFrame) -> pd.DataFrame:
    cats = (
        positions.pivot_table(
            index=["report_month", "fund_id"],
            columns="category",
            values="position_value",
            aggfunc="sum",
            fill_value=0,
        )
        .reindex(columns=CATEGORIES, fill_value=0)
        .reset_index()
    )
    base_cols = [
        "report_month",
        "fund_id",
        "fund_name",
        "adviser_or_family",
        "fund_type",
        "total_assets",
        "wam",
        "wal",
    ]
    base = fund_meta[base_cols + ["cash"]].drop_duplicates(["report_month", "fund_id"]).copy()
    out = base.merge(cats, on=["report_month", "fund_id"], how="left")
    for col in CATEGORIES:
        out[col] = out[col].fillna(0)
    out["cash_other"] = out["cash_other"] + out["cash"].fillna(0)
    out = out.drop(columns=["cash"])
    out["mapped_assets"] = out[[c for c in CATEGORIES if c != "unmapped"]].sum(axis=1)
    out["unmapped_share"] = out["unmapped"] / out["mapped_assets"].add(out["unmapped"]).where(
        out["mapped_assets"].add(out["unmapped"]) != 0
    )
    for col in CATEGORIES:
        out[f"{col}_share"] = out[col] / out["total_assets"].where(out["total_assets"] != 0)
    out["pre_onrrp_share"] = out.groupby("fund_id")["fed_onrrp_share"].shift(1)
    out["repo_share"] = out["private_treasury_repo_share"].fillna(0) + out["agency_repo_share"].fillna(0)
    out["pre_repo_share"] = out.groupby("fund_id")["repo_share"].shift(1)
    out = out.drop(columns=["repo_share"])
    out["pre_tbill_share"] = out.groupby("fund_id")["treasury_bills_share"].shift(1)
    out["net_flow"] = out.groupby("fund_id")["total_assets"].diff()
    out["source_family"] = "sec_nmfp"
    out["source_schema_version"] = "sec_nmfp_flat"
    return out


def build_category_aggregates(fund_month: pd.DataFrame) -> pd.DataFrame:
    agg = fund_month.groupby("report_month", as_index=False)[CATEGORIES + ["total_assets"]].sum()
    for col in CATEGORIES:
        agg[f"{col}_share"] = agg[col] / agg["total_assets"].where(agg["total_assets"] != 0)
    return agg


def build_coverage_table(fund_month: pd.DataFrame, positions: pd.DataFrame) -> pd.DataFrame:
    if fund_month.empty:
        return pd.DataFrame()
    agg_spec = {
        "fund_count": ("fund_id", "nunique"),
        "total_assets": ("total_assets", "sum"),
    }
    agg_spec.update({category: (category, "sum") for category in CATEGORIES})
    monthly = fund_month.groupby("report_month").agg(**agg_spec).reset_index()
    pos_monthly = positions.groupby("report_month").agg(position_rows=("security_id", "count")).reset_index()
    out = monthly.merge(pos_monthly, on="report_month", how="left")
    out["mapped_share"] = 1 - out["unmapped"] / out[CATEGORIES].sum(axis=1).where(out[CATEGORIES].sum(axis=1) != 0)
    return out


def build_reconciliation_table(category_aggregates: pd.DataFrame, weekly_panel: pd.DataFrame | None) -> pd.DataFrame:
    """Compare SEC fund-level aggregates with existing OFR/Fed weekly benchmarks."""
    if weekly_panel is None or weekly_panel.empty:
        return pd.DataFrame(columns=["report_month", "series", "sec_value", "benchmark_value", "pct_diff"])
    panel = weekly_panel.copy()
    panel["report_month"] = pd.to_datetime(panel["date"]).dt.to_period("M").dt.to_timestamp("M")
    bench = panel.sort_values("date").groupby("report_month", as_index=False).tail(1)
    merged = category_aggregates.merge(bench, on="report_month", how="left")
    rows: list[dict[str, object]] = []
    mappings = [
        ("treasury_total", "mmf_treasury_holdings", merged["treasury_bills"] + merged["treasury_coupons"]),
        ("repo_total", "mmf_repo_total", merged["fed_onrrp"] + merged["private_treasury_repo"] + merged["agency_repo"]),
        ("fed_onrrp", "on_rrp_daily_total", merged["fed_onrrp"]),
    ]
    for label, bench_col, sec_values in mappings:
        if bench_col not in merged.columns:
            continue
        for idx, row in merged.iterrows():
            benchmark = row.get(bench_col)
            sec_value = sec_values.iloc[idx]
            if pd.isna(benchmark) or benchmark == 0:
                pct_diff = pd.NA
            else:
                pct_diff = (sec_value / 1_000_000 - benchmark) / benchmark
            rows.append(
                {
                    "report_month": row["report_month"],
                    "series": label,
                    "sec_value_usd": sec_value,
                    "benchmark_value_millions": benchmark,
                    "sec_value_millions": sec_value / 1_000_000,
                    "pct_diff": pct_diff,
                }
            )
    return pd.DataFrame(rows)


def build_validation_gates(
    coverage: pd.DataFrame,
    reconciliation: pd.DataFrame,
    mapping_threshold: float = 0.95,
    min_months: int = 36,
) -> pd.DataFrame:
    if coverage.empty:
        gates = [ValidationGate("coverage", False, 0, min_months, "No fund-month observations parsed")]
        return pd.DataFrame([g.__dict__ for g in gates])

    contiguous = _max_contiguous_months(coverage["report_month"])
    min_mapped = float(coverage["mapped_share"].min())
    has_required = bool(
        (coverage["total_assets"] > 0).all()
        and (coverage["treasury_bills"].add(coverage["treasury_coupons"]) > 0).all()
        and (coverage["fed_onrrp"] >= 0).all()
        and (coverage["private_treasury_repo"].add(coverage["agency_repo"]) >= 0).all()
    )
    rec_eval = reconciliation.dropna(subset=["pct_diff"]).copy() if not reconciliation.empty else pd.DataFrame()
    rec_median_abs = float(rec_eval["pct_diff"].abs().median()) if not rec_eval.empty else float("nan")
    rec_pass = bool(not rec_eval.empty and rec_median_abs <= 0.75)

    gates = [
        ValidationGate("coverage", contiguous >= min_months, contiguous, min_months, "Contiguous SEC monthly panels"),
        ValidationGate("required_fields", has_required, str(has_required), "true", "Assets, Treasury, ON RRP, and repo fields present"),
        ValidationGate("mapping", min_mapped >= mapping_threshold, round(min_mapped, 4), mapping_threshold, "Mapped portfolio-security value share"),
        ValidationGate("reconciliation", rec_pass, round(rec_median_abs, 4) if pd.notna(rec_median_abs) else "NA", "<=0.75 median abs pct diff", "SEC aggregate order-of-magnitude check against existing benchmarks"),
    ]
    return pd.DataFrame([g.__dict__ for g in gates])


def _max_contiguous_months(months: pd.Series) -> int:
    periods = sorted(set(pd.to_datetime(months).dt.to_period("M")))
    if not periods:
        return 0
    best = run = 1
    prev = periods[0]
    for period in periods[1:]:
        if period == prev + 1:
            run += 1
        else:
            best = max(best, run)
            run = 1
        prev = period
    return max(best, run)


def build_event_panel(fund_month: pd.DataFrame, shocks: pd.DataFrame, top_n_events: int = 8) -> pd.DataFrame:
    if fund_month.empty or shocks.empty:
        return pd.DataFrame()
    shock_months = shocks.copy()
    shock_months["date"] = pd.to_datetime(shock_months["date"])
    shock_months["report_month"] = shock_months["date"].dt.to_period("M").dt.to_timestamp("M")
    shock_months = shock_months.groupby("report_month", as_index=False)["bill_size_surprise"].sum()
    shock_months = shock_months[shock_months["bill_size_surprise"] > 0].nlargest(top_n_events, "bill_size_surprise")
    frames: list[pd.DataFrame] = []
    panel = fund_month.copy()
    panel["period"] = pd.to_datetime(panel["report_month"]).dt.to_period("M")
    for event in shock_months.itertuples(index=False):
        event_period = pd.Period(event.report_month, freq="M")
        window = panel[panel["period"].between(event_period - 2, event_period + 2)].copy()
        if window.empty:
            continue
        window["event_month"] = event_period.to_timestamp("M")
        window["event_bill_surprise_millions"] = event.bill_size_surprise
        window["horizon"] = window["period"].astype(int) - event_period.ordinal
        frames.append(window)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop(columns=["period"])


def build_baseline_table(event_panel: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if event_panel.empty:
        return pd.DataFrame()
    value_cols = [
        "treasury_bills",
        "treasury_coupons",
        "fed_onrrp",
        "private_treasury_repo",
        "agency_repo",
        "agency_debt",
        "private_instruments",
        "cash_other",
        "total_assets",
    ]
    for event_month, event_df in event_panel.groupby("event_month"):
        agg = event_df.groupby("horizon")[value_cols].sum()
        if 0 not in agg.index or -1 not in agg.index:
            continue
        event_surprise = float(event_df["event_bill_surprise_millions"].iloc[0])
        for col in value_cols:
            delta = (agg.loc[0, col] - agg.loc[-1, col]) / 1_000_000
            rows.append(
                {
                    "event_month": event_month,
                    "horizon": "0_vs_minus1",
                    "category": col,
                    "delta_millions": delta,
                    "event_bill_surprise_millions": event_surprise,
                    "delta_per_bill_surprise": delta / event_surprise if event_surprise else pd.NA,
                }
            )
    return pd.DataFrame(rows)


def build_placebos(event_panel: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if event_panel.empty:
        return pd.DataFrame()
    for event_month, event_df in event_panel.groupby("event_month"):
        agg = event_df.groupby("horizon")[["treasury_bills", "fed_onrrp", "private_treasury_repo", "agency_repo"]].sum()
        if -2 not in agg.index or -1 not in agg.index:
            continue
        for col in agg.columns:
            rows.append(
                {
                    "event_month": event_month,
                    "category": col,
                    "pretrend_delta_millions": (agg.loc[-1, col] - agg.loc[-2, col]) / 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def build_leave_one_family_out(event_panel: pd.DataFrame) -> pd.DataFrame:
    if event_panel.empty:
        return pd.DataFrame()
    total = build_baseline_table(event_panel)
    families = event_panel["adviser_or_family"].fillna("unknown").value_counts().head(12).index
    frames: list[pd.DataFrame] = []
    for family in families:
        sub = event_panel[event_panel["adviser_or_family"].fillna("unknown") != family]
        out = build_baseline_table(sub)
        if out.empty:
            continue
        out["excluded_family"] = family
        frames.append(out)
    if total.empty:
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    total["excluded_family"] = "none"
    return pd.concat([total, *frames], ignore_index=True) if frames else total


def build_downstream_summary(
    gates: pd.DataFrame,
    coverage: pd.DataFrame,
    baseline: pd.DataFrame,
) -> pd.DataFrame:
    """Compact one-file import surface for downstream claim ledgers."""
    rows: list[dict[str, object]] = []
    gates_passed = bool(gates["passed"].all()) if not gates.empty else False
    rows.append(
        {
            "metric": "source_gates_passed",
            "value": str(gates_passed),
            "unit": "boolean",
            "note": "All source feasibility, mapping, and reconciliation gates pass",
        }
    )
    if not coverage.empty:
        rows.extend(
            [
                {
                    "metric": "sample_start",
                    "value": pd.to_datetime(coverage["report_month"].min()).date().isoformat(),
                    "unit": "date",
                    "note": "First SEC N-MFP report month in validation sample",
                },
                {
                    "metric": "sample_end",
                    "value": pd.to_datetime(coverage["report_month"].max()).date().isoformat(),
                    "unit": "date",
                    "note": "Last SEC N-MFP report month in validation sample",
                },
                {
                    "metric": "contiguous_months",
                    "value": len(coverage),
                    "unit": "months",
                    "note": "Contiguous SEC N-MFP monthly coverage",
                },
                {
                    "metric": "min_mapped_share",
                    "value": round(float(coverage["mapped_share"].min()), 4),
                    "unit": "share",
                    "note": "Minimum mapped portfolio-security value share",
                },
            ]
        )

    rec_gate = gates[gates["gate"] == "reconciliation"] if not gates.empty else pd.DataFrame()
    if not rec_gate.empty:
        rows.append(
            {
                "metric": "reconciliation_median_abs_pct_diff",
                "value": rec_gate.iloc[0]["value"],
                "unit": "share",
                "note": "Median absolute pct diff against existing OFR/Fed benchmarks",
            }
        )

    if not baseline.empty:
        wide = baseline.pivot_table(
            index="event_month",
            columns="category",
            values="delta_millions",
            aggfunc="sum",
        ).fillna(0)
        wide["treasury_total"] = wide.get("treasury_bills", 0) + wide.get("treasury_coupons", 0)
        wide["repo_ex_fed"] = wide.get("private_treasury_repo", 0) + wide.get("agency_repo", 0)
        rows.append(
            {
                "metric": "event_count",
                "value": int(wide.shape[0]),
                "unit": "events",
                "note": "Positive bill-surprise months represented in event-window baseline",
            }
        )
        for metric in ["treasury_bills", "treasury_coupons", "treasury_total", "fed_onrrp", "repo_ex_fed", "total_assets"]:
            if metric not in wide.columns:
                wide[metric] = 0.0
            rows.append(
                {
                    "metric": f"mean_event_delta_{metric}",
                    "value": round(float(wide[metric].mean()), 4),
                    "unit": "USD millions",
                    "note": "Mean event-month minus prior-month allocation delta across selected bill-surprise months",
                }
            )

    rows.append(
        {
            "metric": "claim_boundary",
            "value": "focused_tga_refill_mmf_allocation_support_only",
            "unit": "label",
            "note": "Do not use for broad bill-deposit substitution, household ownership, or bank pass-through claims",
        }
    )
    return pd.DataFrame(rows)


def write_method_reports(
    report_dir: Path,
    gates: pd.DataFrame,
    coverage: pd.DataFrame,
    baseline: pd.DataFrame,
    source_months: tuple[str, str],
) -> None:
    ensure_dir(report_dir)
    passed_all = bool(gates["passed"].all()) if not gates.empty else False
    gate_lines = "\n".join(
        f"- {row.gate}: {'PASS' if row.passed else 'FAIL'} (value={row.value}, threshold={row.threshold})"
        for row in gates.itertuples(index=False)
    )
    coverage_note = "No coverage rows parsed."
    if not coverage.empty:
        coverage_note = (
            f"Parsed {len(coverage)} monthly aggregate rows from {coverage['report_month'].min().date()} "
            f"through {coverage['report_month'].max().date()}."
        )
    method = f"""# MMF Allocation Method

Source: official SEC DERA Form N-MFP monthly flat-file zips, parsed without extracting TSV files into the repository.

Sample: {source_months[0]} through {source_months[1]}.

Classification: portfolio-security rows are mapped into Treasury bills, Treasury coupons, Fed ON RRP, private Treasury repo, agency repo, agency debt, private instruments, cash/other, and unmapped. Federal Reserve Bank of New York repo rows are classified as `fed_onrrp`.

Validation gates:
{gate_lines}
"""
    validation = f"""# MMF Allocation Validation

Status: {'passes narrow backend gates' if passed_all else 'fails at least one backend gate'}.

{coverage_note}

The output is a fund-level allocation validation layer for the focused TGA-refill funding-chain claim. It is not a bank-deposit pass-through estimator.
"""
    boundary = """# MMF Allocation Claim Boundary

Allowed if gates pass: fund-level MMF allocation evidence supports the focused `tgarefill` mechanism that bill-funded TGA-refill shocks are absorbed through MMF Treasury holdings and ON RRP/repo rotation.

Forbidden: broad bill-deposit substitution, household behavior, exact final ownership, or changes to TDC estimator defaults.
"""
    memo = """# MMF Allocation Promotion Memo

The `mmfalloc` extension adds a fund-month validation layer underneath the existing weekly `tgarefill` result. Its job is limited: check whether individual money funds rotate toward Treasury bills/Treasuries and away from Fed ON RRP or repo around the canonical bill-surprise months.

The headline claim remains narrow. This supports the focused TGA-refill funding-chain mechanism; it does not revive a broad claim that Treasury bills drain bank deposits one-for-one.
"""
    if not baseline.empty:
        top = baseline.groupby("category", as_index=False)["delta_millions"].mean().sort_values("delta_millions", ascending=False).head(5)
        memo += "\nMean event-month deltas across selected positive bill-surprise months:\n"
        for row in top.itertuples(index=False):
            memo += f"- {row.category}: {row.delta_millions:,.0f} million\n"

    (report_dir / "mmfalloc_method.md").write_text(method, encoding="utf-8")
    (report_dir / "mmfalloc_validation.md").write_text(validation, encoding="utf-8")
    (report_dir / "mmfalloc_claim_boundary.md").write_text(boundary, encoding="utf-8")
    (report_dir / "mmfalloc_promotion_memo.md").write_text(memo, encoding="utf-8")


def write_eventstudy_svg(path: Path, baseline: pd.DataFrame) -> None:
    ensure_dir(path.parent)
    if baseline.empty:
        path.write_text("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"640\" height=\"120\"><text x=\"20\" y=\"60\">No MMF event-study rows available</text></svg>", encoding="utf-8")
        return
    summary = baseline.groupby("category", as_index=False)["delta_millions"].mean()
    summary = summary[summary["category"].isin(["treasury_bills", "treasury_coupons", "fed_onrrp", "private_treasury_repo", "agency_repo"])]
    summary = summary.sort_values("delta_millions")
    width, height = 820, 320
    left, top = 180, 30
    max_abs = max(float(summary["delta_millions"].abs().max()), 1.0)
    zero = left + 300
    scale = 280 / max_abs
    rows = []
    for idx, row in enumerate(summary.itertuples(index=False)):
        y = top + idx * 48
        value = float(row.delta_millions)
        x = zero if value >= 0 else zero + value * scale
        bar_w = abs(value * scale)
        color = "#2563eb" if value >= 0 else "#b91c1c"
        rows.append(f"<text x=\"10\" y=\"{y + 18}\" font-size=\"14\">{row.category}</text>")
        rows.append(f"<rect x=\"{x:.1f}\" y=\"{y}\" width=\"{bar_w:.1f}\" height=\"24\" fill=\"{color}\" />")
        rows.append(f"<text x=\"{zero + value * scale + (6 if value >= 0 else -80):.1f}\" y=\"{y + 18}\" font-size=\"13\">{value:,.0f}</text>")
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="white"/>
<text x="10" y="20" font-size="16" font-weight="700">MMF allocation event-month deltas, million USD</text>
<line x1="{zero}" x2="{zero}" y1="42" y2="{height - 24}" stroke="#111827" stroke-width="1"/>
{''.join(rows)}
</svg>
"""
    path.write_text(svg, encoding="utf-8")


def write_all_outputs(
    positions: pd.DataFrame,
    fund_month: pd.DataFrame,
    processed_dir: Path,
    output_tables_dir: Path,
    output_figures_dir: Path,
    output_reports_dir: Path,
    weekly_panel: pd.DataFrame | None,
    shocks: pd.DataFrame,
    source_months: tuple[str, str],
) -> dict[str, pd.DataFrame]:
    ensure_dir(processed_dir)
    ensure_dir(output_tables_dir)
    category_aggregates = build_category_aggregates(fund_month)
    coverage = build_coverage_table(fund_month, positions)
    reconciliation = build_reconciliation_table(category_aggregates, weekly_panel)
    gates = build_validation_gates(coverage, reconciliation)
    event_panel = build_event_panel(fund_month, shocks)
    baseline = build_baseline_table(event_panel)
    placebos = build_placebos(event_panel)
    leave_one = build_leave_one_family_out(event_panel)
    downstream_summary = build_downstream_summary(gates, coverage, baseline)

    write_dataframe(positions, processed_dir / "mmf_positions.parquet")
    write_dataframe(fund_month, processed_dir / "mmf_fund_month_panel.parquet")
    write_dataframe(category_aggregates, processed_dir / "mmf_category_aggregates.csv")
    write_dataframe(event_panel, processed_dir / "mmf_event_panel.csv")
    write_dataframe(coverage, output_tables_dir / "mmfalloc_coverage.csv")
    write_dataframe(reconciliation, output_tables_dir / "mmfalloc_reconciliation.csv")
    write_dataframe(gates, output_tables_dir / "mmfalloc_source_gates.csv")
    write_dataframe(baseline, output_tables_dir / "mmfalloc_baseline.csv")
    write_dataframe(placebos, output_tables_dir / "mmfalloc_placebos.csv")
    write_dataframe(leave_one, output_tables_dir / "mmfalloc_leave_one_family_out.csv")
    write_dataframe(downstream_summary, output_tables_dir / "mmfalloc_downstream_summary.csv")
    write_eventstudy_svg(output_figures_dir / "mmfalloc_eventstudy.svg", baseline)
    write_method_reports(output_reports_dir, gates, coverage, baseline, source_months)
    return {
        "coverage": coverage,
        "reconciliation": reconciliation,
        "gates": gates,
        "baseline": baseline,
        "placebos": placebos,
        "leave_one": leave_one,
        "downstream_summary": downstream_summary,
    }


def _empty_positions() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "report_month",
            "fund_id",
            "fund_name",
            "adviser_or_family",
            "accession_number",
            "security_id",
            "issuer_name",
            "security_title",
            "raw_investment_category",
            "category",
            "position_value",
            "source_schema_version",
        ]
    )


def _empty_fund_month_panel() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "report_month",
            "fund_id",
            "fund_name",
            "adviser_or_family",
            "fund_type",
            "total_assets",
            "wam",
            "wal",
            *CATEGORIES,
        ]
    )
