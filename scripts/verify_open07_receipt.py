"""Fail-closed verification for the thesis OPEN-07 producer receipt."""
from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RECEIPT_PATH = ROOT / "provenance" / "open07_tgarefill_producer_receipt.json"


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _git_blob(commit: str, path: str) -> bytes:
    result = subprocess.run(
        ["git", "show", f"{commit}:{path}"],
        cwd=ROOT,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        message = result.stderr.decode("utf-8", errors="replace").strip()
        raise ValueError(f"cannot read {path} at producer commit {commit}: {message}")
    return result.stdout


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _artifact(receipt: dict[str, Any], role: str) -> dict[str, Any]:
    matches = [item for item in receipt["retained_artifacts"] if item["role"] == role]
    if len(matches) != 1:
        raise ValueError(f"receipt must contain exactly one retained artifact for {role}")
    return matches[0]


def _check_hashes(receipt: dict[str, Any], errors: list[str]) -> None:
    commit = receipt["project"]["producer_commit"]
    for item in receipt["generating_surface"]:
        try:
            actual = _sha256_bytes(_git_blob(commit, item["path"]))
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if actual != item["sha256"]:
            errors.append(
                f"committed generating-surface hash mismatch: {item['path']} "
                f"expected {item['sha256']} got {actual}"
            )

    for group in ("inputs", "retained_artifacts"):
        for item in receipt[group]:
            path = ROOT / item["path"]
            if not path.is_file():
                errors.append(f"missing {group[:-1]}: {item['path']}")
                continue
            actual = _sha256_file(path)
            if actual != item["sha256"]:
                errors.append(
                    f"{group[:-1]} hash mismatch: {item['path']} "
                    f"expected {item['sha256']} got {actual}"
                )


def _check_csv_contracts(receipt: dict[str, Any], errors: list[str]) -> None:
    shock = _artifact(receipt, "canonical bill-supply shock")
    shock_rows = _read_csv(ROOT / shock["path"])
    if len(shock_rows) != shock["rows"]:
        errors.append(f"canonical shock row count mismatch: {len(shock_rows)}")
    if not shock_rows:
        errors.append("canonical shock is empty")
    else:
        required = {
            "date",
            "bill_size_surprise",
            "bill_size_surprise_announcement_week",
            "tax_receipt_surprise",
            "rapid_rebuild_flag",
            "regime",
            "rmp_regime",
            "sample",
            "canonical_sample_end",
        }
        missing = required.difference(shock_rows[0])
        if missing:
            errors.append(f"canonical shock missing columns: {sorted(missing)}")
        dates = [row["date"] for row in shock_rows]
        if min(dates) != shock["first_date"] or max(dates) != shock["last_date"]:
            errors.append("canonical shock date range mismatch")
        if {row["sample"] for row in shock_rows} != {
            "canonical_pre_rmp_through_2025_11"
        }:
            errors.append("canonical shock sample label mismatch")
        if {row["canonical_sample_end"] for row in shock_rows} != {"2025-11-30"}:
            errors.append("canonical shock cutoff mismatch")

    lp = _artifact(receipt, "local-projection output")
    lp_rows = _read_csv(ROOT / lp["path"])
    if len(lp_rows) != lp["rows"]:
        errors.append(f"LP row count mismatch: {len(lp_rows)}")
    spec_counts = Counter(row["shock_spec"] for row in lp_rows)
    if dict(spec_counts) != lp["row_count_by_shock_spec"]:
        errors.append(f"LP shock-spec counts mismatch: {dict(spec_counts)}")
    if {row["canonical_sample_end"] for row in lp_rows} != {"2025-11-30"}:
        errors.append("LP canonical cutoff mismatch")
    if {row["rmp_regime_start"] for row in lp_rows} != {"2025-12-01"}:
        errors.append("LP RMP-regime start mismatch")

    promotion = _artifact(receipt, "promoted robustness summary")
    promotion_rows = _read_csv(ROOT / promotion["path"])
    if len(promotion_rows) != promotion["rows"]:
        errors.append(f"promotion-summary row count mismatch: {len(promotion_rows)}")
    if {row["spec"] for row in promotion_rows} != set(promotion["specs"]):
        errors.append("promotion-summary spec set mismatch")
    canonical = {
        row["response_var"]: row
        for row in promotion_rows
        if row["spec"] == "canonical_issue_week"
    }
    for expected in receipt["promoted_canonical_h4"]:
        actual = canonical.get(expected["response_var"])
        if actual is None:
            errors.append(f"missing promoted response: {expected['response_var']}")
            continue
        for field in ("shock_sd_bn", "h4_effect_bn", "h4_t_stat_nw"):
            if float(actual[field]) != float(expected[field]):
                errors.append(
                    f"promoted {field} mismatch for {expected['response_var']}: "
                    f"expected {expected[field]} got {actual[field]}"
                )
        actual_sig = actual["h4_significant_5pct"].strip().lower() == "true"
        if actual_sig is not expected["h4_significant_5pct"]:
            errors.append(
                f"promoted significance mismatch for {expected['response_var']}"
            )


def _check_claim_boundary(receipt: dict[str, Any], errors: list[str]) -> None:
    boundary = receipt.get("claim_boundary", {})
    if boundary.get("status") != "aggregate_funding_association_only":
        errors.append("claim boundary status is not aggregate_funding_association_only")
    required_exclusions = {
        "household behavior",
        "a general bill-deposit substitution law",
        "exact final-owner tracing",
        "bank-deposit pass-through",
        "an unrestricted causal mechanism",
    }
    if set(boundary.get("excluded", [])) != required_exclusions:
        errors.append("claim-boundary exclusions mismatch")


def main() -> int:
    errors: list[str] = []
    if not RECEIPT_PATH.is_file():
        print(f"FAIL: missing receipt {RECEIPT_PATH.relative_to(ROOT)}", file=sys.stderr)
        return 1
    try:
        receipt = json.loads(RECEIPT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"FAIL: unreadable receipt: {exc}", file=sys.stderr)
        return 1

    if receipt.get("schema_version") != "tgarefill.open07.producer-receipt.v1":
        errors.append("unsupported receipt schema")
    if receipt.get("generation", {}).get("command") != (
        "python -B scripts/build_auction_shock_lp.py"
    ):
        errors.append("generation command mismatch")

    _check_hashes(receipt, errors)
    _check_csv_contracts(receipt, errors)
    _check_claim_boundary(receipt, errors)

    if errors:
        for error in errors:
            print(f"FAIL: {error}", file=sys.stderr)
        return 1

    result = {
        "status": "pass",
        "producer_commit": receipt["project"]["producer_commit"],
        "receipt": str(RECEIPT_PATH.relative_to(ROOT)).replace("\\", "/"),
        "receipt_sha256": _sha256_file(RECEIPT_PATH),
        "verified_inputs": len(receipt["inputs"]),
        "verified_artifacts": len(receipt["retained_artifacts"]),
        "verified_generating_surface_files": len(receipt["generating_surface"]),
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
