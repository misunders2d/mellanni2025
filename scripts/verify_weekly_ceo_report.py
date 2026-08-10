#!/usr/bin/env python3
"""Independent file/control verification for weekly CEO report artifacts."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


def check(checks: list[dict], name: str, ok: bool, details: str = "") -> None:
    checks.append({"name": name, "status": "pass" if ok else "fail", "details": details})


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--report-dir", type=Path, required=True)
    p.add_argument("--week-end", required=True)
    args = p.parse_args()
    base = args.report_dir.resolve()
    week_end = args.week_end
    checks: list[dict] = []

    ver_path = base / f"verification_summary_{week_end}.json"
    ctrl_path = base / "independent_control_totals.csv"
    account_path = base / "source_data_kiosk_account_by_date.csv"
    account_raw_path = base / "source_data_kiosk_account_by_date_raw.jsonl"
    account_manifest_path = base / "source_data_kiosk_account_by_date_manifest.json"
    html_path = base / f"email_body_{week_end}.html"
    xlsx_path = base / f"Mellanni_Weekly_CEO_Overview_{week_end}.xlsx"

    ver = json.loads(ver_path.read_text(encoding="utf-8"))
    ctrl = pd.read_csv(ctrl_path).iloc[0]
    html = html_path.read_text(encoding="utf-8")
    metrics = ver["metrics"]

    check(checks, "account_by_date_source_present", account_path.exists(), str(account_path))
    if not account_path.exists():
        print("fail")
        return 1
    account = pd.read_csv(account_path)
    required_account_columns = {"date", "sales", "orders", "units", "sessions", "unit_conversion"}
    missing_account_columns = sorted(required_account_columns - set(account.columns))
    check(checks, "account_by_date_columns", not missing_account_columns, ", ".join(missing_account_columns) if missing_account_columns else "ok")
    if missing_account_columns:
        print("fail")
        return 1
    account["date"] = pd.to_datetime(account["date"], errors="coerce").dt.date
    for col in ["sales", "orders", "units", "sessions", "unit_conversion"]:
        account[col] = pd.to_numeric(account[col], errors="coerce")
    end = datetime.strptime(week_end, "%Y-%m-%d").date()
    start = end - timedelta(days=6)
    prior_end = start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=6)

    provenance_files_present = account_raw_path.exists() and account_manifest_path.exists()
    check(checks, "account_source_provenance_files", provenance_files_present, "raw document and manifest required")
    if provenance_files_present:
        manifest = json.loads(account_manifest_path.read_text(encoding="utf-8"))
        required_manifest = {
            "source": "SP-API Data Kiosk",
            "dataset": "analytics_salesAndTraffic_2024_04_24.salesAndTrafficByDate",
            "aggregate_by": "DAY",
            "marketplace_id": "ATVPDKIKX0DER",
            "start_date": str(prior_start),
            "end_date": str(end),
            "raw_file": account_raw_path.name,
            "normalized_file": account_path.name,
            "row_count": 14,
        }
        metadata_ok = all(manifest.get(k) == v for k, v in required_manifest.items())
        hashes_ok = (
            manifest.get("raw_sha256") == sha256_file(account_raw_path)
            and manifest.get("normalized_sha256") == sha256_file(account_path)
        )
        try:
            raw_rows = [json.loads(line) for line in account_raw_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            expected_dates = {str(prior_start + timedelta(days=i)) for i in range(14)}
            by_date_shape = (
                len(raw_rows) == 14
                and {str(row.get("startDate")) for row in raw_rows} == expected_dates
                and all(
                    "startDate" in row and row.get("endDate") == row.get("startDate")
                    and row.get("marketplaceId") == "ATVPDKIKX0DER"
                    and "childAsin" not in row and "sku" not in row
                    and "sales" in row and "traffic" in row for row in raw_rows
                )
            )
            expected = pd.DataFrame([{
                "date": row["startDate"],
                "sales": row["sales"]["orderedProductSales"]["amount"],
                "orders": row["sales"]["totalOrderItems"],
                "units": row["sales"]["unitsOrdered"],
                "sessions": row["traffic"]["sessions"],
                "unit_conversion": row["traffic"]["unitSessionPercentage"] / 100,
            } for row in raw_rows]).sort_values("date").reset_index(drop=True)
            normalized = pd.read_csv(account_path).sort_values("date").reset_index(drop=True)[expected.columns]
            normalized_matches_raw = by_date_shape and len(normalized) == len(expected)
            for col in ["sales", "orders", "units", "sessions", "unit_conversion"]:
                normalized_matches_raw = normalized_matches_raw and all(
                    math.isclose(float(a), float(b), rel_tol=0.0, abs_tol=1e-12)
                    for a, b in zip(normalized[col], expected[col])
                )
            normalized_matches_raw = normalized_matches_raw and normalized["date"].astype(str).equals(expected["date"].astype(str))
        except Exception:
            by_date_shape = False
            normalized_matches_raw = False
        provenance_ok = metadata_ok and bool(manifest.get("query_id")) and bool(manifest.get("document_id")) and hashes_ok and by_date_shape and normalized_matches_raw
    else:
        provenance_ok = False
        by_date_shape = False
        normalized_matches_raw = False
    check(checks, "account_source_provenance", provenance_ok, f"metadata/hash/by-date shape/raw normalization must match; shape={by_date_shape}; normalized={normalized_matches_raw}")

    def account_totals(period_start, period_end):
        rows = account[account["date"].between(period_start, period_end)]
        units = float(rows["units"].sum())
        sessions = float(rows["sessions"].sum())
        daily_integral = all(
            math.isfinite(float(value)) and float(value).is_integer()
            for value in pd.concat([rows["units"], rows["sessions"]])
        )
        integral = daily_integral and math.isfinite(units) and math.isfinite(sessions) and units.is_integer() and sessions.is_integer()
        conversion = units / sessions if integral and sessions else math.nan
        return rows, units, sessions, conversion, integral

    current_rows, account_units, account_sessions, account_conversion, current_integral = account_totals(start, end)
    prior_rows, prior_units, prior_sessions, prior_conversion, prior_integral = account_totals(prior_start, prior_end)
    check(checks, "account_by_date_current_coverage", len(current_rows) == 7 and current_rows["date"].nunique() == 7, f"rows={len(current_rows)}")
    check(checks, "account_by_date_prior_coverage", len(prior_rows) == 7 and prior_rows["date"].nunique() == 7, f"rows={len(prior_rows)}")
    check(checks, "account_counts_integral", current_integral and prior_integral, f"current={account_units}/{account_sessions}; prior={prior_units}/{prior_sessions}")
    check(checks, "headline_units_match_account", current_integral and float(metrics["units"]) == account_units, f"headline={metrics['units']}; account={account_units}")
    check(checks, "headline_sessions_match_account", current_integral and float(metrics["sessions"]) == account_sessions, f"headline={metrics['sessions']}; account={account_sessions}")
    check(checks, "headline_conversion_matches_account", current_integral and math.isclose(float(metrics["conversion"]), account_conversion, rel_tol=0.0, abs_tol=1e-12), f"headline={metrics['conversion']}; account={account_conversion}")
    check(checks, "prior_units_match_account", prior_integral and float(metrics["prior_units"]) == prior_units, f"headline={metrics['prior_units']}; account={prior_units}")
    check(checks, "prior_sessions_match_account", prior_integral and float(metrics["prior_sessions"]) == prior_sessions, f"headline={metrics['prior_sessions']}; account={prior_sessions}")
    check(checks, "prior_conversion_matches_account", prior_integral and math.isclose(float(metrics["prior_conversion"]), prior_conversion, rel_tol=0.0, abs_tol=1e-12), f"headline={metrics['prior_conversion']}; account={prior_conversion}")

    check(checks, "workbook_zipfile", zipfile.is_zipfile(xlsx_path), str(xlsx_path))
    check(checks, "html_has_prepared_band", "Prepared by Sergey's AI helper." in html)
    check(checks, "html_has_top_promo_section", "Top promo discounts" in html)
    check(checks, "html_has_top_asin_table", "Top ASINs" in html and "B00NLLUMOE" in html and "See attached workbook for full Top ASINs table" not in html, "CEO body must include visible Top ASIN table, not only a workbook pointer")
    check(checks, "html_uses_ld_not_unk", "UNK" not in html, "UNK absent from CEO HTML")
    check(checks, "html_pp_columns_formatted", " pp" in html and "0.0018849879539747" not in html and "0.006745349644595826" not in html, "percentage-point columns should not show raw decimal fractions")
    conversion_phrase_ok = (metrics["conversion"] > metrics["prior_conversion"] and "conversion improved to" in html) or (metrics["conversion"] < metrics["prior_conversion"] and ("conversion declined to" in html or "conversion fell to" in html)) or (metrics["conversion"] == metrics["prior_conversion"] and "conversion held at" in html)
    sessions_phrase_ok = (metrics["sessions"] > metrics["prior_sessions"] and "Sessions rose to" in html) or (metrics["sessions"] < metrics["prior_sessions"] and "Sessions fell to" in html) or (metrics["sessions"] == metrics["prior_sessions"] and "Sessions held at" in html)
    check(checks, "html_narrative_signs_match_metrics", conversion_phrase_ok and sessions_phrase_ok, "bullet wording must match metric direction")
    check(checks, "gross_sales_kpi_matches_all_orders", abs(metrics["gross_sales"] - float(ctrl["ao_current_gross_item_sales"])) < 0.01, "Gross Sales KPI must come from all-orders gross_item_sales")
    check(checks, "br_gross_control_matches_control", abs(float(metrics.get("br_gross_sales", ctrl["br_current_gross_sales"])) - float(ctrl["br_current_gross_sales"])) < 0.01, "Business Report gross is control-only")
    check(checks, "gross_sales_gte_net_item_sales", metrics["gross_sales"] + 0.01 >= metrics["net_item_sales"], f"gross={metrics['gross_sales']:,.2f}; net={metrics['net_item_sales']:,.2f}")
    check(checks, "net_item_sales_formula", abs((metrics["gross_sales"] - metrics["item_promo_discount"]) - metrics["net_item_sales"]) < 0.05, "net = all-orders gross - item promo discount")
    check(checks, "sessions_match_control", abs(metrics["sessions"] - float(ctrl["br_current_sessions"])) < 0.01)
    check(checks, "units_match_control", abs(metrics["units"] - float(ctrl["br_current_units"])) < 0.01)
    check(checks, "conversion_matches_control", abs(metrics["conversion"] - float(ctrl["br_current_conversion"])) < 1e-12)
    check(checks, "net_item_sales_matches_control", abs(metrics["net_item_sales"] - float(ctrl["ao_current_net_item_sales"])) < 0.01)
    check(checks, "item_promo_discount_matches_control", abs(metrics["item_promo_discount"] - float(ctrl["ao_current_item_promo_discount"])) < 0.01)

    products = pd.read_csv(base / "product_performance.csv")
    asins = pd.read_csv(base / "asin_performance_size_color.csv")
    keywords = pd.read_csv(base / "tracked_sqp_keywords.csv")
    h10_path = base / "h10" / "h10_keyword_candidates.csv"
    h10_keywords = pd.read_csv(h10_path) if h10_path.exists() else pd.DataFrame()
    promos = pd.read_csv(base / "top_promo_discounts.csv")
    check(checks, "product_sales_sum_matches_br", abs(products["sales"].sum() - float(ctrl["br_current_gross_sales"])) < 0.05)
    check(checks, "asin_sales_sum_matches_br", abs(asins["sales"].sum() - float(ctrl["br_current_gross_sales"])) < 0.05)
    check(checks, "top_asin_size_color_non_numeric", not asins.head(25)["size"].astype(str).str.fullmatch(r"\d+(\.\d+)?").any())
    check(checks, "legacy_keyword_rows_present", len(keywords) >= 15, f"rows={len(keywords)}")
    check(checks, "h10_keyword_rows_present", len(h10_keywords) >= 15, f"rows={len(h10_keywords)}")
    check(checks, "html_has_h10_keyword_section", "H10 keyword standings with SQP diagnostics" in html and "Tracked SQP keyword standings" not in html and "H10 keyword intelligence — degraded" not in html, "H10 is mandatory; SQP/manual fallback section is invalid")
    prior_h10_path = base / "h10" / "h10_prior_keyword_candidates.csv"
    h10_movement_required = prior_h10_path.exists()
    h10_movement_labels_present = all(label in html for label in [
        "H10 keyword demand / rank movement",
        "Relative H10 keyword-sales estimate",
        "H10 est. weekly keyword sales",
        "Best organic rank",
        "Rank change vs prior H10",
        "rank_movement = prior_position - current_position",
        "green ↓ N",
        "red ↑ N",
    ])
    check(checks, "html_h10_movement_labeled", (not h10_movement_required) or h10_movement_labels_present, "If prior H10 exists, CEO chart must label keyword-sales estimate, organic rank, and exact signed rank change")
    check(checks, "promo_rows_present", len(promos) > 0, f"rows={len(promos)}")

    status = "pass" if all(c["status"] == "pass" for c in checks) else "fail"
    out = {"status": status, "checks": checks, "account_source": str(account_path), "control_file": str(ctrl_path)}
    (base / f"independent_verification_{week_end}.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(status)
    return 0 if status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
