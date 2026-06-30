#!/usr/bin/env python3
"""Independent file/control verification for weekly CEO report artifacts."""
from __future__ import annotations

import argparse
import json
import zipfile
from pathlib import Path

import pandas as pd


def check(checks: list[dict], name: str, ok: bool, details: str = "") -> None:
    checks.append({"name": name, "status": "pass" if ok else "fail", "details": details})


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
    html_path = base / f"email_body_{week_end}.html"
    xlsx_path = base / f"Mellanni_Weekly_CEO_Overview_{week_end}.xlsx"

    ver = json.loads(ver_path.read_text(encoding="utf-8"))
    ctrl = pd.read_csv(ctrl_path).iloc[0]
    html = html_path.read_text(encoding="utf-8")
    metrics = ver["metrics"]

    check(checks, "workbook_zipfile", zipfile.is_zipfile(xlsx_path), str(xlsx_path))
    check(checks, "html_has_prepared_band", "Prepared by Sergey's AI helper." in html)
    check(checks, "html_has_top_promo_section", "Top promo discounts" in html)
    check(checks, "html_has_top_asin_table", "Top ASINs" in html and "B00NLLUMOE" in html and "See attached workbook for full Top ASINs table" not in html, "CEO body must include visible Top ASIN table, not only a workbook pointer")
    check(checks, "html_uses_ld_not_unk", "UNK" not in html, "UNK absent from CEO HTML")
    check(checks, "html_pp_columns_formatted", " pp" in html and "0.0018849879539747" not in html and "0.006745349644595826" not in html, "percentage-point columns should not show raw decimal fractions")
    conversion_phrase_ok = (metrics["conversion"] > metrics["prior_conversion"] and "conversion improved to" in html) or (metrics["conversion"] < metrics["prior_conversion"] and ("conversion declined to" in html or "conversion fell to" in html)) or (metrics["conversion"] == metrics["prior_conversion"] and "conversion held at" in html)
    sessions_phrase_ok = (metrics["sessions"] > metrics["prior_sessions"] and "Sessions rose to" in html) or (metrics["sessions"] < metrics["prior_sessions"] and "Sessions fell to" in html) or (metrics["sessions"] == metrics["prior_sessions"] and "Sessions held at" in html)
    check(checks, "html_narrative_signs_match_metrics", conversion_phrase_ok and sessions_phrase_ok, "bullet wording must match metric direction")
    check(checks, "br_gross_matches_control", abs(metrics["gross_sales"] - float(ctrl["br_current_gross_sales"])) < 0.01)
    check(checks, "sessions_match_control", abs(metrics["sessions"] - float(ctrl["br_current_sessions"])) < 0.01)
    check(checks, "units_match_control", abs(metrics["units"] - float(ctrl["br_current_units"])) < 0.01)
    check(checks, "conversion_matches_control", abs(metrics["conversion"] - float(ctrl["br_current_conversion"])) < 1e-12)
    check(checks, "net_item_sales_matches_control", abs(metrics["net_item_sales"] - float(ctrl["ao_current_net_item_sales"])) < 0.01)
    check(checks, "item_promo_discount_matches_control", abs(metrics["item_promo_discount"] - float(ctrl["ao_current_item_promo_discount"])) < 0.01)

    products = pd.read_csv(base / "product_performance.csv")
    asins = pd.read_csv(base / "asin_performance_size_color.csv")
    keywords = pd.read_csv(base / "tracked_sqp_keywords.csv")
    promos = pd.read_csv(base / "top_promo_discounts.csv")
    check(checks, "product_sales_sum_matches_br", abs(products["sales"].sum() - float(ctrl["br_current_gross_sales"])) < 0.05)
    check(checks, "asin_sales_sum_matches_br", abs(asins["sales"].sum() - float(ctrl["br_current_gross_sales"])) < 0.05)
    check(checks, "top_asin_size_color_non_numeric", not asins.head(25)["size"].astype(str).str.fullmatch(r"\d+(\.\d+)?").any())
    check(checks, "keyword_rows_present", len(keywords) >= 15, f"rows={len(keywords)}")
    check(checks, "promo_rows_present", len(promos) > 0, f"rows={len(promos)}")

    status = "pass" if all(c["status"] == "pass" for c in checks) else "fail"
    out = {"status": status, "checks": checks, "control_file": str(ctrl_path)}
    (base / f"independent_verification_{week_end}.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(status)
    return 0 if status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
