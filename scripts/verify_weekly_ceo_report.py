#!/usr/bin/env python3
"""Independent file/control verification for weekly CEO report artifacts."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import zipfile
from datetime import datetime, timedelta
import re
from pathlib import Path

import pandas as pd

from report_storage import require_tmp_artifact_path


def require_tmp(path: Path) -> Path:
    return require_tmp_artifact_path(path, label="Weekly report artifacts", error_type=SystemExit)


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
    base = require_tmp(args.report_dir)
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
        actual_raw_sha = sha256_file(account_raw_path)
        provider_sha = manifest.get("provider_sha256")
        provider_sha_ok = (provider_sha == actual_raw_sha) if provider_sha else True
        hashes_ok = (
            manifest.get("raw_sha256") == actual_raw_sha
            and provider_sha_ok
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

    products = pd.read_csv(base / "product_performance.csv")
    asins = pd.read_csv(base / "asin_performance_size_color.csv").sort_values("sales", ascending=False)
    keywords = pd.read_csv(base / "tracked_sqp_keywords.csv")
    h10_path = base / "h10" / "h10_keyword_candidates.csv"
    h10_keywords = pd.read_csv(h10_path) if h10_path.exists() else pd.DataFrame()
    promos = pd.read_csv(base / "top_promo_discounts.csv")

    raw_asin_json = base / "sources" / "asin.json"
    raw_net_json = base / "sources" / "net.json"
    if raw_asin_json.is_file() and raw_net_json.is_file():
        asin_rows_raw = json.loads(raw_asin_json.read_text(encoding="utf-8")).get("rows") or []
        net_rows_raw = json.loads(raw_net_json.read_text(encoding="utf-8")).get("rows") or []
        net_cur_raw = next((r for r in net_rows_raw if str(r.get("period")) == "current"), None)
        ctrl_br_gross = sum(float(r.get("sales") or 0) for r in asin_rows_raw)
        ctrl_ao_gross = float(net_cur_raw["gross_item_sales"]) if net_cur_raw else float(ctrl["ao_current_gross_item_sales"])
        ctrl_ao_net = float(net_cur_raw["net_item_sales"]) if net_cur_raw else float(ctrl["ao_current_net_item_sales"])
        ctrl_ao_promo = float(net_cur_raw["item_promo_discount"]) if net_cur_raw else float(ctrl["ao_current_item_promo_discount"])
        control_mode = "raw_sources"
    else:
        ctrl_br_gross = float(ctrl["br_current_gross_sales"])
        ctrl_ao_gross = float(ctrl["ao_current_gross_item_sales"])
        ctrl_ao_net = float(ctrl["ao_current_net_item_sales"])
        ctrl_ao_promo = float(ctrl["ao_current_item_promo_discount"])
        control_mode = "independent_control_totals.csv"

    check(checks, "workbook_zipfile", zipfile.is_zipfile(xlsx_path), str(xlsx_path))
    check(checks, "html_has_prepared_band", "Prepared by Sergey's AI helper." in html)
    check(checks, "html_has_top_promo_section", "Top promo discounts" in html)
    top_asin = str(asins.iloc[0]["asin"]) if len(asins) else ""
    check(checks, "html_has_top_asin_table", "Top ASINs" in html and (not top_asin or top_asin in html) and "See attached workbook for full Top ASINs table" not in html, "CEO body must include visible Top ASIN table, not only a workbook pointer")
    check(checks, "html_uses_ld_not_unk", "UNK" not in html, "UNK absent from CEO HTML")
    raw_fraction_cell = re.search(r"<td\b[^>]*>\s*[+-]?0\.\d{6,}\s*</td>", html)
    check(checks, "html_pp_columns_formatted", " pp" in html and raw_fraction_cell is None, "percentage-point columns should not show raw decimal fractions")
    conversion_phrase_ok = (metrics["conversion"] > metrics["prior_conversion"] and "conversion improved to" in html) or (metrics["conversion"] < metrics["prior_conversion"] and ("conversion declined to" in html or "conversion fell to" in html)) or (metrics["conversion"] == metrics["prior_conversion"] and "conversion held at" in html)
    sessions_phrase_ok = (metrics["sessions"] > metrics["prior_sessions"] and "Sessions rose to" in html) or (metrics["sessions"] < metrics["prior_sessions"] and "Sessions fell to" in html) or (metrics["sessions"] == metrics["prior_sessions"] and "Sessions held at" in html)
    check(checks, "html_narrative_signs_match_metrics", conversion_phrase_ok and sessions_phrase_ok, "bullet wording must match metric direction")
    check(checks, "gross_sales_kpi_matches_all_orders", abs(metrics["gross_sales"] - ctrl_ao_gross) < 0.01, f"mode={control_mode}; Gross Sales KPI must come from all-orders gross_item_sales")
    check(checks, "br_gross_control_matches_control", "br_gross_sales" in metrics and abs(float(metrics["br_gross_sales"]) - ctrl_br_gross) < 0.01, f"mode={control_mode}; Business Report gross is control-only")
    check(checks, "gross_sales_gte_net_item_sales", metrics["gross_sales"] + 0.01 >= metrics["net_item_sales"], f"gross={metrics['gross_sales']:,.2f}; net={metrics['net_item_sales']:,.2f}")
    check(checks, "net_item_sales_formula", abs((metrics["gross_sales"] - metrics["item_promo_discount"]) - metrics["net_item_sales"]) < 0.05, "net = all-orders gross - item promo discount")
    check(checks, "sessions_match_account_control", current_integral and abs(metrics["sessions"] - account_sessions) < 0.01, f"account={account_sessions}; headline={metrics['sessions']}")
    check(checks, "units_match_account_control", current_integral and abs(metrics["units"] - account_units) < 0.01, f"account={account_units}; headline={metrics['units']}")
    check(checks, "conversion_matches_account_control", current_integral and math.isclose(float(metrics["conversion"]), account_conversion, rel_tol=0.0, abs_tol=1e-12), f"account={account_conversion}; headline={metrics['conversion']}")
    check(checks, "net_item_sales_matches_control", abs(metrics["net_item_sales"] - ctrl_ao_net) < 0.01, f"mode={control_mode}")
    check(checks, "item_promo_discount_matches_control", abs(metrics["item_promo_discount"] - ctrl_ao_promo) < 0.01, f"mode={control_mode}")

    check(checks, "product_sales_sum_matches_br", abs(products["sales"].sum() - ctrl_br_gross) < 0.05, f"mode={control_mode}")
    check(checks, "asin_sales_sum_matches_br", abs(asins["sales"].sum() - ctrl_br_gross) < 0.05, f"mode={control_mode}")
    check(checks, "top_asin_size_color_non_numeric", not asins.head(25)["size"].astype(str).str.fullmatch(r"\d+(\.\d+)?").any())
    check(checks, "tracked_sqp_rows_present", len(keywords) > 0, f"rows={len(keywords)}")
    check(checks, "h10_keyword_rows_present", len(h10_keywords) > 0 and len(h10_keywords) == len(keywords), f"h10={len(h10_keywords)}; tracked_sqp={len(keywords)}")
    scope_path = base / "sources" / "keyword-scope.json"
    if scope_path.is_file():
        scope_raw = json.loads(scope_path.read_text(encoding="utf-8"))
        scoped_pairs = scope_raw.get("pairs") if isinstance(scope_raw, dict) else None
        scoped_n = len(scope_raw.get("phrases") or scoped_pairs or []) if isinstance(scope_raw, dict) else len(scope_raw)
        check(checks, "keyword_scope_rows_match", len(h10_keywords) == scoped_n, f"h10={len(h10_keywords)}; scoped={scoped_n}")
        tracked_files = sorted((base / "sources").glob("tracked-*.json"))
        sqp_json_path = base / "sources" / "sqp.json"
        if raw_asin_json.is_file() and sqp_json_path.is_file() and tracked_files:
            has_scope_meta = (
                isinstance(scope_raw, dict)
                and isinstance(scoped_pairs, list)
                and isinstance(scope_raw.get("top_selling_asins"), int)
                and scope_raw["top_selling_asins"] > 0
                and isinstance(scope_raw.get("tracked_asins_by_sales"), list)
                and isinstance(scope_raw.get("untracked_top_selling_asins"), list)
                and isinstance(scope_raw.get("limit"), int)
                and scope_raw["limit"] > 0
            )
            if not has_scope_meta:
                check(checks, "keyword_scope_recomputed_match", False, "scope file missing top_selling_asins, tracked_asins_by_sales, untracked_top_selling_asins, limit, or pairs")
            else:
                sqp_raw = json.loads(sqp_json_path.read_text(encoding="utf-8")).get("rows") or []
                sqp_now = {
                    str(r["search_query"]).strip().lower(): float(r.get("total_purchases") or 0) * float(r.get("purchase_price_median") or 0)
                    for r in sqp_raw if str(r.get("reporting_date")) == week_end and r.get("search_query")
                }
                asin_raw_rows = json.loads(raw_asin_json.read_text(encoding="utf-8")).get("rows") or []
                tracked_map: dict[tuple[str, str], dict] = {}
                for tp in tracked_files:
                    tp_payload = json.loads(tp.read_text(encoding="utf-8"))
                    for tr in (tp_payload.get("data") or tp_payload).get("rows") or []:
                        tracked_map.setdefault((str(tr["asin"]).strip(), str(tr["keywords_phrase"]).strip().lower()), tr)
                tracked_asins = {ra for (ra, _) in tracked_map}
                seen_top: set[str] = set()
                top_asins: list[str] = []
                for r in sorted(asin_raw_rows, key=lambda r: (-float(r.get("sales") or 0), str(r.get("asin") or ""))):
                    a = str(r.get("asin") or "").strip()
                    if a and a not in seen_top:
                        seen_top.add(a)
                        top_asins.append(a)
                        if len(top_asins) >= int(scope_raw["top_selling_asins"]):
                            break
                ordered_asins = [a for a in top_asins if a in tracked_asins]
                expected_untracked = [a for a in top_asins if a not in tracked_asins]
                rebuilt_map: dict[str, tuple[str, str, float, float, float]] = {}
                for a in ordered_asins:
                    for (ra, ph), tr in tracked_map.items():
                        if ra == a and ph in sqp_now and ph not in rebuilt_map:
                            rebuilt_map[ph] = (a, ph, sqp_now[ph], float(tr.get("keyword_sales") or 0), float(tr.get("search_volume") or 0))
                expected_pairs = [{"asin": a, "phrase": ph} for a, ph, *_ in sorted(rebuilt_map.values(), key=lambda x: (-x[2], -x[3], -x[4], x[1]))[:int(scope_raw["limit"])]]
                actual_pairs = [{"asin": str(p["asin"]).strip(), "phrase": str(p["phrase"]).strip().lower()} for p in scoped_pairs]
                asins_match = [str(a).strip() for a in scope_raw["tracked_asins_by_sales"]] == ordered_asins
                untracked_match = [str(a).strip() for a in scope_raw["untracked_top_selling_asins"]] == expected_untracked
                check(checks, "keyword_scope_recomputed_match", asins_match and untracked_match and actual_pairs == expected_pairs, f"asins_match={asins_match}; untracked_match={untracked_match}; expected={len(expected_pairs)}; actual={len(actual_pairs)}")
    check(checks, "html_has_h10_keyword_section", "H10 keyword standings with SQP diagnostics" in html and "Tracked SQP keyword standings" not in html and "H10 keyword intelligence — degraded" not in html, "H10 is mandatory; SQP/manual fallback section is invalid")
    prior_h10_path = base / "h10" / "h10_prior_keyword_candidates.csv"
    h10_movement_required = prior_h10_path.exists()
    h10_movement_labels_present = all(label in html for label in [
        "H10 keyword standings with SQP diagnostics",
        "H10 Kw Sales",
        "Org. Rank",
        "Prior Org. Rank",
        "Rank Change",
        "rank_movement = prior_position - current_position",
        "green ↓ N",
        "red ↑ N",
    ])
    check(checks, "html_h10_movement_labeled", (not h10_movement_required) or h10_movement_labels_present, "If prior H10 exists, CEO chart must label keyword-sales estimate, organic rank, and exact signed rank change")
    keyword_table_count = len(re.findall(r"<th\b[^>]*>\s*Keyword\s*</th>", html, re.IGNORECASE))
    check(checks, "exactly_one_keyword_table", keyword_table_count == 1, f"tables={keyword_table_count}")
    check(checks, "promo_rows_present", len(promos) > 0, f"rows={len(promos)}")

    status = "pass" if all(c["status"] == "pass" for c in checks) else "fail"
    out = {"status": status, "checks": checks, "account_source": str(account_path), "control_file": str(ctrl_path)}
    (base / f"independent_verification_{week_end}.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(status)
    return 0 if status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
