#!/usr/bin/env python3
"""Build Mellanni daily sales report from temporary CSV extracts.

This script is deterministic: it does not call SP-API, BigQuery, Google Sheets,
or Gmail. First place source extracts in a work directory under /tmp, then run
this builder to produce the HTML body, subject, comparison CSV, and verification JSON.

Default input filenames in --report-dir:
  all_orders_us_filtered_sanitized.csv       target SP-API all-orders rows
  dictionary_full_asin_collection.csv        full dictionary ASIN->collection
  bigquery_collection_breakdown_current_prior.csv  optional prior fallback rows
  ppc_hourly_current_prior.csv               hourly Sheet rows for target/prior

Outputs stay in the same /tmp work directory until manual deletion.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from html import escape
from pathlib import Path
from typing import Any

import pandas as pd

from report_storage import require_tmp_artifact_path

DEFAULT_COMPETITOR_CONFIG = Path(__file__).parents[1] / "report_configs/daily_sales_competitors.json"
REQUIRED_COMPETITOR_BRANDS = {"Amazon Basics", "Utopia Bedding", "CGK Unlimited", "Bedsure"}
COMPETITOR_MAX_AGE_MINUTES = 60
COMPETITOR_SOURCES = {"Keepa product cache", "Keepa deal snapshot (Mellanni MCP)"}
DEFAULT_PROMOTION_NAMES = Path(__file__).parents[1] / "config/promotion_names.json"
COLLECTION_ROWS_SHOWN = 8


@dataclass
class Check:
    name: str
    status: str
    details: str = ""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--report-dir", type=Path, required=True)
    p.add_argument("--target-date", required=True, help="Pacific date YYYY-MM-DD")
    p.add_argument("--prior-date", help="Pacific date YYYY-MM-DD; defaults to target minus 7 days")
    p.add_argument("--target-orders", type=Path, help="Target all-orders CSV; default from report dir")
    p.add_argument("--prior-orders", type=Path, help="Optional prior all-orders CSV")
    p.add_argument("--prior-collection", type=Path, help="Optional prior collection breakdown CSV")
    p.add_argument("--dictionary", type=Path, help="Full dictionary CSV with asin, collection")
    p.add_argument("--spend-hourly", type=Path, help="Raw hourly Sheet CSV with date_pt, hour_pt, spend")
    p.add_argument("--deal-calendar", type=Path, help="Google Calendar deal-event JSON; default deal_calendar_status.json")
    p.add_argument("--competitor-check", type=Path, help="Current Keepa competitor snapshot; default competitor_check.json")
    p.add_argument("--competitor-config", type=Path, default=DEFAULT_COMPETITOR_CONFIG, help="Configured representative competitor ASINs")
    p.add_argument("--promotion-names", type=Path, default=DEFAULT_PROMOTION_NAMES, help="Optional ignored JSON {promotion_id: display name}")
    p.add_argument("--order-summary", type=Path, help="Optional exact order summary CSV with date_pt,sales,units,orders,rows")
    p.add_argument("--sales-api-control", type=Path, help="Optional SP-API Sales API control CSV with date_pt,sales,units,orders")
    p.add_argument("--strict", action="store_true", help="Exit non-zero unless verification status is pass")
    return p.parse_args()


def require_tmp(path: Path) -> Path:
    return require_tmp_artifact_path(path, label="Daily report artifacts", error_type=SystemExit)


def resolve_default(report_dir: Path, explicit: Path | None, names: list[str]) -> Path | None:
    if explicit:
        return explicit
    for name in names:
        p = report_dir / name
        if p.exists():
            return p
    return None


def read_dictionary(path: Path | None, checks: list[Check]) -> pd.DataFrame:
    if not path or not path.exists():
        checks.append(Check("dictionary", "fail", "missing full dictionary CSV"))
        return pd.DataFrame(columns=["asin", "collection"])
    df = pd.read_csv(path, dtype=str)
    missing = {"asin", "collection"} - set(df.columns)
    if missing:
        checks.append(Check("dictionary_columns", "fail", ", ".join(sorted(missing))))
        return pd.DataFrame(columns=["asin", "collection"])
    df["asin"] = df["asin"].fillna("").astype(str).str.strip()
    df["collection"] = df["collection"].fillna("").astype(str).str.strip()
    out = df[(df["asin"] != "") & (df["collection"] != "")].drop_duplicates("asin")[["asin", "collection"]]
    checks.append(Check("dictionary_full_local_join", "pass", f"raw_rows={len(df)}, dedup_asins={len(out)}"))
    return out


def add_date_pt(df: pd.DataFrame) -> pd.DataFrame:
    if "date_pt" in df.columns:
        df["date_pt"] = df["date_pt"].astype(str)
        return df
    date_col = "purchase-date" if "purchase-date" in df.columns else "purchase_date"
    if date_col not in df.columns:
        raise ValueError("orders CSV needs date_pt or purchase-date/purchase_date")
    dt = pd.to_datetime(df[date_col], utc=True, errors="coerce")
    df["date_pt"] = dt.dt.tz_convert("America/Los_Angeles").dt.date.astype(str)
    return df


def promotion_summary(df: pd.DataFrame, checks: list[Check], label: str) -> dict[str, Any]:
    """Exclusive order-line promo groups; never allocate stacked sales to each ID."""
    aliases = {
        "ids": ("promotion-ids", "promotion_ids"),
        "item_discount": ("item-promotion-discount", "item_promotion_discount"),
        "shipping_discount": ("ship-promotion-discount", "ship_promotion_discount"),
        "sales": ("item-price", "item_price"),
        "order": ("amazon-order-id", "amazon_order_id"),
        "units": ("quantity",),
    }
    columns = {key: next((col for col in options if col in df.columns), None) for key, options in aliases.items()}
    missing = [key for key, col in columns.items() if col is None]
    if missing or df.empty:
        checks.append(Check(f"{label}_promotion_source", "fail", "missing promotion fields/rows: " + ", ".join(missing)))
        return {"status": "fail", "groups": [], "totals": {}}

    def money(value: Any) -> Decimal:
        # Blank Amazon discount values mean zero, not missing columns.
        text = "" if pd.isna(value) else str(value).strip()
        result = Decimal(text or "0")
        if not result.is_finite() or result < 0:
            raise ValueError("invalid monetary value")
        return result

    groups: dict[tuple[str, ...], dict[str, Any]] = {}
    all_orders: set[str] = set()
    try:
        for row in df.to_dict("records"):
            text = "" if pd.isna(row[columns["ids"]]) else str(row[columns["ids"]]).strip()
            ids = tuple(sorted({part.strip() for part in text.split(",") if part.strip().lower() not in {"", "nan", "null", "none"}}))
            sales = money(row[columns["sales"]])
            item_discount = money(row[columns["item_discount"]])
            shipping_discount = money(row[columns["shipping_discount"]])
            units_decimal = money(row[columns["units"]])
            if units_decimal != units_decimal.to_integral_value():
                raise ValueError("quantity must be integral")
            order = str(row[columns["order"]]).strip()
            if not order or order.lower() in {"nan", "null", "none"}:
                raise ValueError("missing order ID")
            category = "identified" if ids else "unidentified" if item_discount else "no_recorded_promotion"
            key = (category, *ids)
            if key not in groups:
                groups[key] = {
                    "category": category, "promotion_ids": list(ids),
                    "label": " + ".join(ids) + (" (stacked IDs)" if len(ids) > 1 else "") if ids else "Unidentified promotion" if category == "unidentified" else "No recorded promotion",
                    "sales": Decimal(0), "item_discount": Decimal(0), "shipping_discount": Decimal(0),
                    "net_sales": Decimal(0), "units": 0, "rows": 0, "order_ids": set(),
                }
            group = groups[key]
            for metric, value in (("sales", sales), ("item_discount", item_discount), ("shipping_discount", shipping_discount), ("net_sales", sales - item_discount)):
                group[metric] += value
            group["units"] += int(units_decimal)
            group["rows"] += 1
            group["order_ids"].add(order)
            all_orders.add(order)
    except (ValueError, InvalidOperation) as exc:
        checks.append(Check(f"{label}_promotion_source", "fail", f"invalid promotion numeric/order evidence: {exc}"))
        return {"status": "fail", "groups": [], "totals": {}}
    monetary = ("sales", "item_discount", "shipping_discount", "net_sales")
    totals = {metric: float(round(sum(group[metric] for group in groups.values()), 2)) for metric in monetary}
    totals.update(units=sum(group["units"] for group in groups.values()), rows=len(df), orders=len(all_orders))
    order_sets = {tuple(group["promotion_ids"]): group["order_ids"] for group in groups.values() if group["category"] == "identified"}
    output = []
    for group in groups.values():
        group["orders"] = len(group.pop("order_ids"))
        for metric in monetary:
            group[metric] = float(round(group[metric], 2))
        output.append(group)
    output.sort(key=lambda group: (group["category"] != "identified", -group["sales"], group["label"]))
    remaining = [group for group in output if group["category"] == "identified"][10:]
    other_totals = {metric: sum(group[metric] for group in remaining) for metric in (*monetary, "units")}
    other_totals["orders"] = len(set().union(*(order_sets[tuple(group["promotion_ids"])] for group in remaining)))
    checks.append(Check(f"{label}_promotion_source", "pass", f"rows={len(df)}, exclusive_groups={len(output)}"))
    return {"status": "pass", "groups": output, "totals": totals, "other_identified_totals": other_totals}


def normalize_orders(path: Path | None, target_date: str, dictionary: pd.DataFrame, checks: list[Check], label: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not path or not path.exists():
        checks.append(Check(f"{label}_orders_file", "fail", "missing"))
        return pd.DataFrame(), {"date_pt": target_date, "sales": 0.0, "units": 0, "orders": None, "rows": 0, "source": label}
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df = add_date_pt(df)
    before = len(df)
    df = df[df["date_pt"].eq(target_date)].copy()
    if "sales-channel" in df.columns:
        df = df[df["sales-channel"].eq("Amazon.com")]
    if "sales_channel" in df.columns:
        df = df[df["sales_channel"].eq("Amazon.com")]
    if "currency" in df.columns:
        df = df[df["currency"].eq("USD")]
    status_col = "order-status" if "order-status" in df.columns else "order_status" if "order_status" in df.columns else None
    if status_col:
        df = df[~df[status_col].isin({"Cancelled", "Canceled"})]
    if df.empty:
        checks.append(Check(f"{label}_orders_rows", "fail", f"no rows for {target_date}; input_rows={before}"))
        return df, {"date_pt": target_date, "sales": 0.0, "units": 0, "orders": None, "rows": 0, "source": label}
    qty_col = "quantity"
    price_col = "item-price" if "item-price" in df.columns else "item_price"
    if qty_col not in df.columns or price_col not in df.columns:
        checks.append(Check(f"{label}_orders_columns", "fail", "missing quantity or item-price/item_price"))
        return pd.DataFrame(), {"date_pt": target_date, "sales": 0.0, "units": 0, "orders": None, "rows": 0, "source": label}
    promos = promotion_summary(df, checks, label)
    df["quantity"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)
    df["item_price"] = pd.to_numeric(df[price_col], errors="coerce").fillna(0.0)
    if "collection" not in df.columns:
        if "asin" not in df.columns:
            checks.append(Check(f"{label}_collection_join", "fail", "orders missing asin for dictionary join"))
            return pd.DataFrame(), {"date_pt": target_date, "sales": 0.0, "units": 0, "orders": None, "rows": 0, "source": label}
        df = df.merge(dictionary, on="asin", how="left")
    df["collection"] = df["collection"].fillna("Unmapped")
    unmapped = df[df["collection"].eq("Unmapped")]
    checks.append(Check(f"{label}_unmapped", "pass" if unmapped.empty else "fail", f"rows={len(unmapped)}, sales={unmapped['item_price'].sum():.2f}"))
    group = df.groupby(["date_pt", "collection"], as_index=False).agg(
        sales=("item_price", "sum"),
        units=("quantity", "sum"),
        row_count=("collection", "size"),
    )
    group["orders"] = pd.NA
    group["source"] = label
    order_col = "amazon-order-id" if "amazon-order-id" in df.columns else "amazon_order_id" if "amazon_order_id" in df.columns else None
    orders = int(df[order_col].nunique()) if order_col else None
    zero_price = df[df["item_price"].eq(0) & df["quantity"].gt(0)]
    pending_rows = int(df[status_col].eq("Pending").sum()) if status_col else 0
    summary = {
        "date_pt": target_date,
        "sales": round(float(df["item_price"].sum()), 2),
        "units": int(df["quantity"].sum()),
        "orders": orders,
        "rows": int(len(df)),
        "source": label,
        "promotions": promos,
        "zero_price_units": int(zero_price["quantity"].sum()),
        "zero_price_collections": sorted(zero_price["collection"].unique().tolist()),
        "pending_rows": pending_rows,
    }
    checks.append(Check(f"{label}_orders_rows", "pass", f"rows={len(df)}, source_rows={before}"))
    return group, summary


def read_collection_breakdown(path: Path | None, date_pt: str, checks: list[Check], label: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not path or not path.exists():
        checks.append(Check(f"{label}_collection_file", "fail", "missing"))
        return pd.DataFrame(), {"date_pt": date_pt, "sales": 0.0, "units": 0, "orders": None, "rows": 0, "source": label}
    df = pd.read_csv(path)
    if "row_count" in df.columns and "rows" not in df.columns:
        df = df.rename(columns={"row_count": "rows"})
    need = {"date_pt", "collection", "sales", "units"}
    missing = need - set(df.columns)
    if missing:
        checks.append(Check(f"{label}_collection_columns", "fail", ", ".join(sorted(missing))))
        return pd.DataFrame(), {"date_pt": date_pt, "sales": 0.0, "units": 0, "orders": None, "rows": 0, "source": label}
    df = df[df["date_pt"].astype(str).eq(date_pt)].copy()
    if df.empty:
        checks.append(Check(f"{label}_collection_rows", "fail", f"no rows for {date_pt}"))
        return df, {"date_pt": date_pt, "sales": 0.0, "units": 0, "orders": None, "rows": 0, "source": label}
    for col in ["sales", "units", "orders", "rows"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    if "orders" not in df.columns:
        df["orders"] = pd.NA
    if "rows" not in df.columns:
        df["rows"] = df.get("row_count", 0)
    df["source"] = label
    summary = {
        "date_pt": date_pt,
        "sales": round(float(df["sales"].sum()), 2),
        "units": int(df["units"].sum()),
        "orders": None,
        "rows": int(df["rows"].sum()) if "rows" in df.columns else None,
        "source": label,
    }
    checks.append(Check(f"{label}_collection_rows", "pass", f"rows={len(df)}"))
    return df[["date_pt", "collection", "sales", "units", "orders", "rows", "source"]], summary


def apply_exact_summary(summary: dict[str, Any], path: Path | None, checks: list[Check], label: str) -> dict[str, Any]:
    if not path or not path.exists():
        return summary
    df = pd.read_csv(path)
    if "date_pt" not in df.columns:
        checks.append(Check(f"{label}_summary_control", "fail", "missing date_pt"))
        return summary
    row = df[df["date_pt"].astype(str).eq(str(summary["date_pt"]))]
    if row.empty:
        return summary
    row = row.iloc[0]
    for col in ["sales", "units", "orders", "rows"]:
        if col in row.index and not pd.isna(row[col]):
            summary[col] = int(row[col]) if col in {"units", "orders", "rows"} else round(float(row[col]), 2)
    checks.append(Check(f"{label}_summary_control", "pass", f"used {path.name}"))
    return summary


def verify_sales_api_control(summary: dict[str, Any], path: Path | None, checks: list[Check], label: str) -> dict[str, Any]:
    if not path or not path.exists():
        return summary
    df = pd.read_csv(path)
    need = {"date_pt", "sales", "units"}
    missing = need - set(df.columns)
    if missing:
        checks.append(Check(f"{label}_sales_api_control", "fail", ", ".join(sorted(missing))))
        return summary
    row = df[df["date_pt"].astype(str).eq(str(summary["date_pt"]))]
    if row.empty:
        return summary
    row = row.iloc[0]
    sales_diff = round(float(summary["sales"]) - float(row["sales"]), 2)
    units_diff = int(summary["units"]) - int(row["units"])
    status = "pass" if abs(sales_diff) < 0.01 and units_diff == 0 else "fail"
    checks.append(Check(f"{label}_sales_api_control", status, f"sales_diff={sales_diff}, units_diff={units_diff}"))
    summary["sales_api_control_sales"] = round(float(row["sales"]), 2)
    summary["sales_api_control_units"] = int(row["units"])
    if "orders" in row.index and not pd.isna(row["orders"]):
        summary["orders"] = int(row["orders"])
        summary["sales_api_control_orders"] = int(row["orders"])
    return summary


def read_spend(path: Path | None, dates: list[str], checks: list[Check]) -> dict[str, dict[str, Any]]:
    if not path or not path.exists():
        checks.append(Check("spend_hourly_file", "fail", "missing ppc_hourly_current_prior.csv"))
        return {d: {"spend": 0.0, "hours": 0, "missing_hours": list(range(24)), "duplicate_hours": []} for d in dates}
    df = pd.read_csv(path, dtype=str)
    missing = {"date_pt", "hour_pt", "spend"} - set(df.columns)
    if missing:
        checks.append(Check("spend_hourly_columns", "fail", ", ".join(sorted(missing))))
        return {d: {"spend": 0.0, "hours": 0, "missing_hours": list(range(24)), "duplicate_hours": []} for d in dates}
    df["hour_pt"] = pd.to_numeric(df["hour_pt"], errors="coerce")
    df["spend"] = pd.to_numeric(df["spend"], errors="coerce").fillna(0.0)
    out: dict[str, dict[str, Any]] = {}
    expected = set(range(24))
    for d in dates:
        sub = df[df["date_pt"].astype(str).eq(d)].copy()
        counts = sub["hour_pt"].value_counts(dropna=True).to_dict()
        present = {int(h) for h in counts if not pd.isna(h) and int(h) in expected}
        missing_hours = sorted(expected - present)
        duplicate_hours = sorted(int(h) for h, count in counts.items() if not pd.isna(h) and count > 1)
        invalid_hours = sorted(str(h) for h in counts if pd.isna(h) or int(h) not in expected)
        status = "pass" if not missing_hours and not duplicate_hours and not invalid_hours and len(sub) == 24 else "fail"
        details = f"rows={len(sub)}, spend={sub['spend'].sum():.2f}, missing={missing_hours}, duplicate={duplicate_hours}, invalid={invalid_hours}"
        checks.append(Check(f"spend_hours:{d}", status, details))
        out[d] = {
            "spend": round(float(sub["spend"].sum()), 2),
            "hours": len(present),
            "rows": int(len(sub)),
            "missing_hours": missing_hours,
            "duplicate_hours": duplicate_hours,
            "invalid_hours": invalid_hours,
        }
    return out


def parse_calendar_timestamp(value: Any) -> pd.Timestamp:
    """Normalize RFC3339 date-times or Calendar all-day dates to Pacific time."""
    text = str(value)
    timestamp = pd.Timestamp(text)
    if timestamp.tzinfo is None:
        if text != timestamp.strftime("%Y-%m-%d"):
            raise ValueError("timezone required for non-all-day Calendar event")
        timestamp = timestamp.tz_localize("America/Los_Angeles")
    return timestamp.tz_convert("America/Los_Angeles")


def read_deal_calendar(path: Path | None, dates: list[str], checks: list[Check]) -> dict[str, dict[str, Any]]:
    """Validate raw deal-calendar evidence and classify events by duration."""
    empty = {d: {"status": "Unverified", "events": []} for d in dates}
    if not path or not path.exists():
        checks.append(Check("deal_calendar_file", "fail", "missing deal_calendar_status.json"))
        return empty
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        checks.append(Check("deal_calendar_file", "fail", f"invalid JSON: {exc}"))
        return empty
    source_ok = (
        data.get("source") == "Google Calendar Events API"
        and data.get("calendar_name") == "Lightning Deals"
        and isinstance(data.get("calendar_id"), str)
        and bool(data.get("calendar_id"))
    )
    checks.append(Check("deal_calendar_source", "pass" if source_ok else "fail", f"source={data.get('source')}; calendar={data.get('calendar_name')}; calendar_id_present={bool(data.get('calendar_id'))}"))
    raw_dates = data.get("dates") if isinstance(data.get("dates"), dict) else {}
    out: dict[str, dict[str, Any]] = {}
    for day in dates:
        item = raw_dates.get(day)
        if not isinstance(item, dict) or item.get("checked") is not True or not isinstance(item.get("events"), list):
            checks.append(Check(f"deal_calendar:{day}", "fail", "date missing, not checked, or events not a list"))
            out[day] = empty[day]
            continue
        day_start = pd.Timestamp(day, tz="America/Los_Angeles")
        day_end = pd.Timestamp(day_start.date() + timedelta(days=1), tz="America/Los_Angeles")
        try:
            time_min = pd.Timestamp(item["time_min"]).tz_convert("America/Los_Angeles")
            time_max = pd.Timestamp(item["time_max"]).tz_convert("America/Los_Angeles")
            bounds_ok = time_min == day_start and time_max == day_end
        except Exception:
            bounds_ok = False
        normalized: list[dict[str, str]] = []
        valid = source_ok and bounds_ok
        for event in item["events"]:
            try:
                start_text = str(event["start"])
                end_text = str(event["end"])
                start = parse_calendar_timestamp(start_text)
                end = parse_calendar_timestamp(end_text)
                all_day = len(start_text) == 10 and len(end_text) == 10
                duration = (date.fromisoformat(end_text) - date.fromisoformat(start_text)).days if all_day else (end - start).total_seconds() / 3600
                overlaps = start < day_end and end > day_start
                confirmed = event.get("status") == "confirmed"
                deal_type = "Best Deal" if all_day and duration >= 1 else "Lightning Deal" if 4 <= duration <= 12 else "Best Deal" if duration >= 24 else "Invalid"
                valid = valid and overlaps and confirmed and deal_type != "Invalid"
                normalized.append({
                    "name": str(event.get("summary") or "Unnamed deal"),
                    "deal_type": deal_type,
                    "start_pt": start.isoformat(),
                    "end_pt": end.isoformat(),
                })
            except Exception:
                valid = False
        status = "None" if not normalized else " + ".join(sorted({event["deal_type"] for event in normalized}))
        checks.append(Check(f"deal_calendar:{day}", "pass" if valid else "fail", f"events={len(normalized)}, status={status}, bounds_ok={bounds_ok}"))
        out[day] = {"status": status if valid else "Unverified", "events": normalized if valid else []}
    return out


def read_competitor_check(
    path: Path | None,
    config_path: Path | None,
    checks: list[Check],
    *,
    now_utc: pd.Timestamp | None = None,
    target_date: str | None = None,
) -> list[dict[str, Any]]:
    if not path or not path.is_file():
        checks.append(Check("competitor_check", "fail", "missing competitor_check.json"))
        return []
    if not config_path or not config_path.is_file():
        checks.append(Check("competitor_check", "fail", "missing competitor configuration"))
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        checks.append(Check("competitor_check", "fail", f"invalid JSON: {exc}"))
        return []

    products = data.get("products") if isinstance(data.get("products"), list) else []
    configured = config.get("products") if isinstance(config.get("products"), list) else []
    expected_pairs = {
        (str(item.get("brand") or "").strip(), str(item.get("asin") or "").strip().upper())
        for item in configured
    }
    actual_pairs = {
        (str(item.get("brand") or "").strip(), str(item.get("asin") or "").strip().upper())
        for item in products
    }
    expected_brands = {brand for brand, _ in expected_pairs}
    missing_brands = REQUIRED_COMPETITOR_BRANDS - expected_brands
    now = now_utc if now_utc is not None else pd.Timestamp.now(tz="UTC")
    if now.tzinfo is None:
        now = now.tz_localize("UTC")
    else:
        now = now.tz_convert("UTC")
    ages: list[float] = []
    valid = (
        data.get("status") == "pass"
        and data.get("marketplace") == "Amazon.com"
        and bool(expected_pairs)
        and not missing_brands
        and actual_pairs == expected_pairs
        and len(products) == len(actual_pairs)
        and (target_date is None or data.get("target_date") == target_date)
    )
    for item in products:
        price = item.get("current_price")
        deals = item.get("deals")
        duration_status = item.get("duration_status")
        valid = valid and isinstance(price, (int, float)) and price > 0
        valid = valid and isinstance(deals, list)
        valid = valid and duration_status in {"not_applicable", "not_exposed_by_keepa", "end_only", "complete", "ongoing"}
        valid = valid and item.get("source") in COMPETITOR_SOURCES
        try:
            checked = pd.Timestamp(item["checked_at_utc"])
            if checked.tzinfo is None:
                raise ValueError("timezone missing")
            age_minutes = (now - checked.tz_convert("UTC")).total_seconds() / 60
            ages.append(age_minutes)
            valid = valid and 0 <= age_minutes <= COMPETITOR_MAX_AGE_MINUTES
        except Exception:
            valid = False
    missing_pairs = sorted(expected_pairs - actual_pairs)
    extra_pairs = sorted(actual_pairs - expected_pairs)
    max_age = max(ages) if ages else float("inf")
    details = (
        f"products={len(products)}, configured={len(expected_pairs)}, "
        f"missing_brands={sorted(missing_brands)}, missing_pairs={missing_pairs}, "
        f"extra_pairs={extra_pairs}, max_age_minutes={max_age:.1f}, "
        f"target_date={data.get('target_date')}"
    )
    checks.append(Check("competitor_check", "pass" if valid else "fail", details))
    return products if valid else []


def fmt_money(v: float, digits: int = 0) -> str:
    return f"${float(v):,.{digits}f}"


def fmt_int(v: float | int | None) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    return f"{int(round(float(v))):,}"


def fmt_pct(v: float, digits: int = 2) -> str:
    return f"{float(v) * 100:.{digits}f}%"


def delta_money(v: float) -> str:
    return ("+" if v >= 0 else "-") + fmt_money(abs(v), 0)


def delta_int(v: float) -> str:
    return ("+" if v >= 0 else "-") + f"{abs(int(round(float(v)))):,}"


def delta_pp(v: float) -> str:
    return f"{v * 100:+.2f} pp"


def color(value: float, lower_good: bool = False) -> str:
    good = value < 0 if lower_good else value >= 0
    return "#0f8a4b" if good else "#b91c1c"


def pct_delta(current: float, prior: float) -> float | None:
    if prior == 0:
        return None
    return current / prior - 1


CELL = "padding:7px 8px;border:1px solid #e5e7eb"
HEAD = "padding:7px 8px;border:1px solid #1f2937;background:#1f2937;color:#fff"


def short_date(day: str) -> str:
    try:
        return datetime.strptime(day, "%Y-%m-%d").strftime("%a %b %-d")
    except ValueError:
        return day


def clock(value: str | None) -> str:
    if not value:
        return ""
    return pd.Timestamp(value).tz_convert("America/Los_Angeles").strftime("%b %-d %-I:%M %p")


def signed_pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:+.1f}%"


def promotion_label(group: dict[str, Any], names: dict[str, str]) -> str:
    if group.get("category") != "identified" or not group.get("promotion_ids"):
        return group["label"]
    parts = [names.get(pid) or (f"Promo {pid[:8]}" if len(pid) == 36 and pid.count("-") == 4 else pid) for pid in group["promotion_ids"]]
    return " + ".join(parts) + (" (stacked)" if len(parts) > 1 else "")


def plain_sales(promotions: dict[str, Any]) -> float | None:
    """Sales on lines with no recorded promotion; None when promotion evidence is unverified."""
    if promotions.get("status") != "pass":
        return None
    return next((group["sales"] for group in promotions["groups"] if group.get("label") == "No recorded promotion"), 0.0)


def render_promotions(target_date: str, prior_date: str, promotions: dict[str, dict[str, Any]], names: dict[str, str] | None = None) -> str:
    names = names or {}
    sections = ["<h2 style='font-size:18px;margin:22px 0 8px;color:#111827'>Promotion-associated sales</h2>"]
    sections.append("<p style='font-size:12px;color:#667085;margin:0 0 8px'>Ranked by gross sales on tagged items, not entire order baskets. These are associated sales, not incremental lift or proof that a promotion caused purchases. Stacked IDs stay together; no sales are counted twice.</p>")
    for day in (target_date, prior_date):
        item = promotions.get(day, {})
        if item.get("status") != "pass":
            sections.append(f"<p>{escape(day)}: promotion evidence unverified — report blocked.</p>")
            continue
        groups = item["groups"]
        identified = [group for group in groups if group["category"] == "identified"]
        controls = [group for group in groups if group["category"] != "identified"]
        totals = item["totals"]
        sections.append(f"<h3 style='font-size:14px;margin:14px 0 6px'>{escape(short_date(day))} ({escape(day)}) PT</h3>")
        if not identified:
            sections.append("<p style='font-size:12px'>No identified promotion ranking available; discounted items with blank IDs remain unidentified.</p>")
        elif len(identified) > 10:
            sections.append(f"<p style='font-size:12px;color:#667085'>Top 10 of {len(identified)} identified promotion groups. Remaining groups are combined below.</p>")
        displayed = identified[:10]
        if len(identified) > 10:
            remaining = identified[10:]
            displayed.append({"label": f"Other {len(remaining)} identified groups", **item["other_identified_totals"]})
        displayed += controls
        displayed.append({"label": "All items — reconciliation total", **totals})
        rows = []
        for number, group in enumerate(displayed):
            rank = f"#{number + 1} · " if number < min(10, len(identified)) else ""
            name = escape(rank + promotion_label(group, names))
            bold = "font-weight:700;" if group["label"].startswith("All items") else ""
            counts = "".join(f"<td style='{CELL};{bold}text-align:right'>{fmt_int(group[metric])}</td>" for metric in ("units", "orders"))
            discount_pct = fmt_pct(group["item_discount"] / group["sales"]) if group["sales"] > 0 else "—"
            money_cells = "".join(f"<td style='{CELL};{bold}text-align:right;white-space:nowrap'>{fmt_money(group[metric], 0)}</td>" for metric in ("sales", "item_discount", "net_sales"))
            rows.append(f"<tr><td style='{CELL};{bold}overflow-wrap:anywhere;word-break:break-word'>{name}</td>{counts}{money_cells}<td style='{CELL};{bold}text-align:right'>{discount_pct}</td></tr>")
        headings = "".join(
            f"<th style='{HEAD};text-align:{align};font-size:10px'>{heading}</th>"
            for heading, align in (("Promotion", "left"), ("Units", "right"), ("Orders", "right"), ("Gross", "right"), ("Discount", "right"), ("Net", "right"), ("Actual discount %", "right"))
        )
        sections.append("<table width='100%' cellpadding='0' cellspacing='0' style='table-layout:fixed;border-collapse:collapse;font-size:11px'><colgroup><col style='width:28%'><col style='width:9%'><col style='width:10%'><col style='width:13%'><col style='width:12%'><col style='width:13%'><col style='width:15%'></colgroup><tr>" + headings + "</tr>" + "".join(rows) + "</table>")
    sections.append("<p style='font-size:11px;color:#667085'>Actual discount % = item discounts ÷ gross item sales (sales-weighted); zero gross sales is shown as —. Net deducts item discounts only. Unknown IDs are not assumed Lightning Deals. Distinct orders across promo groups are not additive. Full promotion IDs are in promotion_sales.csv.</p>")
    return "\n".join(sections)


def deal_text(item: dict[str, Any]) -> str:
    if item["status"] == "Unverified":
        return "Unverified — report blocked"
    if item["status"] == "None":
        return "No Lightning Deal or Best Deal"
    return "; ".join(f"{event['deal_type']}: {event['name']} ({clock(event['start_pt'])} – {clock(event['end_pt'])})" for event in item["events"])


def competitor_deal_text(item: dict[str, Any]) -> str:
    if item.get("date_deals"):
        parts = []
        for deal in item["date_deals"]:
            window = f"{clock(deal['start'])} – {clock(deal['end'])}, {deal['duration_hours']} h" if deal.get("end") else f"since {clock(deal['start'])}, ongoing"
            parts.append(f"{deal['badge']} at {fmt_money(deal['deal_price'], 2)} ({window})")
        return "; ".join(parts)
    if not item["deals"]:
        return "No deal"
    badges = ", ".join(str(deal.get("badge") or deal.get("type")) for deal in item["deals"])
    kind = "Best/Lightning" if item["best_or_lightning_deal"] else "not Best/Lightning"
    if item["duration_status"] == "complete":
        duration = f"{item.get('duration_hours')} h"
    elif item["duration_status"] == "end_only":
        duration = f"ends {clock(item.get('deal_end'))}"
    else:
        duration = "duration not exposed by Keepa"
    return f"Active at check: {badges} ({kind}; {duration})"


def summary_bullets(target_date: str, prior_date: str, current: dict[str, Any], prior: dict[str, Any], spend: dict[str, dict[str, Any]], deals: dict[str, dict[str, Any]], comp: pd.DataFrame, promotions: dict[str, dict[str, Any]]) -> list[str]:
    bullets = [
        f"Sales {fmt_money(current['sales'])} vs {fmt_money(prior['sales'])} on {short_date(prior_date)} ({signed_pct(pct_delta(current['sales'], prior['sales']))}); units {fmt_int(current['units'])} vs {fmt_int(prior['units'])}."
    ]
    for day in (target_date, prior_date):
        if deals[day]["status"] not in {"None", "Unverified"}:
            bullets.append(f"{short_date(day)} had {deal_text(deals[day])} — headline comparison is not like-for-like.")
    cur_plain = plain_sales(promotions.get(target_date, {}))
    pri_plain = plain_sales(promotions.get(prior_date, {}))
    if cur_plain is not None and pri_plain is not None:
        bullets.append(f"Like-for-like, sales on lines with no recorded promotion: {fmt_money(cur_plain)} vs {fmt_money(pri_plain)} ({signed_pct(pct_delta(cur_plain, pri_plain))}).")
    movers = comp.reindex(comp["sales_delta"].abs().sort_values(ascending=False).index).head(2)
    if not movers.empty:
        bullets.append("Largest collection moves: " + "; ".join(f"{r['collection']} {('+' if r['sales_delta'] >= 0 else '-')}{fmt_money(abs(r['sales_delta']))} ({signed_pct(None if pd.isna(r['sales_delta_pct']) else float(r['sales_delta_pct']))})" for _, r in movers.iterrows()) + ".")
    cur_spend, pri_spend = spend[target_date]["spend"], spend[prior_date]["spend"]
    if current["sales"] > 0 and prior["sales"] > 0:
        bullets.append(f"PPC spend {fmt_money(cur_spend)} ({signed_pct(pct_delta(cur_spend, pri_spend))}); TACOS {fmt_pct(cur_spend / current['sales'])} ({delta_pp(cur_spend / current['sales'] - pri_spend / prior['sales'])}).")
    if current.get("rows") and current.get("pending_rows", 0) / current["rows"] >= 0.25:
        bullets.append(f"{current['pending_rows']} of {current['rows']} order lines are still Pending (vs {prior.get('pending_rows', 0)} of {prior.get('rows', 0)} a week earlier); target-day figures can still change through cancellations.")
    if current.get("zero_price_units"):
        bullets.append(f"{current['zero_price_units']} unit(s) recorded at $0.00 item price ({', '.join(current['zero_price_collections'])}) — check for replacements or pricing errors.")
    return bullets


def render_html(target_date: str, prior_date: str, current: dict[str, Any], prior: dict[str, Any], spend: dict[str, dict[str, Any]], deals: dict[str, dict[str, Any]], competitors: list[dict[str, Any]], comp: pd.DataFrame, promotions: dict[str, dict[str, Any]], promotion_names: dict[str, str] | None = None) -> str:
    cur_spend = spend[target_date]["spend"]
    pri_spend = spend[prior_date]["spend"]
    cur_tacos = cur_spend / current["sales"]
    pri_tacos = pri_spend / prior["sales"]
    sales_delta = current["sales"] - prior["sales"]
    units_delta = current["units"] - prior["units"]
    spend_delta = cur_spend - pri_spend
    tacos_delta = cur_tacos - pri_tacos
    sales_pct = pct_delta(current["sales"], prior["sales"])
    units_pct = pct_delta(current["units"], prior["units"])
    spend_pct = pct_delta(cur_spend, pri_spend)
    t_label, p_label = short_date(target_date), short_date(prior_date)
    kpis = [
        ("Sales", fmt_money(current["sales"], 0), f"{delta_money(sales_delta)} ({signed_pct(sales_pct)})", color(sales_delta)),
        ("Units", fmt_int(current["units"]), f"{delta_int(units_delta)} ({signed_pct(units_pct)})", color(units_delta)),
        ("PPC Spend", fmt_money(cur_spend, 0), f"{delta_money(spend_delta)} ({signed_pct(spend_pct)})", color(spend_delta, lower_good=True)),
        ("TACOS", fmt_pct(cur_tacos), delta_pp(tacos_delta), color(tacos_delta, lower_good=True)),
    ]
    cards = [
        f"""<td style='width:50%;vertical-align:top;padding:0 {'6px' if i % 2 == 0 else '0'} 10px {'0' if i % 2 == 0 else '6px'}'>
      <div style='background:#f8fbff;border:1px solid #dbeafe;border-radius:10px;padding:12px'>
        <div style='font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#475467;font-weight:700'>{escape(label)}</div>
        <div style='font-size:22px;font-weight:800;margin:5px 0;color:#111827'>{escape(value)}</div>
        <div style='font-size:12px;color:{line_color};font-weight:700'>{escape(line)} vs {escape(p_label)}</div>
      </div>
    </td>""" for i, (label, value, line, line_color) in enumerate(kpis)
    ]
    kpi_rows = f"<tr>{cards[0]}{cards[1]}</tr><tr>{cards[2]}{cards[3]}</tr>"
    bullets = "".join(f"<li style='margin:0 0 5px'>{escape(text)}</li>" for text in summary_bullets(target_date, prior_date, current, prior, spend, deals, comp, promotions))
    cur_plain = plain_sales(promotions.get(target_date, {}))
    pri_plain = plain_sales(promotions.get(prior_date, {}))

    def metric_row(label: str, cur: str, pri: str, change: str, change_color: str, note: str = "") -> str:
        note_html = f"<br><span style='font-size:11px;color:#667085;font-weight:400'>{escape(note)}</span>" if note else ""
        return f"<tr><td style='{CELL};font-weight:700'>{escape(label)}{note_html}</td><td style='{CELL};text-align:right'>{cur}</td><td style='{CELL};text-align:right'>{pri}</td><td style='{CELL};text-align:right;color:{change_color};font-weight:700;white-space:nowrap'>{change}</td></tr>"

    exec_rows = "".join([
        metric_row("Sales", fmt_money(current["sales"], 0), fmt_money(prior["sales"], 0), f"{delta_money(sales_delta)}<br>{signed_pct(sales_pct)}", color(sales_delta)),
        metric_row("Sales, no recorded promo", fmt_money(cur_plain, 0), fmt_money(pri_plain, 0), f"{delta_money(cur_plain - pri_plain)}<br>{signed_pct(pct_delta(cur_plain, pri_plain))}", color(cur_plain - pri_plain), "like-for-like")
        if cur_plain is not None and pri_plain is not None else "",
        metric_row("Units", fmt_int(current["units"]), fmt_int(prior["units"]), f"{delta_int(units_delta)}<br>{signed_pct(units_pct)}", color(units_delta)),
        metric_row("PPC Spend", fmt_money(cur_spend, 0), fmt_money(pri_spend, 0), f"{delta_money(spend_delta)}<br>{signed_pct(spend_pct)}", color(spend_delta, lower_good=True)),
        metric_row("TACOS", fmt_pct(cur_tacos), fmt_pct(pri_tacos), delta_pp(tacos_delta), color(tacos_delta, lower_good=True)),
    ])
    deal_rows = "".join(
        f"<tr><td style='{CELL};font-weight:700;white-space:nowrap'>{escape(short_date(day))}</td><td style='{CELL}'>{escape(deal_text(deals[day]))}</td></tr>"
        for day in [target_date, prior_date]
    )
    competitor_rows = []
    for item in competitors:
        price_notes = [str(item["current_price_source"]).replace("_", " ").title()]
        if item.get("list_price"):
            price_notes.append(f"list {fmt_money(item['list_price'], 2)}")
        if item.get("coupon"):
            price_notes.append(f"coupon {item['coupon']}")
        competitor_rows.append(f"""
      <tr>
        <td style='{CELL}'><b>{escape(str(item['brand']))}</b><br><span style='font-size:11px;color:#667085'>{escape(str(item['label']))} · {escape(str(item['asin']))}</span></td>
        <td style='{CELL};text-align:right'><b>{fmt_money(item['current_price'], 2)}</b><br><span style='font-size:11px;color:#667085'>{escape(' · '.join(price_notes))}</span></td>
        <td style='{CELL}'>{escape(competitor_deal_text(item))}</td>
      </tr>""")
    ordered = comp.reindex(comp["sales_delta"].abs().sort_values(ascending=False).index)
    shown, rest = ordered.head(COLLECTION_ROWS_SHOWN), ordered.iloc[COLLECTION_ROWS_SHOWN:]
    max_sales = max(float(comp["current_sales"].max() or 0), float(comp["prior_sales"].max() or 0), 1.0)

    def collection_row(name: str, cur_sales: float, cur_units: float, pri_sales: float, pri_units: float, bars: bool = True) -> str:
        d = cur_sales - pri_sales
        bar_html = ""
        if bars:
            cw = max(1, min(100, cur_sales / max_sales * 100))
            pw = max(1, min(100, pri_sales / max_sales * 100))
            bar_html = (f"<div style='margin-top:5px;background:#eef2ff;height:6px;border-radius:4px;overflow:hidden'><div style='width:{cw:.1f}%;background:#2563eb;height:6px'></div></div>"
                        f"<div style='margin-top:2px;background:#f1f5f9;height:6px;border-radius:4px;overflow:hidden'><div style='width:{pw:.1f}%;background:#94a3b8;height:6px'></div></div>")
        return f"""
      <tr>
        <td style='{CELL};font-weight:700'>{escape(name)}{bar_html}</td>
        <td style='{CELL};text-align:right;white-space:nowrap'>{fmt_money(cur_sales, 0)}<br><span style='font-size:11px;color:#667085'>{fmt_int(cur_units)} u</span></td>
        <td style='{CELL};text-align:right;white-space:nowrap'>{fmt_money(pri_sales, 0)}<br><span style='font-size:11px;color:#667085'>{fmt_int(pri_units)} u</span></td>
        <td style='{CELL};text-align:right;color:{color(d)};font-weight:700;white-space:nowrap'>{('+' if d >= 0 else '-') + fmt_money(abs(d), 0)}<br><span style='font-size:11px'>{signed_pct(pct_delta(cur_sales, pri_sales))}</span></td>
      </tr>"""

    rows = [collection_row(str(r["collection"]), float(r["current_sales"]), float(r["current_units"]), float(r["prior_sales"]), float(r["prior_units"])) for _, r in shown.iterrows()]
    if not rest.empty:
        rows.append(collection_row(f"Other {len(rest)} collections", float(rest["current_sales"].sum()), float(rest["current_units"].sum()), float(rest["prior_sales"].sum()), float(rest["prior_units"].sum()), bars=False))
    rows.append(collection_row("Total", float(comp["current_sales"].sum()), float(comp["current_units"].sum()), float(comp["prior_sales"].sum()), float(comp["prior_units"].sum()), bars=False))
    th = f"style='{HEAD};text-align:right;white-space:nowrap'"
    return f"""
<meta charset="utf-8">
<div style="font-family:Arial,Helvetica,sans-serif;color:#242424;max-width:760px;margin:0 auto;background:#fff;font-size:13px;line-height:1.4">
  <div style="background:#111827;color:#fff;padding:20px 22px;margin-bottom:12px">
    <h1 style="margin:0 0 6px;font-size:23px;line-height:1.2">Mellanni Daily Sales Report</h1>
    <div style="font-size:13px;color:#dbe4f0">{escape(t_label)} ({target_date}) PT · vs {escape(p_label)} ({prior_date})</div>
  </div>
  <div style="border-left:4px solid #f97316;background:#fff7ed;padding:10px 14px;margin:0 0 14px;font-weight:700">Prepared by Sergey's AI helper.</div>
  <h2 style="font-size:18px;margin:14px 0 6px;color:#111827">What happened</h2>
  <ul style="margin:0 0 12px;padding-left:20px">{bullets}</ul>
  <h2 style="font-size:18px;margin:16px 0 8px;color:#111827">Executive snapshot</h2>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;margin-bottom:4px">{kpi_rows}</table>
  <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;margin:8px 0 16px;font-size:13px">
    <tr><th style="{HEAD};text-align:left">Metric</th><th {th}>{escape(t_label)}</th><th {th}>{escape(p_label)}</th><th {th}>Change</th></tr>
    {exec_rows}
  </table>
  <h2 style="font-size:18px;margin:20px 0 8px;color:#111827">Deal calendar check</h2>
  <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;margin-bottom:16px;font-size:13px">
    <tr><th style="{HEAD};text-align:left;width:28%">Pacific date</th><th style="{HEAD};text-align:left">Lightning / Best Deal</th></tr>
    {deal_rows}
  </table>
  <h2 style="font-size:18px;margin:20px 0 8px;color:#111827">Competitor price and deal check</h2>
  <table width="100%" cellpadding="0" cellspacing="0" style="table-layout:fixed;border-collapse:collapse;margin-bottom:6px;font-size:12px">
    <colgroup><col style="width:38%"><col style="width:22%"><col style="width:40%"></colgroup>
    <tr><th style="{HEAD};text-align:left">Competitor</th><th style="{HEAD};text-align:right">Price now</th><th style="{HEAD};text-align:left">Deal on {escape(t_label)}</th></tr>
    {''.join(competitor_rows)}
  </table>
  <p style="font-size:11px;color:#64748b;margin:0 0 16px">Live price and badges from fresh Keepa evidence. Lightning Deal and Prime Exclusive windows on the date come from Keepa price history, including deals that already ended; Best Deal history is not available, and missing durations are stated rather than inferred.</p>
  {render_promotions(target_date, prior_date, promotions, promotion_names)}
  <h2 style="font-size:18px;margin:22px 0 8px;color:#111827">Collection breakdown</h2>
  <p style="font-size:12px;color:#667085;margin:0 0 6px">Top {COLLECTION_ROWS_SHOWN} collections by absolute sales change; bars show {escape(t_label)} (blue) and {escape(p_label)} (gray) on one scale. Full list in collection_comparison.csv.</p>
  <table width="100%" cellpadding="0" cellspacing="0" style="table-layout:fixed;border-collapse:collapse;font-size:12px">
    <colgroup><col style="width:46%"><col style="width:18%"><col style="width:18%"><col style="width:18%"></colgroup>
    <tr><th style="{HEAD};text-align:left">Collection</th><th {th}>{escape(t_label)}</th><th {th}>{escape(p_label)}</th><th {th}>Change</th></tr>
    {''.join(rows)}
  </table>
  <p style="font-size:12px;color:#667085;margin-top:14px">TACOS = PPC spend ÷ total sales. Source notes are recorded in the verification JSON.</p>
</div>
"""


def main() -> int:
    args = parse_args()
    report_dir = require_tmp(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    target_date = args.target_date
    prior_date = args.prior_date or (datetime.strptime(target_date, "%Y-%m-%d").date() - timedelta(days=7)).isoformat()
    checks: list[Check] = []

    dictionary_path = resolve_default(report_dir, args.dictionary, ["dictionary_full_asin_collection.csv"])
    dictionary = read_dictionary(dictionary_path, checks)
    target_orders = resolve_default(report_dir, args.target_orders, ["target_all_orders.csv", "all_orders_us_filtered_sanitized.csv", "all_orders_us_mapped_sanitized.csv"])
    current_coll, current_summary = normalize_orders(target_orders, target_date, dictionary, checks, "target_spapi_all_orders")

    prior_orders = resolve_default(report_dir, args.prior_orders, ["prior_all_orders.csv"])
    prior_coll = pd.DataFrame()
    prior_summary: dict[str, Any]
    prior_source = "prior_spapi_all_orders"
    if prior_orders and prior_orders.exists():
        prior_coll, prior_summary = normalize_orders(prior_orders, prior_date, dictionary, checks, prior_source)
    else:
        prior_collection = resolve_default(report_dir, args.prior_collection, ["prior_collection_breakdown.csv", "bigquery_collection_breakdown_current_prior.csv"])
        prior_source = "prior_bigquery_all_orders_fallback"
        prior_coll, prior_summary = read_collection_breakdown(prior_collection, prior_date, checks, prior_source)
        checks.append(Check("prior_fallback_reason", "warning", "SP-API prior all-orders file unavailable; used BigQuery prior fallback/control"))

    summary_path = resolve_default(report_dir, args.order_summary, ["all_orders_summary.csv"])
    current_summary = apply_exact_summary(current_summary, summary_path, checks, "target")
    control_path = resolve_default(report_dir, args.sales_api_control, ["spapi_sales_api_control.csv"])
    prior_summary = verify_sales_api_control(prior_summary, control_path, checks, "prior")

    promotions = {
        target_date: current_summary.pop("promotions", {"status": "fail", "groups": [], "totals": {}}),
        prior_date: prior_summary.pop("promotions", {"status": "fail", "groups": [], "totals": {}}),
    }
    for day, summary in ((target_date, current_summary), (prior_date, prior_summary)):
        promo = promotions[day]
        totals = promo["totals"]
        reconciled = promo["status"] == "pass" and all(
            abs(totals.get(metric, float("inf")) - summary[metric]) < 0.01
            for metric in ("sales", "units", "rows")
        )
        if promo["status"] == "pass":
            reconciled = reconciled and all(
                abs(sum(group[metric] for group in promo["groups"]) - totals[metric]) < 0.01
                for metric in ("sales", "item_discount", "shipping_discount", "net_sales", "units", "rows")
            )
        checks.append(Check(f"promotion_reconciliation:{day}", "pass" if reconciled else "fail", "exclusive groups vs selected source totals; order-line promotion evidence required"))
        if not reconciled:
            promo["status"] = "fail"
    promo_path = report_dir / "promotion_sales.csv"
    pd.DataFrame([{**group, "date_pt": day, "promotion_ids": json.dumps(group["promotion_ids"])} for day, promo in promotions.items() for group in promo["groups"]]).to_csv(promo_path, index=False)

    spend_path = resolve_default(report_dir, args.spend_hourly, ["ppc_hourly_current_prior.csv"])
    spend = read_spend(spend_path, [target_date, prior_date], checks)
    deal_path = resolve_default(report_dir, args.deal_calendar, ["deal_calendar_status.json"])
    deals = read_deal_calendar(deal_path, [target_date, prior_date], checks)
    competitor_path = resolve_default(report_dir, args.competitor_check, ["competitor_check.json"])
    competitors = read_competitor_check(competitor_path, args.competitor_config, checks, target_date=target_date)

    if current_summary["sales"] <= 0:
        checks.append(Check("target_sales_nonzero", "fail", "target sales are zero; TACOS undefined"))
    else:
        checks.append(Check("target_sales_nonzero", "pass", f"sales={current_summary['sales']:.2f}"))
    if prior_summary["sales"] <= 0:
        checks.append(Check("prior_sales_nonzero", "fail", "prior sales are zero; TACOS undefined"))
    else:
        checks.append(Check("prior_sales_nonzero", "pass", f"sales={prior_summary['sales']:.2f}"))

    # Compare collection totals to selected date totals.
    for label, coll, summary in [("target", current_coll, current_summary), ("prior", prior_coll, prior_summary)]:
        sales_diff = round(float(coll["sales"].sum()) - float(summary["sales"]), 2) if not coll.empty else float("nan")
        units_diff = int(round(float(coll["units"].sum()) - float(summary["units"]))) if not coll.empty else 999999
        status = "pass" if abs(sales_diff) < 0.01 and units_diff == 0 else "fail"
        checks.append(Check(f"{label}_collection_reconciliation", status, f"sales_diff={sales_diff}, units_diff={units_diff}"))

    cur = current_coll[["collection", "sales", "units"]].rename(columns={"sales": "current_sales", "units": "current_units"})
    pri = prior_coll[["collection", "sales", "units"]].rename(columns={"sales": "prior_sales", "units": "prior_units"})
    comp = cur.merge(pri, on="collection", how="outer").fillna(0)
    comp["sales_delta"] = comp["current_sales"] - comp["prior_sales"]
    comp["sales_delta_pct"] = comp.apply(lambda r: None if r["prior_sales"] == 0 else r["current_sales"] / r["prior_sales"] - 1, axis=1)
    comp["units_delta"] = comp["current_units"] - comp["prior_units"]
    comp_path = report_dir / "collection_comparison.csv"
    comp.to_csv(comp_path, index=False)

    subject = f"{target_date} results: {fmt_int(current_summary['units'])} units, {fmt_money(current_summary['sales'], 0)} sales, PPC Spend = {fmt_money(spend[target_date]['spend'], 0)} with {fmt_pct(spend[target_date]['spend'] / current_summary['sales'])} TACOS"
    promotion_names = json.loads(args.promotion_names.read_text(encoding="utf-8")) if args.promotion_names and args.promotion_names.is_file() else {}
    html_body = render_html(target_date, prior_date, current_summary, prior_summary, spend, deals, competitors, comp, promotions, promotion_names)
    html_path = report_dir / f"email_body_{target_date}.html"
    subject_path = report_dir / f"email_subject_{target_date}.txt"
    verification_path = report_dir / f"verification_{target_date}.json"
    html_path.write_text(html_body, encoding="utf-8")
    subject_path.write_text(subject + "\n", encoding="utf-8")

    status = "pass" if checks and all(c.status in {"pass", "warning"} for c in checks) else "fail"
    def rel(path: Path) -> str:
        return path.relative_to(report_dir).as_posix()

    verification = {
        "status": status,
        "target_date_pt": target_date,
        "prior_date_pt": prior_date,
        "subject": subject,
        "current": current_summary | {"ppc_spend": spend[target_date]["spend"], "spend_hours": spend[target_date]["hours"], "tacos": spend[target_date]["spend"] / current_summary["sales"] if current_summary["sales"] else None},
        "prior": prior_summary | {"ppc_spend": spend[prior_date]["spend"], "spend_hours": spend[prior_date]["hours"], "tacos": spend[prior_date]["spend"] / prior_summary["sales"] if prior_summary["sales"] else None},
        "deals": deals,
        "competitors": competitors,
        "promotions": promotions,
        "checks": [asdict(c) for c in checks],
        "outputs": {"html": rel(html_path), "subject": rel(subject_path), "comparison": rel(comp_path), "promotions": rel(promo_path), "verification": rel(verification_path)},
        "draft_created": False,
        "email_sent": False,
    }
    verification_path.write_text(json.dumps(verification, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"status": status, "subject": subject, "html": str(html_path), "verification": str(verification_path)}, indent=2))
    if args.strict and status != "pass":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
