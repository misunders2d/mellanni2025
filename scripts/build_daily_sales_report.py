#!/usr/bin/env python3
"""Build Mellanni daily sales report from durable CSV extracts.

This script is deterministic: it does not call SP-API, BigQuery, Google Sheets,
or Gmail. First place source extracts in the report directory, then run this
builder to produce the HTML body, subject, comparison CSV, and verification JSON.

Default input filenames in --report-dir:
  all_orders_us_filtered_sanitized.csv       target SP-API all-orders rows
  dictionary_full_asin_collection.csv        full dictionary ASIN->collection
  bigquery_collection_breakdown_current_prior.csv  optional prior fallback rows
  ppc_hourly_current_prior.csv               hourly Sheet rows for target/prior

Useful output assets stay in the same durable report directory.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from html import escape
from pathlib import Path
from typing import Any

import pandas as pd

FORBIDDEN_OUTPUT_ROOT = Path("/home/misunderstood/temp").resolve()


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
    p.add_argument("--order-summary", type=Path, help="Optional exact order summary CSV with date_pt,sales,units,orders,rows")
    p.add_argument("--sales-api-control", type=Path, help="Optional SP-API Sales API control CSV with date_pt,sales,units,orders")
    p.add_argument("--strict", action="store_true", help="Exit non-zero unless verification status is pass")
    return p.parse_args()


def fail_if_temp(path: Path) -> None:
    resolved = path.resolve()
    try:
        resolved.relative_to(FORBIDDEN_OUTPUT_ROOT)
    except ValueError:
        return
    raise SystemExit(f"Refusing to write useful report asset under temp: {resolved}")


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


def normalize_orders(path: Path | None, target_date: str, dictionary: pd.DataFrame, checks: list[Check], label: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not path or not path.exists():
        checks.append(Check(f"{label}_orders_file", "fail", "missing"))
        return pd.DataFrame(), {"date_pt": target_date, "sales": 0.0, "units": 0, "orders": None, "rows": 0, "source": label}
    df = pd.read_csv(path, dtype=str)
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
        df = df[~df[status_col].eq("Cancelled")]
    if df.empty:
        checks.append(Check(f"{label}_orders_rows", "fail", f"no rows for {target_date}; input_rows={before}"))
        return df, {"date_pt": target_date, "sales": 0.0, "units": 0, "orders": None, "rows": 0, "source": label}
    qty_col = "quantity"
    price_col = "item-price" if "item-price" in df.columns else "item_price"
    if qty_col not in df.columns or price_col not in df.columns:
        checks.append(Check(f"{label}_orders_columns", "fail", "missing quantity or item-price/item_price"))
        return pd.DataFrame(), {"date_pt": target_date, "sales": 0.0, "units": 0, "orders": None, "rows": 0, "source": label}
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
    summary = {
        "date_pt": target_date,
        "sales": round(float(df["item_price"].sum()), 2),
        "units": int(df["quantity"].sum()),
        "orders": orders,
        "rows": int(len(df)),
        "source": label,
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


def render_html(target_date: str, prior_date: str, current: dict[str, Any], prior: dict[str, Any], spend: dict[str, dict[str, Any]], deals: dict[str, dict[str, Any]], comp: pd.DataFrame) -> str:
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
    kpis = [
        ("Sales", fmt_money(current["sales"], 0), f"{delta_money(sales_delta)} vs {prior_date} ({sales_pct * 100:+.1f}%)", color(sales_delta)),
        ("Units", fmt_int(current["units"]), f"{delta_int(units_delta)} vs {prior_date} ({units_pct * 100:+.1f}%)", color(units_delta)),
        ("PPC Spend", fmt_money(cur_spend, 0), f"{delta_money(spend_delta)} vs {prior_date} ({spend_pct * 100:+.1f}%)", color(spend_delta, lower_good=True)),
        ("TACOS", fmt_pct(cur_tacos), f"{delta_pp(tacos_delta)} vs {prior_date}", color(tacos_delta, lower_good=True)),
    ]
    kpi_html = "".join(
        f"""
    <td style='width:25%;vertical-align:top;padding:0 8px 12px 0'>
      <div style='background:#f8fbff;border:1px solid #dbeafe;border-radius:10px;padding:14px'>
        <div style='font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#475467;font-weight:700'>{escape(label)}</div>
        <div style='font-size:24px;font-weight:800;margin:7px 0;color:#111827'>{escape(value)}</div>
        <div style='font-size:13px;color:{line_color};font-weight:700'>{escape(line)}</div>
      </div>
    </td>""" for label, value, line, line_color in kpis
    )
    def deal_text(day: str) -> str:
        item = deals[day]
        if item["status"] == "Unverified":
            return "Unverified — report blocked"
        if item["status"] == "None":
            return "No Lightning Deal or Best Deal"
        return "; ".join(f"{event['deal_type']}: {event['name']}" for event in item["events"])

    deal_rows = "".join(
        f"<tr><td style='padding:9px;border:1px solid #e5e7eb;font-weight:700'>{day}</td><td style='padding:9px;border:1px solid #e5e7eb'>{escape(deal_text(day))}</td></tr>"
        for day in [target_date, prior_date]
    )
    render = comp.sort_values(["current_sales", "prior_sales"], ascending=[False, False]).head(12)
    max_sales = max(float(render["current_sales"].max() or 0), float(render["prior_sales"].max() or 0), 1.0)
    rows = []
    for _, r in render.iterrows():
        d = float(r["sales_delta"])
        pdlt = r.get("sales_delta_pct")
        pct_s = "n/a" if pd.isna(pdlt) else f"{float(pdlt) * 100:+.1f}%"
        cw = max(2, min(100, float(r["current_sales"]) / max_sales * 100))
        pw = max(2, min(100, float(r["prior_sales"]) / max_sales * 100))
        rows.append(f"""
      <tr>
        <td style='padding:9px;border:1px solid #e5e7eb;font-weight:700'>{escape(str(r['collection']))}</td>
        <td style='padding:9px;border:1px solid #e5e7eb;text-align:right'>{fmt_money(r['current_sales'], 0)}<br><span style='font-size:11px;color:#667085'>{fmt_int(r['current_units'])} units</span></td>
        <td style='padding:9px;border:1px solid #e5e7eb;text-align:right'>{fmt_money(r['prior_sales'], 0)}<br><span style='font-size:11px;color:#667085'>{fmt_int(r['prior_units'])} units</span></td>
        <td style='padding:9px;border:1px solid #e5e7eb;text-align:right;color:{color(d)};font-weight:700'>{delta_money(d)}<br><span style='font-size:11px'>{pct_s}</span></td>
        <td style='padding:9px;border:1px solid #e5e7eb;min-width:170px'>
          <div style='font-size:11px;color:#667085;margin-bottom:3px'>Current</div><div style='background:#dbeafe;height:10px;border-radius:8px;overflow:hidden'><div style='width:{cw:.1f}%;background:#2563eb;height:10px'></div></div>
          <div style='font-size:11px;color:#667085;margin:5px 0 3px'>Prior</div><div style='background:#fee2e2;height:10px;border-radius:8px;overflow:hidden'><div style='width:{pw:.1f}%;background:#ef4444;height:10px'></div></div>
        </td>
      </tr>""")
    return f"""
<meta charset="utf-8">
<div style="font-family:Arial,Helvetica,sans-serif;color:#242424;max-width:980px;margin:0 auto;background:#fff">
  <div style="background:#111827;color:#fff;padding:24px 26px;margin-bottom:14px">
    <h1 style="margin:0 0 8px;font-size:26px;line-height:1.2">Mellanni Daily Sales Report</h1>
    <div style="font-size:13px;color:#dbe4f0">{target_date} PT · Compared to same weekday last week ({prior_date})</div>
  </div>
  <div style="border-left:4px solid #f97316;background:#fff7ed;padding:11px 14px;margin:0 0 20px;font-weight:700">Prepared by Sergey's AI helper.</div>
  <h2 style="font-size:20px;margin:18px 0 10px;color:#111827">Executive snapshot</h2>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;margin-bottom:8px"><tr>{kpi_html}</tr></table>
  <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;margin:14px 0 18px;font-size:13px">
    <tr style="background:#1f2937;color:#fff"><th style="padding:9px;text-align:left;border:1px solid #1f2937">Metric</th><th style="padding:9px;text-align:right;border:1px solid #1f2937">{target_date}</th><th style="padding:9px;text-align:right;border:1px solid #1f2937">{prior_date}</th><th style="padding:9px;text-align:right;border:1px solid #1f2937">Change</th></tr>
    <tr><td style="padding:9px;border:1px solid #e5e7eb;font-weight:700">Sales</td><td style="padding:9px;border:1px solid #e5e7eb;text-align:right">{fmt_money(current['sales'], 2)}</td><td style="padding:9px;border:1px solid #e5e7eb;text-align:right">{fmt_money(prior['sales'], 2)}</td><td style="padding:9px;border:1px solid #e5e7eb;text-align:right;color:{color(sales_delta)};font-weight:700">{delta_money(sales_delta)} ({sales_pct * 100:+.1f}%)</td></tr>
    <tr><td style="padding:9px;border:1px solid #e5e7eb;font-weight:700">Units</td><td style="padding:9px;border:1px solid #e5e7eb;text-align:right">{fmt_int(current['units'])}</td><td style="padding:9px;border:1px solid #e5e7eb;text-align:right">{fmt_int(prior['units'])}</td><td style="padding:9px;border:1px solid #e5e7eb;text-align:right;color:{color(units_delta)};font-weight:700">{delta_int(units_delta)} ({units_pct * 100:+.1f}%)</td></tr>
    <tr><td style="padding:9px;border:1px solid #e5e7eb;font-weight:700">PPC Spend</td><td style="padding:9px;border:1px solid #e5e7eb;text-align:right">{fmt_money(cur_spend, 2)}</td><td style="padding:9px;border:1px solid #e5e7eb;text-align:right">{fmt_money(pri_spend, 2)}</td><td style="padding:9px;border:1px solid #e5e7eb;text-align:right;color:{color(spend_delta, lower_good=True)};font-weight:700">{delta_money(spend_delta)} ({spend_pct * 100:+.1f}%)</td></tr>
    <tr><td style="padding:9px;border:1px solid #e5e7eb;font-weight:700">TACOS</td><td style="padding:9px;border:1px solid #e5e7eb;text-align:right">{fmt_pct(cur_tacos)}</td><td style="padding:9px;border:1px solid #e5e7eb;text-align:right">{fmt_pct(pri_tacos)}</td><td style="padding:9px;border:1px solid #e5e7eb;text-align:right;color:{color(tacos_delta, lower_good=True)};font-weight:700">{delta_pp(tacos_delta)}</td></tr>
  </table>
  <h2 style="font-size:20px;margin:20px 0 10px;color:#111827">Deal calendar check</h2>
  <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;margin-bottom:18px;font-size:13px">
    <tr style="background:#1f2937;color:#fff"><th style="padding:9px;text-align:left;border:1px solid #1f2937">Pacific date</th><th style="padding:9px;text-align:left;border:1px solid #1f2937">Lightning / Best Deal</th></tr>
    {deal_rows}
  </table>
  <h2 style="font-size:20px;margin:20px 0 10px;color:#111827">Collection breakdown</h2>
  <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;font-size:13px">
    <tr style="background:#1f2937;color:#fff"><th style="padding:9px;text-align:left;border:1px solid #1f2937">Collection</th><th style="padding:9px;text-align:right;border:1px solid #1f2937">{target_date}</th><th style="padding:9px;text-align:right;border:1px solid #1f2937">{prior_date}</th><th style="padding:9px;text-align:right;border:1px solid #1f2937">Sales change</th><th style="padding:9px;text-align:left;border:1px solid #1f2937">Visual</th></tr>
    {''.join(rows)}
  </table>
  <p style="font-size:12px;color:#667085;margin-top:14px">TACOS = PPC spend ÷ total sales. Source notes are recorded in the verification JSON.</p>
</div>
"""


def main() -> int:
    args = parse_args()
    report_dir = args.report_dir.resolve()
    fail_if_temp(report_dir)
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

    spend_path = resolve_default(report_dir, args.spend_hourly, ["ppc_hourly_current_prior.csv"])
    spend = read_spend(spend_path, [target_date, prior_date], checks)
    deal_path = resolve_default(report_dir, args.deal_calendar, ["deal_calendar_status.json"])
    deals = read_deal_calendar(deal_path, [target_date, prior_date], checks)

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
    html_body = render_html(target_date, prior_date, current_summary, prior_summary, spend, deals, comp)
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
        "checks": [asdict(c) for c in checks],
        "outputs": {"html": rel(html_path), "subject": rel(subject_path), "comparison": rel(comp_path), "verification": rel(verification_path)},
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
