#!/usr/bin/env python3
"""Build a validated daily competitor snapshot from fresh Keepa evidence.

Two inputs are supported:
  --mcp-snapshot  JSON saved from the Mellanni MCP tool mellanni_keepa_deal_snapshot
  --cache-dir     raw Keepa product caches written by the local Pi Keepa tool

This deterministic step validates freshness/brand identity and writes the compact
JSON consumed by the daily sales report builder.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from report_storage import require_tmp_artifact_path

PACIFIC = ZoneInfo("America/Los_Angeles")
DEFAULT_CONFIG = Path(__file__).parents[1] / "report_configs/daily_sales_competitors.json"
DEFAULT_CACHE = Path("/home/misunderstood/.pi/agent/keepa-cache")
REQUIRED_BRANDS = {"Amazon Basics", "Utopia Bedding", "CGK Unlimited", "Bedsure"}
CACHE_SOURCE = "Keepa product cache"
MCP_SOURCE = "Keepa deal snapshot (Mellanni MCP)"
CSV_MAP = {
    "amazon": (0, 2),
    "new_3p": (1, 2),
    "list_price": (4, 2),
    "lightning_deal": (8, 2),
    "new_fba": (10, 2),
    "buy_box": (18, 3),
    "prime_exclusive": (33, 2),
}
DEAL_SERIES = (("LIGHTNING_DEAL", "Lightning Deal", 8), ("PRIME_EXCLUSIVE", "Prime Exclusive", 33))


def require_tmp(path: Path) -> Path:
    return require_tmp_artifact_path(path, label="Daily report artifacts", error_type=SystemExit)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--mcp-snapshot", type=Path, help="Saved mellanni_keepa_deal_snapshot JSON")
    parser.add_argument("--mcp-history", type=Path, help="Saved mellanni_keepa_extract_history JSON keyed by ASIN -> metric")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-age-minutes", type=int, default=60)
    parser.add_argument("--target-date", type=str, default=None)
    return parser.parse_args()


def price_from_csv(raw: list[Any] | None, row_size: int) -> float | None:
    if not raw or len(raw) < row_size:
        return None
    value = raw[-row_size + 1]
    if not isinstance(value, (int, float)) or value <= 0:
        return None
    total_cents = float(value)
    if row_size == 3:
        shipping = raw[-1]
        if isinstance(shipping, (int, float)) and shipping > 0:
            total_cents += float(shipping)
    return round(total_cents / 100, 2)


def keepa_time(minutes: Any) -> datetime | None:
    if not isinstance(minutes, (int, float)) or minutes <= 0:
        return None
    return datetime.fromtimestamp((minutes + 21_564_000) * 60, timezone.utc).astimezone(PACIFIC)


def deal_intervals(raw: list[Any] | None, day_start: datetime, day_end: datetime) -> list[dict[str, Any]]:
    """Every positive deal-price run overlapping [day_start, day_end); end=None means ongoing."""
    points = []
    for i in range(0, len(raw or []) - 1, 2):
        moment, value = keepa_time(raw[i]), raw[i + 1]
        if moment is not None and isinstance(value, (int, float)):
            points.append((moment, value))
    intervals = []
    start = None
    prices: list[float] = []
    for moment, value in [*points, (None, -1)]:
        if value > 0:
            if start is None:
                start, prices = moment, []
            prices.append(value)
            continue
        if start is not None and start < day_end and (moment is None or moment > day_start):
            intervals.append({
                "deal_price": round(min(prices) / 100, 2),
                "start": start.isoformat(),
                "end": moment.isoformat() if moment else None,
                # Subtract instants: same-zone aware datetimes subtract wall-clock time across DST.
                "duration_hours": round((moment.timestamp() - start.timestamp()) / 3600, 1) if moment else None,
                "ongoing_at_last_update": moment is None,
            })
        start = None
    return intervals


def coupon_text(raw: Any) -> str | None:
    if isinstance(raw, list):
        raw = raw[0] if raw else None
    if isinstance(raw, (int, float)) and raw:
        return f"${raw / 100:.2f} off" if raw > 0 else f"{abs(raw):g}% off"
    return None


def summarize_price_points(
    points: list[tuple[datetime, float, bool]],
    day_start: datetime,
    day_end: datetime,
    source: str,
) -> dict[str, Any] | None:
    """Summarize the target-date Pacific price trajectory from points with carry-in."""
    in_or_before = sorted((pt for pt in points if pt[0] < day_end), key=lambda pt: pt[0])
    if not in_or_before:
        return None
    carry_in = [pt for pt in in_or_before if pt[0] <= day_start]
    in_window = [pt for pt in in_or_before if pt[0] > day_start]
    active: list[tuple[datetime, float, bool]] = []
    if carry_in:
        dt, price, _ = carry_in[-1]
        if price > 0:
            active.append((dt, price, True))
    for dt, price, carried in in_window:
        if price > 0:
            active.append((dt, price, carried))
    if not active:
        return None
    deduped: list[dict[str, Any]] = []
    for dt, price, carried in active:
        rounded = round(float(price), 2)
        if not deduped or deduped[-1]["price"] != rounded:
            deduped.append({"time_pt": dt.astimezone(PACIFIC).isoformat(), "price": rounded, "carried_in": carried})
    prices = [item["price"] for item in deduped]
    return {
        "target_date_price": prices[0],
        "target_date_price_end": prices[-1],
        "target_date_price_min": min(prices),
        "target_date_price_max": max(prices),
        "target_date_price_source": source,
        "target_date_price_points": deduped,
    }


def target_date_price_from_csv(
    csv_data: list[Any],
    day_start: datetime,
    day_end: datetime,
) -> dict[str, Any] | None:
    for name, index, row_size in (("buy_box", 18, 3), ("new_fba", 10, 2), ("new_3p", 1, 2), ("amazon", 0, 2)):
        raw = csv_data[index] if len(csv_data) > index else None
        if not raw or len(raw) < row_size:
            continue
        points: list[tuple[datetime, float, bool]] = []
        for i in range(0, len(raw) - row_size + 1, row_size):
            moment = keepa_time(raw[i])
            val = raw[i + 1]
            if moment is None or not isinstance(val, (int, float)) or val <= 0:
                continue
            cents = float(val)
            if row_size == 3:
                shipping = raw[i + 2]
                if isinstance(shipping, (int, float)) and shipping > 0:
                    cents += float(shipping)
            points.append((moment, cents / 100.0, False))
        summary = summarize_price_points(points, day_start, day_end, name)
        if summary is not None:
            return summary
    return None


def target_date_price_from_mcp_history(
    asin: str,
    asin_history: dict[str, Any],
    target_date: str,
) -> dict[str, Any]:
    day_start, day_end = day_bounds(target_date)
    price_summary: dict[str, Any] | None = None
    for metric in ("buy_box", "amazon_price", "new_fba", "new_3p"):
        payload = asin_history.get(metric)
        if not isinstance(payload, dict) or payload.get("status") != "success":
            continue
        points: list[tuple[datetime, float, bool]] = []
        for item in payload.get("history") or []:
            raw_date = item.get("date")
            val = item.get("price") if metric == "buy_box" and item.get("price") is not None else item.get("value")
            if not raw_date or not isinstance(val, (int, float)) or val <= 0:
                continue
            dt = datetime.fromisoformat(str(raw_date).replace("Z", "+00:00")).astimezone(PACIFIC)
            points.append((dt, float(val), bool(item.get("carried_forward"))))
        price_summary = summarize_price_points(points, day_start, day_end, metric)
        if price_summary is not None:
            break
    if price_summary is None:
        raise ValueError(f"{asin}: missing verified target-date Keepa price history for {target_date}")
    coupon_summary: str | None = None
    coupon_payload = asin_history.get("coupon")
    if isinstance(coupon_payload, dict) and coupon_payload.get("status") == "success":
        c_points = []
        for item in coupon_payload.get("history") or []:
            raw_date = item.get("date")
            one_time = item.get("one_time_coupon")
            if raw_date and isinstance(one_time, (int, float)):
                dt = datetime.fromisoformat(str(raw_date).replace("Z", "+00:00")).astimezone(PACIFIC)
                if dt < day_end:
                    c_points.append((dt, one_time))
        if c_points:
            c_points.sort(key=lambda x: x[0])
            coupon_summary = coupon_text(c_points[-1][1])
    price_summary["target_date_coupon"] = coupon_summary
    return price_summary


def is_best_or_lightning(deals: list[dict[str, str]]) -> bool:
    return any(word in f"{deal['type']} {deal['badge']}".upper() for deal in deals for word in ("BEST", "LIGHTNING"))


def deal_fields(date_deals: list[dict[str, Any]], active: list[dict[str, Any]]) -> dict[str, Any]:
    """Prefer deals proven on the target date; otherwise describe badges active at check time."""
    if date_deals:
        first = date_deals[0]
        return {
            "deals": [{"type": deal["type"], "badge": deal["badge"], "audience": "PRIME" if deal["type"] == "PRIME_EXCLUSIVE" else "ALL"} for deal in date_deals],
            "best_or_lightning_deal": True,
            "deal_start": first["start"],
            "deal_end": first["end"],
            "duration_hours": first["duration_hours"],
            "duration_status": "ongoing" if any(deal["end"] is None for deal in date_deals) else "complete",
        }
    deals = [{"type": str(item.get("type") or "Unknown"), "badge": str(item.get("badge") or item.get("type") or "Unknown"), "audience": str(item.get("access") or "Unknown")} for item in active]
    timed = next((item for item in active if item.get("end")), None)
    hours = None
    if timed and timed.get("start"):
        hours = round((datetime.fromisoformat(timed["end"]).timestamp() - datetime.fromisoformat(timed["start"]).timestamp()) / 3600, 1)
    return {
        "deals": deals,
        "best_or_lightning_deal": is_best_or_lightning(deals),
        "deal_start": timed.get("start") if timed else None,
        "deal_end": timed.get("end") if timed else None,
        "duration_hours": hours,
        "duration_status": "not_applicable" if not deals else "complete" if hours is not None else "end_only" if timed else "not_exposed_by_keepa",
    }


def validate_identity(entry: dict[str, str], asin: Any, brand: Any) -> str:
    expected = entry["asin"].strip().upper()
    if str(asin or "").strip().upper() != expected:
        raise ValueError(f"{expected}: payload ASIN mismatch; found {asin!r}")
    if str(brand or "").strip() != entry["brand"].strip():
        raise ValueError(f"{expected}: expected brand {entry['brand']!r}, found {brand!r}")
    return expected


def check_age(asin: str, checked_at: float, now: float, max_age_seconds: int) -> None:
    age_seconds = now - checked_at
    if checked_at <= 0 or age_seconds < 0 or age_seconds > max_age_seconds:
        raise ValueError(f"{asin}: Keepa evidence is stale or missing; age_seconds={age_seconds:.0f}")


def day_bounds(target_date: str) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(target_date).replace(tzinfo=PACIFIC)
    following = start.date() + timedelta(days=1)
    return start, datetime(following.year, following.month, following.day, tzinfo=PACIFIC)


def extract_product(
    entry: dict[str, str],
    product: dict[str, Any],
    now: float,
    max_age_seconds: int,
    *,
    target_date: str | None = None,
) -> dict[str, Any]:
    asin = validate_identity(entry, product.get("asin"), product.get("brand"))
    cached_at = float(product.get("_cached_at") or 0)
    check_age(asin, cached_at, now, max_age_seconds)

    csv_data = product.get("csv") or []
    prices = {
        name: price_from_csv(csv_data[index] if len(csv_data) > index else None, row_size)
        for name, (index, row_size) in CSV_MAP.items()
    }
    # Live prices only: an ended deal's price is never the current price.
    current_source = next((name for name in ("lightning_deal", "prime_exclusive", "buy_box", "new_fba", "new_3p", "amazon") if prices[name] is not None), None)
    if current_source is None:
        raise ValueError(f"{asin}: no current consumer price")

    date_deals = []
    date_price_info: dict[str, Any] = {}
    if target_date:
        day_start, day_end = day_bounds(target_date)
        date_deals = [
            {"type": kind, "badge": badge, **interval}
            for kind, badge, index in DEAL_SERIES
            for interval in deal_intervals(csv_data[index] if len(csv_data) > index else None, day_start, day_end)
        ]
        date_price_info = target_date_price_from_csv(csv_data, day_start, day_end) or {}
    active = []
    for item in product.get("deals") or []:
        prime_end = product.get("primeDealEndTime") if "PRIME" in str(item.get("dealType") or "").upper() else None
        start, end = keepa_time(item.get("startTime")), keepa_time(item.get("endTime") or prime_end)
        active.append({
            "type": item.get("dealType"), "badge": item.get("badge"), "access": item.get("accessType"),
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end and end.timestamp() > cached_at else None,
        })

    return {
        "brand": entry["brand"].strip(),
        "asin": asin,
        "label": entry.get("label") or product.get("title") or asin,
        "title": product.get("title"),
        "current_price": prices[current_source],
        "current_price_source": current_source,
        "list_price": prices["list_price"],
        "coupon": coupon_text(product.get("coupon")),
        **deal_fields(date_deals, active),
        **date_price_info,
        "date_deals": date_deals,
        "checked_at_utc": datetime.fromtimestamp(cached_at, timezone.utc).isoformat(),
        "source": CACHE_SOURCE,
    }


def extract_mcp_product(
    entry: dict[str, str],
    item: dict[str, Any],
    now: float,
    max_age_seconds: int,
    *,
    target_date: str | None = None,
    asin_history: dict[str, Any] | None = None,
) -> dict[str, Any]:
    asin = validate_identity(entry, item.get("asin"), item.get("brand"))
    checked_at = datetime.fromisoformat(str(item.get("checked_at_utc"))).timestamp()
    check_age(asin, checked_at, now, max_age_seconds)
    price = item.get("current_price")
    if not isinstance(price, (int, float)) or price <= 0:
        raise ValueError(f"{asin}: no current consumer price")
    date_deals = item.get("date_deals")
    active = item.get("active_deals")
    if not isinstance(date_deals, list) or not isinstance(active, list):
        raise ValueError(f"{asin}: snapshot lacks date_deals/active_deals lists")
    date_deals = [{**deal, "ongoing_at_last_update": deal.get("end") is None} for deal in date_deals]
    date_price_info: dict[str, Any] = {}
    if asin_history is not None:
        if not target_date:
            raise ValueError("MCP history requires target_date")
        date_price_info = target_date_price_from_mcp_history(asin, asin_history, target_date)
    return {
        "brand": entry["brand"].strip(),
        "asin": asin,
        "label": entry.get("label") or item.get("title") or asin,
        "title": item.get("title"),
        "current_price": round(float(price), 2),
        "current_price_source": item.get("current_price_source"),
        "list_price": item.get("list_price"),
        "coupon": coupon_text(item.get("coupon")),
        **deal_fields(date_deals, active),
        **date_price_info,
        "date_deals": date_deals,
        "checked_at_utc": item["checked_at_utc"],
        "source": MCP_SOURCE,
    }


def config_entries(config: dict[str, Any]) -> list[dict[str, str]]:
    entries = config.get("products")
    if not isinstance(entries, list) or not entries:
        raise ValueError("competitor config needs a non-empty products list")
    missing = REQUIRED_BRANDS - {str(item.get("brand") or "") for item in entries}
    if missing:
        raise ValueError("competitor config missing required brands: " + ", ".join(sorted(missing)))
    for entry in entries:
        if len(str(entry.get("asin") or "").strip()) != 10:
            raise ValueError(f"invalid competitor ASIN: {entry.get('asin')!r}")
    return entries


def snapshot(config: dict[str, Any], products: list[dict[str, Any]], target_date: str | None = None) -> dict[str, Any]:
    return {
        "status": "pass",
        "target_date": target_date,
        "marketplace": config.get("marketplace") or "Amazon.com",
        "required_brands": sorted(REQUIRED_BRANDS),
        "products": products,
    }


def build_snapshot(
    config: dict[str, Any],
    cache_dir: Path,
    max_age_minutes: int,
    *,
    now: float | None = None,
    target_date: str | None = None,
) -> dict[str, Any]:
    now = time.time() if now is None else now
    products = []
    for entry in config_entries(config):
        cache_path = cache_dir / f"{entry['asin'].strip().upper()}.json"
        if not cache_path.is_file():
            raise ValueError(f"missing Keepa cache for {entry['asin']}; fetch it first")
        products.append(extract_product(entry, json.loads(cache_path.read_text(encoding="utf-8")), now, max_age_minutes * 60, target_date=target_date))
    return snapshot(config, products, target_date)


def build_snapshot_from_mcp(
    config: dict[str, Any],
    payload: dict[str, Any],
    max_age_minutes: int,
    *,
    target_date: str,
    now: float | None = None,
    mcp_history: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if payload.get("status") != "success" or payload.get("date_pt") != target_date:
        raise ValueError(f"MCP snapshot must be a successful check for {target_date}; found {payload.get('date_pt')!r}")
    by_asin = {str(item.get("asin") or "").upper(): item for item in payload.get("products") or [] if isinstance(item, dict)}
    history_by_asin = {str(k).upper(): v for k, v in (mcp_history or {}).items() if isinstance(v, dict)}
    now = time.time() if now is None else now
    products = []
    for entry in config_entries(config):
        asin_key = entry["asin"].strip().upper()
        item = by_asin.get(asin_key)
        if item is None:
            raise ValueError(f"MCP snapshot lacks configured ASIN {entry['asin']}")
        checked_raw = item.get("checked_at_utc")
        checked_pt_day = datetime.fromisoformat(str(checked_raw)).astimezone(PACIFIC).date().isoformat() if checked_raw else None
        if mcp_history is None and checked_pt_day != target_date:
            raise ValueError(
                f"{asin_key}: check date {checked_pt_day} != target_date {target_date}; "
                "--mcp-history (mellanni_keepa_extract_history) is required so D+1 live prices are never used"
            )
        asin_hist = history_by_asin.get(asin_key) if mcp_history is not None else None
        if mcp_history is not None and asin_hist is None:
            raise ValueError(f"MCP history lacks configured ASIN {entry['asin']}")
        products.append(extract_mcp_product(entry, item, now, max_age_minutes * 60, target_date=target_date, asin_history=asin_hist))
    return snapshot(config, products, target_date)


def main() -> int:
    args = parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    if args.mcp_snapshot:
        if not args.target_date:
            raise SystemExit("--mcp-snapshot requires --target-date")
        history_payload = json.loads(args.mcp_history.read_text(encoding="utf-8")) if args.mcp_history else None
        result = build_snapshot_from_mcp(
            config,
            json.loads(args.mcp_snapshot.read_text(encoding="utf-8")),
            args.max_age_minutes,
            target_date=args.target_date,
            mcp_history=history_payload,
        )
    else:
        result = build_snapshot(config, args.cache_dir, args.max_age_minutes, target_date=args.target_date)
    output = require_tmp(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "pass", "products": len(result["products"]), "output": str(output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
