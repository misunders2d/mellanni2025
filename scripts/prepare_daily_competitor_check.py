#!/usr/bin/env python3
"""Build a validated daily competitor snapshot from fresh Keepa product caches.

Fetch configured ASINs with the approved Keepa tool first. This deterministic
step reads only cached product JSON, validates freshness/brand identity, and
writes the compact JSON consumed by the daily sales report builder.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from report_storage import require_tmp_artifact_path

PACIFIC = ZoneInfo("America/Los_Angeles")
DEFAULT_CONFIG = Path(__file__).parents[1] / "report_configs/daily_sales_competitors.json"
DEFAULT_CACHE = Path("/home/misunderstood/.pi/agent/keepa-cache")
REQUIRED_BRANDS = {"Amazon Basics", "Utopia Bedding", "CGK Unlimited"}
CSV_MAP = {
    "amazon": (0, 2),
    "new_3p": (1, 2),
    "list_price": (4, 2),
    "lightning_deal": (8, 2),
    "new_fba": (10, 2),
    "buy_box": (18, 3),
    "prime_exclusive": (33, 2),
}


def require_tmp(path: Path) -> Path:
    return require_tmp_artifact_path(path, label="Daily report artifacts", error_type=SystemExit)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE)
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


def deal_from_csv(
    raw: list[Any] | None, row_size: int, day_start: datetime, day_end: datetime
) -> tuple[float | None, datetime | None, datetime | None, float | None]:
    if not raw or len(raw) < row_size:
        return None, None, None, None
    for i in range(0, len(raw), row_size):
        start_min = raw[i]
        val = raw[i + 1]
        if not isinstance(start_min, (int, float)) or not isinstance(val, (int, float)):
            continue
        start_dt = datetime.fromtimestamp((start_min + 21_564_000) * 60, timezone.utc).astimezone(PACIFIC)
        if i + row_size < len(raw):
            next_min = raw[i + row_size]
            end_dt = datetime.fromtimestamp((next_min + 21_564_000) * 60, timezone.utc).astimezone(PACIFIC)
        else:
            end_dt = datetime.now(PACIFIC)
        if val > 0 and start_dt < day_end and end_dt > day_start:
            price = round(float(val) / 100, 2)
            duration_hours = round((end_dt - start_dt).total_seconds() / 3600, 1)
            return price, start_dt, end_dt, duration_hours
    return None, None, None, None


def deal_end(product: dict[str, Any], checked_at: float) -> str | None:
    raw = product.get("primeDealEndTime")
    if not isinstance(raw, (int, float)) or raw <= 0:
        return None
    end_epoch = (raw + 21_564_000) * 60
    if end_epoch <= checked_at:
        return None
    return datetime.fromtimestamp(end_epoch, timezone.utc).isoformat()


def extract_product(
    entry: dict[str, str],
    product: dict[str, Any],
    now: float,
    max_age_seconds: int,
    *,
    target_date: str | None = None,
) -> dict[str, Any]:
    asin = entry["asin"].strip().upper()
    actual_asin = str(product.get("asin") or "").strip().upper()
    if actual_asin != asin:
        raise ValueError(f"{asin}: cache payload ASIN mismatch; found {actual_asin!r}")
    expected_brand = entry["brand"].strip()
    actual_brand = str(product.get("brand") or "").strip()
    if actual_brand != expected_brand:
        raise ValueError(f"{asin}: expected brand {expected_brand!r}, found {actual_brand!r}")
    cached_at = float(product.get("_cached_at") or 0)
    age_seconds = now - cached_at
    if cached_at <= 0 or age_seconds < 0 or age_seconds > max_age_seconds:
        raise ValueError(f"{asin}: Keepa cache is stale or missing; age_seconds={age_seconds:.0f}")

    csv_data = product.get("csv") or []
    prices = {
        name: price_from_csv(csv_data[index] if len(csv_data) > index else None, row_size)
        for name, (index, row_size) in CSV_MAP.items()
    }

    target_deal = None
    if target_date:
        day_start = datetime.fromisoformat(target_date).replace(tzinfo=PACIFIC)
        day_end = day_start + timedelta(days=1)
        ld_price, ld_start, ld_end, ld_dur = deal_from_csv(csv_data[8] if len(csv_data) > 8 else None, 2, day_start, day_end)
        pe_price, pe_start, pe_end, pe_dur = deal_from_csv(csv_data[33] if len(csv_data) > 33 else None, 2, day_start, day_end)
        if ld_price is not None:
            prices["lightning_deal"] = ld_price
            target_deal = {
                "deals": [{"type": "LIGHTNING_DEAL", "badge": "Lightning Deal", "audience": "ALL"}],
                "best_or_lightning": True,
                "deal_start": ld_start.isoformat(),
                "deal_end": ld_end.isoformat(),
                "duration_hours": ld_dur,
                "duration_status": "complete",
            }
        elif pe_price is not None:
            prices["prime_exclusive"] = pe_price
            target_deal = {
                "deals": [{"type": "PRIME_EXCLUSIVE", "badge": "Prime Exclusive", "audience": "PRIME"}],
                "best_or_lightning": True,
                "deal_start": pe_start.isoformat(),
                "deal_end": pe_end.isoformat(),
                "duration_hours": pe_dur,
                "duration_status": "complete",
            }

    current_source = next((name for name in ("lightning_deal", "prime_exclusive", "buy_box", "new_fba", "new_3p", "amazon") if prices[name] is not None), None)
    if current_source is None:
        raise ValueError(f"{asin}: no current consumer price")

    if target_deal:
        deals = target_deal["deals"]
        best_or_lightning = target_deal["best_or_lightning"]
        deal_start_val = target_deal["deal_start"]
        end_at = target_deal["deal_end"]
        duration_hours = target_deal["duration_hours"]
        duration_status = target_deal["duration_status"]
    else:
        raw_deals = product.get("deals") or []
        deals = [
            {
                "type": str(item.get("dealType") or "Unknown"),
                "badge": str(item.get("badge") or item.get("dealType") or "Unknown"),
                "audience": str(item.get("accessType") or "Unknown"),
            }
            for item in raw_deals
        ]
        best_or_lightning = any(
            "BEST" in item["type"].upper()
            or "LIGHTNING" in item["type"].upper()
            or "BEST" in item["badge"].upper()
            or "LIGHTNING" in item["badge"].upper()
            for item in deals
        )
        end_at = deal_end(product, cached_at) if deals else None
        deal_start_val = None
        duration_hours = None
        duration_status = "not_applicable" if not deals else "end_only" if end_at else "not_exposed_by_keepa"

    coupon_raw = product.get("coupon")
    if isinstance(coupon_raw, list):
        coupon_raw = coupon_raw[0] if coupon_raw else None
    coupon = None
    if isinstance(coupon_raw, (int, float)) and coupon_raw:
        coupon = f"${coupon_raw / 100:.2f} off" if coupon_raw > 0 else f"{abs(coupon_raw):g}% off"

    return {
        "brand": expected_brand,
        "asin": asin,
        "label": entry.get("label") or product.get("title") or asin,
        "title": product.get("title"),
        "current_price": prices[current_source],
        "current_price_source": current_source,
        "list_price": prices["list_price"],
        "coupon": coupon,
        "deals": deals,
        "best_or_lightning_deal": best_or_lightning,
        "deal_start": deal_start_val,
        "deal_end": end_at,
        "duration_hours": duration_hours,
        "duration_status": duration_status,
        "checked_at_utc": datetime.fromtimestamp(cached_at, timezone.utc).isoformat(),
        "source": "Keepa product cache",
    }


def build_snapshot(
    config: dict[str, Any],
    cache_dir: Path,
    max_age_minutes: int,
    *,
    now: float | None = None,
    target_date: str | None = None,
) -> dict[str, Any]:
    entries = config.get("products")
    if not isinstance(entries, list) or not entries:
        raise ValueError("competitor config needs a non-empty products list")
    brands = {str(item.get("brand") or "") for item in entries}
    missing = REQUIRED_BRANDS - brands
    if missing:
        raise ValueError("competitor config missing required brands: " + ", ".join(sorted(missing)))
    now = time.time() if now is None else now
    products = []
    for entry in entries:
        asin = str(entry.get("asin") or "").strip().upper()
        if len(asin) != 10:
            raise ValueError(f"invalid competitor ASIN: {asin!r}")
        cache_path = cache_dir / f"{asin}.json"
        if not cache_path.is_file():
            raise ValueError(f"missing Keepa cache for {asin}; fetch it first")
        products.append(
            extract_product(
                entry,
                json.loads(cache_path.read_text(encoding="utf-8")),
                now,
                max_age_minutes * 60,
                target_date=target_date,
            )
        )
    return {
        "status": "pass",
        "marketplace": config.get("marketplace") or "Amazon.com",
        "required_brands": sorted(REQUIRED_BRANDS),
        "products": products,
    }


def main() -> int:
    args = parse_args()
    snapshot = build_snapshot(
        json.loads(args.config.read_text(encoding="utf-8")),
        args.cache_dir,
        args.max_age_minutes,
        target_date=args.target_date,
    )
    output = require_tmp(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "pass", "products": len(snapshot["products"]), "output": str(output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
