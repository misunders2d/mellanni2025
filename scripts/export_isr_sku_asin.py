"""Download Cloud data and export only the isr_sku_asin Full Stats sheet.

This is the non-GUI equivalent of:
1. Restock -> Cloud data -> Load dataset
2. Full stats -> select all products/sizes/colors -> Download product
3. Keep only the isr_sku_asin worksheet in product_stats.xlsx
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

import pandas as pd

# Allow both `python scripts/export_isr_sku_asin.py` and module execution.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from classes.dataset import Dataset
from classes.product import Product
from common import excluded_collections, user_folder
from utils import mellanni_modules as mm

SHEET_NAME = "isr_sku_asin"
REPORT_DAYS = 60
DEFAULT_OUTPUT_DIR = Path(user_folder)
LOCAL_DATASET_DIR = Path(user_folder) / "dataset"
CLOUD_CACHE_FILES = (
    "br_asin.csv",
    "br.csv",
    "orders.csv",
    "inventory.csv",
    "inventory_history.csv",
    "advertised_product.csv",
    "purchased_product.csv",
    "attribution.csv",
    "dsp.csv",
    "sba.csv",
    "sbv.csv",
    "sd.csv",
    "fba_shipments.csv",
    "promotions.csv",
    "returns.csv",
    "fees.csv",
    "warehouse.csv",
    "changelog.csv",
    "incoming.csv",
    "cogs.csv",
    "dictionary.csv",
    "pricing.csv",
)
LOCAL_DATASET_ATTRIBUTES = (
    "br_asin",
    "br",
    "orders",
    "inventory",
    "inventory_history",
    "advertised_product",
    "purchased_product",
    "attribution",
    "dsp",
    "sba",
    "sbv",
    "sd",
    "promotions",
    "returns",
    "fees",
    "warehouse",
    "changelog",
    "incoming",
    "cogs",
    "dictionary",
    "pricing",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Mellanni Cloud data and create product_stats.xlsx with only isr_sku_asin."
    )
    parser.add_argument(
        "--start",
        help="Optional start date override: YYYY-MM-DD (must be used with --end)",
    )
    parser.add_argument(
        "--end",
        help="Optional end date override: YYYY-MM-DD (must be used with --start)",
    )
    parser.add_argument("--market", default="US", help="Marketplace (default: US)")
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional output XLSX path; default: ~/temp/isr_START_to_END.xlsx",
    )
    return parser.parse_args()


def validate_dates(start: str, end: str) -> None:
    start_date = pd.to_datetime(start, format="%Y-%m-%d", errors="raise").date()
    end_date = pd.to_datetime(end, format="%Y-%m-%d", errors="raise").date()
    if start_date > end_date:
        raise ValueError(f"Start date {start} must not be later than end date {end}.")


def resolve_report_period(
    market: str, start_override: str | None, end_override: str | None
) -> tuple[str, str]:
    if bool(start_override) != bool(end_override):
        raise ValueError("Use --start and --end together, or omit both for the latest 60 days.")
    if start_override and end_override:
        validate_dates(start_override, end_override)
        return start_override, end_override

    if not re.fullmatch(r"[A-Z]{2}", market):
        raise ValueError(f"Invalid marketplace: {market!r}")

    # Find the latest inventory snapshot available in Cloud, not the current date.
    probe = Dataset(
        start="2000-01-01",
        end="2000-01-01",
        market=market,
        local_data=False,
        save=False,
    )
    query = f"""
        SELECT MAX(DATE(snapshot_date)) AS latest_date
        FROM `reports.fba_inventory_planning`
        WHERE marketplace = UPPER("{market}")
          AND LOWER(condition) != "used"
    """
    result = probe.client.query(query).to_dataframe()
    if result.empty or pd.isna(result.loc[0, "latest_date"]):
        raise RuntimeError(f"No Cloud inventory dates found for marketplace {market}.")

    latest_date = pd.to_datetime(result.loc[0, "latest_date"]).date()
    # Exclude the newest snapshot because that day may still be incomplete.
    # Example: latest Cloud date 2026-08-31 -> report ends on 2026-08-30.
    end_date = latest_date - pd.Timedelta(days=1)
    start_date = end_date - pd.Timedelta(days=REPORT_DAYS - 1)
    return start_date.isoformat(), end_date.isoformat()


def default_output_path(start: str, end: str) -> Path:
    return DEFAULT_OUTPUT_DIR / f"isr_{start}_to_{end}.xlsx"


def all_gui_selectable_asins(dictionary: pd.DataFrame) -> list[str]:
    """Match the GUI's 'Select all products/sizes/colors' selection."""
    required = {"asin", "collection"}
    missing = required.difference(dictionary.columns)
    if missing:
        raise RuntimeError(f"Dictionary is missing columns: {', '.join(sorted(missing))}")

    selected = dictionary.loc[
        ~dictionary["collection"].isin(excluded_collections), "asin"
    ].dropna()
    asins = sorted({str(value).strip() for value in selected if str(value).strip()})
    if not asins:
        raise RuntimeError("No ASINs were found in the downloaded dictionary.")
    return asins


def verify_cloud_cache_updated(download_started_at: float) -> None:
    """Do not permit Local data processing unless every Cloud cache file is fresh."""
    missing: list[str] = []
    stale: list[str] = []
    empty: list[str] = []

    for filename in CLOUD_CACHE_FILES:
        path = LOCAL_DATASET_DIR / filename
        if not path.exists():
            missing.append(filename)
        elif path.stat().st_size == 0:
            empty.append(filename)
        elif path.stat().st_mtime < download_started_at - 2:
            stale.append(filename)

    problems = []
    if missing:
        problems.append(f"missing: {', '.join(missing)}")
    if empty:
        problems.append(f"empty: {', '.join(empty)}")
    if stale:
        problems.append(f"not updated: {', '.join(stale)}")
    if problems:
        raise RuntimeError(
            "Cloud data update was not successful; Local data stage was canceled ("
            + "; ".join(problems)
            + ")."
        )


def verify_local_dataset_loaded(dataset: Dataset) -> None:
    missing = [
        name
        for name in LOCAL_DATASET_ATTRIBUTES
        if not hasattr(dataset, name) or getattr(dataset, name) is None
    ]
    if missing:
        raise RuntimeError(
            "Local data was not loaded completely; Full Stats was canceled. Missing: "
            + ", ".join(missing)
        )


def export_single_sheet(df: pd.DataFrame, output: Path) -> None:
    output = output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name=SHEET_NAME, index=False)
        mm.format_header(df, writer, SHEET_NAME)


def run(
    market: str,
    output: Path | None = None,
    start_override: str | None = None,
    end_override: str | None = None,
) -> Path:
    started = time.perf_counter()

    print(f"[1/6] Finding the latest available Cloud inventory date for {market}...")
    start, end = resolve_report_period(market, start_override, end_override)
    output = output or default_output_path(start, end)
    print(
        f"      Report period: {start} through {end} ({REPORT_DAYS} calendar days; "
        "newest Cloud day excluded)."
    )

    print(f"[2/6] Downloading Cloud data: {start} through {end}; market: {market}...")
    download_started_at = time.time()
    cloud_dataset = Dataset(
        start=start,
        end=end,
        market=market,
        local_data=False,
        save=True,
    )
    # Deliberately synchronous: every Cloud dataset finishes before Full Stats starts.
    cloud_dataset.query_sync()
    verify_cloud_cache_updated(download_started_at)
    print("      Cloud data update verified successfully.")

    # BigQuery returns some date columns as datetime64. Several legacy Full Stats
    # groupby(...).sum() operations cannot aggregate that dtype with current pandas.
    # The GUI workflow persists Cloud data as CSV; reload those files before the
    # calculation to normalize their dtypes and keep the stages strictly sequential.
    print("[3/6] Cloud data downloaded. Reloading the saved dataset for Full Stats...")
    dataset = Dataset(
        start=start,
        end=end,
        market=market,
        local_data=True,
        save=False,
    )
    dataset.query_sync()
    verify_local_dataset_loaded(dataset)
    print("      Local data loaded successfully.")

    asins = all_gui_selectable_asins(dataset.dictionary)
    print(f"[4/6] Preparing Full Stats for {len(asins)} ASINs...")

    product = Product(asin=asins, dataset=dataset, start=start, end=end)
    product.populate_loop()

    print("[5/6] Calculating Full Stats for all selected products, sizes and colors...")
    product.calculate_loop()
    product.summarize()

    isr = product.stats.get(SHEET_NAME)
    if isr is None:
        raise RuntimeError(f"Full Stats did not create the {SHEET_NAME!r} table.")

    expected_columns = ["sku", "asin", "marketplace", "in stock"]
    missing = [column for column in expected_columns if column not in isr.columns]
    if missing:
        raise RuntimeError(f"{SHEET_NAME} is missing columns: {', '.join(missing)}")
    isr = isr[expected_columns].copy()

    print(f"[6/6] Writing one worksheet ({SHEET_NAME}) to {output}...")
    export_single_sheet(isr, output)

    elapsed = time.perf_counter() - started
    final_output = output.expanduser().resolve()
    print(f"Done: {final_output}")
    print(f"Rows: {len(isr):,}; elapsed: {elapsed / 60:.1f} minutes")
    return final_output


def main() -> int:
    args = parse_args()
    try:
        run(
            market=args.market.upper(),
            output=args.output,
            start_override=args.start,
            end_override=args.end,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
