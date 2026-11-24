"""Normalize retrieved H10 tracked keywords/history into weekly builder inputs.

Run after prepare_weekly_ceo_sources.py and before the strict weekly runner.
All exports and outputs remain under /tmp. Exact Pacific week-end observations
only; no interpolation. Current H10 demand and historical ranks stay distinct.
Optional reverse-ASIN payloads under sources/h10/*.json enrich blank metrics
without ever altering tracked ASIN organic rank history.
"""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from report_storage import require_tmp_artifact_path


def parse_rank(value: Any) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        n = int(round(float(value)))
        return n if n > 0 else None
    text = str(value).strip()
    if text.isdigit():
        n = int(text)
        return n if n > 0 else None
    return None


def load_scope(scope_path: Path, available_asins: set[str]) -> list[tuple[str, str]]:
    raw = json.loads(scope_path.read_text(encoding="utf-8"))
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add_pair(asin: str, phrase: str) -> None:
        a = str(asin).strip()
        p = str(phrase).strip().lower()
        if not a or not p:
            raise ValueError(f"Invalid scope entry: asin={asin!r}, phrase={phrase!r}")
        key = (a, p)
        if key not in seen:
            seen.add(key)
            pairs.append(key)

    if isinstance(raw, dict):
        if "asin" in raw and "phrases" in raw:
            for ph in raw["phrases"]:
                add_pair(raw["asin"], ph)
        elif "pairs" in raw:
            for item in raw["pairs"]:
                add_pair(item["asin"], item["phrase"])
        else:
            raise ValueError(f"Unsupported dict scope schema in {scope_path}")
    elif isinstance(raw, list):
        if all(isinstance(x, str) for x in raw):
            if len(available_asins) != 1:
                raise ValueError(
                    f"String-list scope requires exactly 1 tracked ASIN, found {sorted(available_asins)}"
                )
            only_asin = next(iter(available_asins))
            for ph in raw:
                add_pair(only_asin, ph)
        elif all(isinstance(x, dict) and "asin" in x and "phrase" in x for x in raw):
            for item in raw:
                add_pair(item["asin"], item["phrase"])
        else:
            raise ValueError(f"Unsupported list scope schema in {scope_path}")
    else:
        raise ValueError(f"Unsupported scope JSON type in {scope_path}")
    if not pairs:
        raise ValueError(f"Empty scope file: {scope_path}")
    return pairs


def load_tracked_pages(source: Path) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, bool], set[str], dict[str, str]]:
    tracked_files = sorted(source.glob("tracked-*.json"))
    if not tracked_files:
        raise ValueError(f"No tracked-*.json files found in {source}")
    raw_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    has_exhausted_page: dict[str, bool] = {}
    page_row_counts: dict[str, int] = {}
    final_cumulative: dict[str, int] = {}
    seen_asins: set[str] = set()
    for path in tracked_files:
        doc = json.loads(path.read_text(encoding="utf-8"))
        payload = doc["data"] if isinstance(doc, dict) and "data" in doc else doc
        pagination = payload.get("pagination") or {}
        if pagination.get("truncated"):
            raise ValueError(f"Truncated tracked keyword export: {path.name}")
        rows = payload.get("rows") or []
        page_asins = {str(r["asin"]).strip() for r in rows if r.get("asin")}
        seen_asins.update(page_asins)
        page_complete = not bool(pagination.get("has_more"))
        for asin in page_asins:
            has_exhausted_page[asin] = has_exhausted_page.get(asin, False) or page_complete
            page_row_counts[asin] = page_row_counts.get(asin, 0) + sum(1 for r in rows if str(r.get("asin") or "").strip() == asin)
            cum = pagination.get("cumulative_count")
            if page_complete and isinstance(cum, int):
                final_cumulative[asin] = cum
        for row in rows:
            phrase = str(row["keywords_phrase"]).strip().lower()
            asin = str(row["asin"]).strip()
            raw_by_pair.setdefault((asin, phrase), row)
    still_has_more = {asin: not exhausted for asin, exhausted in has_exhausted_page.items()}
    chain_errors: dict[str, str] = {}
    for asin, expected_cum in final_cumulative.items():
        if page_row_counts.get(asin, 0) != expected_cum:
            chain_errors[asin] = f"fetched {page_row_counts.get(asin, 0)} rows != cumulative_count {expected_cum}"
    return raw_by_pair, still_has_more, seen_asins, chain_errors


def load_histories(source: Path) -> dict[str, dict[str, Any]]:
    histories: dict[str, dict[str, Any]] = {}
    hist_dir = source / "rank-histories"
    if not hist_dir.is_dir():
        raise ValueError(f"Missing raw rank-histories directory: {hist_dir}")
    for path in sorted(hist_dir.glob("*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        payload = doc["data"] if isinstance(doc, dict) and "data" in doc else doc
        for row in payload.get("rows") or []:
            asin = str(row.get("asin") or "").strip()
            phrase = str(row.get("keywords_phrase") or "").strip().lower()
            if not asin or not phrase:
                continue
            key = f"{asin}|{phrase}"
            obs = row.get("organic_rank_history") or []
            existing = histories.get(key)
            if existing is None:
                histories[key] = {"organic_rank_history": obs}
            else:
                prev_obs = existing.get("organic_rank_history") or []
                if prev_obs and obs and prev_obs != obs:
                    raise ValueError(f"Conflicting raw rank histories for {key}")
                if not prev_obs and obs:
                    histories[key] = {"organic_rank_history": obs}
    if not histories:
        raise ValueError(f"No raw rank history rows found under {hist_dir}")
    return histories


def load_reverse_asin_index(source: Path) -> dict[str, dict[str, Any]]:
    h10_dir = source / "h10"
    if not h10_dir.is_dir():
        return {}
    candidates: dict[str, list[dict[str, Any]]] = {}
    for path in sorted(h10_dir.glob("*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        keywords = (doc.get("data") or {}).get("keywords") or []
        for kw in keywords:
            phrase = str(kw.get("phrase") or "").strip().lower()
            if phrase:
                candidates.setdefault(phrase, []).append(kw)

    def sort_key(kw: dict[str, Any]) -> tuple[int, float, float]:
        rank = parse_rank(kw.get("organic_rank"))
        sales = float(kw.get("keyword_sales_weekly") or 0)
        vol = float(kw.get("search_volume") or 0)
        return (rank if rank is not None else 10**9, -sales, -vol)

    return {phrase: min(rows, key=sort_key) for phrase, rows in candidates.items()}


def build_child_asin_sqp_scope(
    source: Path,
    week_end: date,
    raw_by_pair: dict[tuple[str, str], dict[str, Any]],
    *,
    top_selling_asins: int = 15,
    limit: int = 20,
) -> dict[str, Any]:
    if top_selling_asins <= 0:
        raise ValueError(f"top_selling_asins must be > 0, got {top_selling_asins}")
    if limit <= 0:
        raise ValueError(f"limit must be > 0, got {limit}")
    asin_rows = json.loads((source / "asin.json").read_text(encoding="utf-8")).get("rows") or []
    sqp_rows = json.loads((source / "sqp.json").read_text(encoding="utf-8")).get("rows") or []
    tracked_asins = {a for (a, _) in raw_by_pair}
    seen_asins: set[str] = set()
    top_asins: list[str] = []
    for r in sorted(asin_rows, key=lambda r: (-float(r.get("sales") or 0), str(r.get("asin") or ""))):
        asin = str(r.get("asin") or "").strip()
        if asin and asin not in seen_asins:
            seen_asins.add(asin)
            top_asins.append(asin)
            if len(top_asins) >= top_selling_asins:
                break
    ordered_asins = [a for a in top_asins if a in tracked_asins]
    untracked_top_asins = [a for a in top_asins if a not in tracked_asins]
    if not ordered_asins:
        raise ValueError(f"No tracked ASINs in top {top_selling_asins} selling child ASINs from sources/asin.json")

    sqp_now = {
        str(r["search_query"]).strip().lower(): (float(r.get("total_purchases") or 0) * float(r.get("purchase_price_median") or 0))
        for r in sqp_rows
        if str(r.get("reporting_date")) == str(week_end) and r.get("search_query")
    }
    by_phrase: dict[str, tuple[str, str, float, float, float]] = {}
    for asin in ordered_asins:
        for (row_asin, phrase), row in raw_by_pair.items():
            if row_asin != asin or phrase not in sqp_now or phrase in by_phrase:
                continue
            by_phrase[phrase] = (
                asin,
                phrase,
                sqp_now[phrase],
                float(row.get("keyword_sales") or 0),
                float(row.get("search_volume") or 0),
            )
    top = sorted(by_phrase.values(), key=lambda x: (-x[2], -x[3], -x[4], x[1]))[:limit]
    if not top:
        raise ValueError(f"Zero overlap between tracked child ASINs and SQP on {week_end}")
    return {
        "selection_rule": (
            f"Top {limit} unique SQP-overlapping phrases across tracked ASINs within the top {top_selling_asins} "
            f"selling child ASINs from asin.json by sales desc ({len(ordered_asins)} tracked, "
            f"{len(untracked_top_asins)} untracked skipped), assigned to highest-selling tracking ASIN and ordered "
            "by SQP est_total_keyword_sales desc"
        ),
        "week_end": str(week_end),
        "top_selling_asins": top_selling_asins,
        "tracked_asins_by_sales": ordered_asins,
        "untracked_top_selling_asins": untracked_top_asins,
        "limit": limit,
        "pairs": [{"asin": a, "phrase": ph} for a, ph, *_ in top],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--week-end", type=date.fromisoformat, required=True)
    parser.add_argument("--scope-file", type=Path, default=None)
    parser.add_argument("--write-scope", action="store_true", help="Deterministically build sources/keyword-scope.json from asin.json, tracked-*.json, and sqp.json and exit.")
    parser.add_argument("--scope-limit", type=int, default=20)
    parser.add_argument("--top-selling-asins", type=int, default=15, help="Top N selling child ASINs from asin.json to intersect with H10 tracked coverage (default: 15).")
    args = parser.parse_args()

    base = require_tmp_artifact_path(args.report_dir, label="Weekly rank comparison")
    source = base / "sources"
    raw_by_pair, still_has_more, seen_asins, chain_errors = load_tracked_pages(source)
    if args.write_scope:
        scope_doc = build_child_asin_sqp_scope(
            source,
            args.week_end,
            raw_by_pair,
            top_selling_asins=args.top_selling_asins,
            limit=args.scope_limit,
        )
        out_scope = args.scope_file or (source / "keyword-scope.json")
        out_scope.write_text(json.dumps(scope_doc, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({
            "status": "pass",
            "scope_file": str(out_scope),
            "pairs": len(scope_doc["pairs"]),
            "top_selling_asins": scope_doc["top_selling_asins"],
            "tracked_asins": scope_doc["tracked_asins_by_sales"],
            "untracked_top_selling_asins": scope_doc["untracked_top_selling_asins"],
        }))
        return
    histories = load_histories(source)
    reverse_index = load_reverse_asin_index(source)

    default_scope = source / "keyword-scope.json"
    scope_path = args.scope_file or (default_scope if default_scope.is_file() else None)
    scoped_mode = scope_path is not None

    if scoped_mode:
        target_pairs = load_scope(scope_path, seen_asins)
        for asin, phrase in target_pairs:
            if (asin, phrase) not in raw_by_pair:
                raise ValueError(
                    f"Missing scoped keyword {asin}|{phrase} in tracked exports "
                    f"(has_more={still_has_more.get(asin)})"
                )
    else:
        incomplete_asins = sorted(a for a, more in still_has_more.items() if more)
        if incomplete_asins:
            raise ValueError(
                f"Incomplete tracked keyword export for {incomplete_asins}; "
                "paginate to has_more=false or provide --scope-file / sources/keyword-scope.json"
            )
        if chain_errors:
            raise ValueError(f"Tracked pagination chain mismatch: {chain_errors}")
        target_pairs = list(raw_by_pair.keys())

    prior_date = args.week_end - timedelta(days=7)
    current: list[dict[str, Any]] = []
    prior: list[dict[str, Any]] = []
    enriched_rows = 0

    for asin, phrase in target_pairs:
        row = raw_by_pair[(asin, phrase)]
        history = histories.get(f"{asin}|{phrase}")
        if history is None:
            raise ValueError(f"History not checked: {asin} {phrase}")
        observations = history["organic_rank_history"]

        def rank_on(day: date) -> int | None:
            candidates = [
                (pd.Timestamp(o["date_time"]), o["rank"])
                for o in observations
                if o.get("rank") and o["rank"] > 0
                and pd.Timestamp(o["date_time"]).tz_convert("America/Los_Angeles").date() == day
            ]
            return max(candidates, key=lambda x: x[0])[1] if candidates else None

        cur_rank, prior_rank = rank_on(args.week_end), rank_on(prior_date)
        rev = reverse_index.get(phrase) or {}
        tracker_spon = parse_rank(row.get("sponsored_rank"))
        rev_spon = parse_rank(rev.get("sponsored_rank"))
        sponsored_rank = tracker_spon if tracker_spon is not None else rev_spon

        iq_score = rev.get("iq_score")
        title_density = rev.get("title_density")
        competing_products = rev.get("competing_products")
        cpc = rev.get("suggested_bid_usd")
        trend = rev.get("search_volume_trend_30d_pct")
        if rev:
            enriched_rows += 1

        note = (
            f"Organic rank dates {prior_date} vs {args.week_end}, Pacific. "
            "Demand is current H10 estimate; missing date observations stay blank."
        )
        if rev:
            note += " Reverse-ASIN metrics enriched from sources/h10."

        shared = {
            "asin": asin,
            "keyword": phrase,
            "source": "Helium 10 Keyword Tracker",
            "notes": note,
        }
        current.append({
            **shared,
            "search_volume": row.get("search_volume") if row.get("search_volume") is not None else rev.get("search_volume"),
            "organic_rank": cur_rank,
            "sponsored_rank": sponsored_rank,
            "cpr": row.get("cpr") if row.get("cpr") is not None else rev.get("cpr"),
            "iq_score": iq_score,
            "title_density": title_density,
            "competing_products": competing_products,
            "keyword_sales": row.get("keyword_sales") if row.get("keyword_sales") is not None else rev.get("keyword_sales_weekly"),
            "cpc": cpc,
            "trend": trend,
        })
        prior.append({**shared, "organic_rank": prior_rank, "keyword_sales": None})

    cur, prev = pd.DataFrame(current), pd.DataFrame(prior)
    if cur.empty or cur.duplicated(["asin", "keyword"]).any():
        raise ValueError("Empty or duplicate keyword scope")
    # Builder groups by phrase: do not let changing ASIN coverage imply movement.
    for phrase, group in cur.groupby("keyword"):
        if len(group) > 1:
            raise ValueError(f"Explicit same-ASIN-set aggregation required: {phrase}")

    (base / "h10").mkdir(parents=True, exist_ok=True)
    cur.to_csv(base / "h10/h10_keyword_candidates.csv", index=False)
    prev.to_csv(base / "h10/h10_prior_keyword_candidates.csv", index=False)

    sqp = pd.DataFrame(json.loads((source / "sqp.json").read_text(encoding="utf-8"))["rows"])
    sqp["keyword"] = sqp["search_query"].str.strip().str.lower()
    now = sqp[sqp.reporting_date.eq(str(args.week_end))].copy()
    old = sqp[sqp.reporting_date.eq(str(prior_date))].copy()
    if now.keyword.duplicated().any() or old.keyword.duplicated().any():
        raise ValueError("Duplicate SQP phrase/date")
    now["est_total_keyword_sales"] = now.total_purchases * now.purchase_price_median
    now["est_brand_keyword_sales"] = now.brand_purchases * now.brand_purchase_price_median
    old["prior_est_total_keyword_sales"] = old.total_purchases * old.purchase_price_median
    joined = cur[["keyword", "asin", "organic_rank"]].rename(
        columns={"asin": "tracked_asins", "organic_rank": "current_position"}
    )
    joined = joined.merge(
        prev[["keyword", "organic_rank"]].rename(columns={"organic_rank": "prior_position"}),
        on="keyword",
        validate="one_to_one",
    )
    joined = joined.merge(now, on="keyword", how="left", validate="one_to_one")
    joined = joined.merge(
        old[["keyword", "prior_est_total_keyword_sales"]],
        on="keyword",
        how="left",
        validate="one_to_one",
    )
    joined["rank_movement"] = joined.prior_position - joined.current_position
    joined["tracked_asin_count"] = 1
    joined["brand_ctr"] = joined.brand_clicks / joined.brand_impressions.replace(0, float("nan"))
    joined["brand_purchase_per_click"] = joined.brand_purchases / joined.brand_clicks.replace(0, float("nan"))
    joined["total_sales_wow"] = joined.est_total_keyword_sales / joined.prior_est_total_keyword_sales.replace(0, float("nan")) - 1
    joined.to_csv(base / "tracked_sqp_keywords.csv", index=False)

    paired = cur.organic_rank.notna() & prev.organic_rank.notna()
    print(json.dumps({
        "status": "pass",
        "tracked_pairs": len(cur),
        "paired": int(paired.sum()),
        "missing_date_observations": int((~paired).sum()),
        "enriched_rows": enriched_rows,
        "scoped_mode": scoped_mode,
        "prior_date": str(prior_date),
        "current_date": str(args.week_end),
    }))


if __name__ == "__main__":
    main()
