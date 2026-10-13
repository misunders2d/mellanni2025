"""Normalize retrieved H10 tracked keywords/history into weekly builder inputs.

Run after prepare_weekly_ceo_sources.py and before the strict weekly runner.
All exports and outputs remain under /tmp. Exact Pacific week-end observations
only; no interpolation. Current H10 demand and historical ranks stay distinct.
"""
import argparse
import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from report_storage import require_tmp_artifact_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report-dir', type=Path, required=True)
    parser.add_argument('--week-end', type=date.fromisoformat, required=True)
    args = parser.parse_args()
    base = require_tmp_artifact_path(args.report_dir, label='Weekly rank comparison')
    source = base / 'sources'
    histories = dict(json.loads((source / 'rank-histories.json').read_text()))
    prior_date = args.week_end - timedelta(days=7)
    current, prior = [], []
    for path in sorted(source.glob('tracked-*.json')):
        payload = json.loads(path.read_text())['data']
        if payload['pagination']['has_more'] or payload['pagination']['truncated']:
            raise ValueError('Incomplete tracked keyword export')
        for row in payload['rows']:
            phrase = row['keywords_phrase'].strip().lower()
            asin = row['asin']
            history = histories.get(asin + '|' + phrase)
            if history is None:
                raise ValueError(f'History not checked: {asin} {phrase}')
            observations = history['organic_rank_history']
            def rank_on(day):
                candidates = [(pd.Timestamp(o['date_time']), o['rank']) for o in observations
                              if o.get('rank') and o['rank'] > 0
                              and pd.Timestamp(o['date_time']).tz_convert('America/Los_Angeles').date() == day]
                return max(candidates, key=lambda x: x[0])[1] if candidates else None
            cur_rank, prior_rank = rank_on(args.week_end), rank_on(prior_date)
            shared = {'asin': asin, 'keyword': phrase, 'source': 'Helium 10 Keyword Tracker',
                      'notes': f'Organic rank dates {prior_date} vs {args.week_end}, Pacific. Demand is current H10 estimate; missing date observations stay blank.'}
            current.append({**shared, 'search_volume': row.get('search_volume'),
                            'organic_rank': cur_rank, 'sponsored_rank': None,
                            'cpr': row.get('cpr'), 'iq_score': None, 'title_density': None,
                            'competing_products': None, 'keyword_sales': row.get('keyword_sales'),
                            'cpc': None, 'trend': None})
            prior.append({**shared, 'organic_rank': prior_rank, 'keyword_sales': None})
    cur, prev = pd.DataFrame(current), pd.DataFrame(prior)
    if cur.empty or cur.duplicated(['asin', 'keyword']).any():
        raise ValueError('Empty or duplicate keyword scope')
    # Builder groups by phrase: do not let changing ASIN coverage imply movement.
    for phrase, group in cur.groupby('keyword'):
        if len(group) > 1:
            raise ValueError(f'Explicit same-ASIN-set aggregation required: {phrase}')
    cur.to_csv(base / 'h10/h10_keyword_candidates.csv', index=False)
    prev.to_csv(base / 'h10/h10_prior_keyword_candidates.csv', index=False)
    sqp = pd.DataFrame(json.loads((source / "sqp.json").read_text())["rows"])
    sqp["keyword"] = sqp["search_query"].str.strip().str.lower()
    now = sqp[sqp.reporting_date.eq(str(args.week_end))].copy()
    old = sqp[sqp.reporting_date.eq(str(prior_date))].copy()
    if now.keyword.duplicated().any() or old.keyword.duplicated().any():
        raise ValueError("Duplicate SQP phrase/date")
    now["est_total_keyword_sales"] = now.total_purchases * now.purchase_price_median
    now["est_brand_keyword_sales"] = now.brand_purchases * now.brand_purchase_price_median
    old["prior_est_total_keyword_sales"] = old.total_purchases * old.purchase_price_median
    joined = cur[["keyword", "asin", "organic_rank"]].rename(columns={"asin":"tracked_asins", "organic_rank":"current_position"})
    joined = joined.merge(prev[["keyword", "organic_rank"]].rename(columns={"organic_rank":"prior_position"}), on="keyword", validate="one_to_one")
    joined = joined.merge(now, on="keyword", how="left", validate="one_to_one")
    joined = joined.merge(old[["keyword", "prior_est_total_keyword_sales"]], on="keyword", how="left", validate="one_to_one")
    joined["rank_movement"] = joined.prior_position - joined.current_position
    joined["tracked_asin_count"] = 1
    joined["brand_ctr"] = joined.brand_clicks / joined.brand_impressions.replace(0, float("nan"))
    joined["brand_purchase_per_click"] = joined.brand_purchases / joined.brand_clicks.replace(0, float("nan"))
    joined["total_sales_wow"] = joined.est_total_keyword_sales / joined.prior_est_total_keyword_sales.replace(0, float("nan")) - 1
    joined.to_csv(base / "tracked_sqp_keywords.csv", index=False)
    paired = cur.organic_rank.notna() & prev.organic_rank.notna()
    print(json.dumps({'status': 'pass', 'tracked_pairs': len(cur), 'paired': int(paired.sum()),
                      'missing_date_observations': int((~paired).sum()),
                      'prior_date': str(prior_date), 'current_date': str(args.week_end)}))


if __name__ == '__main__':
    main()
