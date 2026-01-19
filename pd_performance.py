import pandas as pd
from functools import reduce
from classes.dataset import Dataset
from common.events import event_dates, event_dates_list

if __name__ == "__main__":

    d = Dataset(
        start="2022-01-01", end="2026-12-31", market="US", local_data=False, save=False
    )

    d.pull_br_data()
    d.pull_dictionary(marketplace="US")
    sales = d.br.copy()
    dictionary = d.dictionary.copy()

    dictionary_asin = (
        dictionary[["asin", "sku"]].groupby("asin").agg(lambda x: ", ".join(x))
    )

    sales["date"] = pd.to_datetime(sales["date"]).dt.date
    event_sales = sales[sales["date"].isin(event_dates_list)]
    non_event_sales = sales[~sales["date"].isin(event_dates_list)]

    final = pd.DataFrame(columns=["asin"])

    for event, dates in event_dates.items():
        days = 60
        event_name = event
        pre_event_sales = non_event_sales[
            non_event_sales["date"].between(
                dates[0] - pd.Timedelta(days=days + 1), dates[0] - pd.Timedelta(days=1)
            )
        ]
        sku_pre_event_average = (
            pre_event_sales.groupby("asin")["unitsOrdered"].agg("sum").reset_index()
        )
        sku_pre_event_average["unitsOrdered"] = (
            sku_pre_event_average["unitsOrdered"] / days
        )
        sku_pre_event_average = sku_pre_event_average.rename(
            columns={"unitsOrdered": f"pre-{event_name} avg units"}
        )

        specific_event_sales = event_sales[event_sales["date"].isin(dates)]
        sku_event_sales = (
            specific_event_sales.groupby("asin")["unitsOrdered"]
            .agg("sum")
            .reset_index()
        )
        sku_event_sales = sku_event_sales.rename(
            columns={"unitsOrdered": f"{event_name} total units"}
        )

        event_days = len(dates)
        sku_event_sales[f"{event_name} average"] = (
            sku_event_sales[f"{event_name} total units"] / event_days
        )

        total_event_stats = pd.merge(
            sku_pre_event_average, sku_event_sales, how="outer", on="asin"
        )

        total_event_stats[f"{event_name} X increase"] = (
            total_event_stats[f"{event_name} total units"]
            / total_event_stats[f"pre-{event_name} avg units"]
        )

        final = pd.merge(
            final, total_event_stats, how="outer", on="asin", validate="1:1"
        )

    final = pd.merge(dictionary_asin, final, how="right", on="asin", validate="1:1")

    final.to_excel("/home/misunderstood/temp/event_stats.xlsx", index=False)
