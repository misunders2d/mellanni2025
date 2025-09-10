import pandas as pd
import numpy as np

from classes.dataset import Dataset
from common.events import event_dates_list
from utils.mellanni_modules import format_header, week_number


def format_rows(df, column, worksheet, rows, target_format, starting_column=0):
    target_indices = df[df[column].isin(rows)].index.tolist()
    for target_index in target_indices:
        for cell, value in enumerate(df.iloc[target_index, starting_column:]):
            if not any([value is pd.NA, value == np.inf, pd.isna(value)]):
                worksheet.write(target_index + 1, cell, value, target_format)


def create_reporting_week(source_df):
    df = source_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["week"] = df["date"].apply(week_number)
    df["year"] = df["date"].dt.year
    # Ensure week starts on Sunday
    df["week_start"] = df["date"] - pd.to_timedelta(
        (df["date"].dt.weekday + 1) % 7, unit="d"
    )
    df["reporting week"] = (
        df["year"].astype(str)
        + "-"
        + df["week"].astype(str)
        + "\n"
        + df["week_start"].astype(str)
    )
    return df


def main(events=False, change_list=["Price increase"]):
    countries = ["US"]
    # stats_columns = ['unitsOrdered', 'unitsOrderedB2B',
    #        'orderedProductSales','orderedProductSalesB2B',
    #        'browserSessions','browserSessionsB2B',
    #        'mobileAppSessions', 'mobileAppSessionsB2B',
    #        'sessions']
    stats_columns = ["unitsOrdered", "orderedProductSales", "sessions"]

    d = Dataset(
        start="2025-01-05",
        end="2025-12-31",
        market=countries,
        local_data=True,
        save=False,
    )
    d.pull_changelog()
    d.pull_br_asin_data()
    d.pull_dictionary()

    dictionary = d.dictionary.copy()
    dictionary = dictionary[dictionary["marketplace"].isin(countries)]
    dictionary = dictionary[["sku", "asin", "collection", "size", "color"]]
    dictionary = dictionary.drop_duplicates("asin")

    sales = d.br_asin.copy()
    sales = sales[sales["country_code"].isin(countries)]
    sales["date"] = pd.to_datetime(sales["date"]).dt.date
    conditions = sales["date"] >= pd.to_datetime(d.start).date()
    if not events:
        conditions = conditions & (~sales["date"].isin(event_dates_list))
    sales = sales[conditions]
    changes = d.changelog.copy()
    changes["date"] = pd.to_datetime(changes["date"]).dt.date

    changes = changes[
        (changes["country_code"].isin(countries))
        & (changes["date"] >= pd.to_datetime(d.start).date())
    ]
    if change_list:
        changes = changes[changes["change_type"].isin(change_list)].copy()
    changes["change"] = np.where(
        changes["notes"].astype(str).str.strip() != "nan",
        changes["change_type"] + " : " + changes["notes"],
        changes["change_type"],
    )
    changes = pd.merge(changes, dictionary, how="left", on="sku", validate="m:1")
    changes = changes.dropna(subset="collection")
    changes = create_reporting_week(changes)
    # changes['reporting week'] = changes['date'].dt.year.astype(str) + '-' + changes['date'].dt.isocalendar().week.astype(str)
    changes_asins = (
        changes.groupby(["reporting week", "asin", "collection", "size", "color"])[
            "change"
        ]
        .agg(lambda x: " | ".join(x.unique()))
        .reset_index()
    )

    if change_list:
        price_asins = changes["asin"].unique().tolist()
        sales_asins = sales[sales["asin"].isin(price_asins)].copy()
    else:
        sales_asins = sales.copy()

    sales_asins = create_reporting_week(sales_asins)
    sales_asins = pd.merge(
        sales_asins, dictionary, how="left", on="asin", validate="m:1"
    )

    sales_asins = (
        sales_asins.groupby(["reporting week", "asin", "collection", "size", "color"])[
            stats_columns
        ]
        .agg("sum")
        .reset_index()
    )
    sales_asins["conversion"] = sales_asins["unitsOrdered"] / sales_asins["sessions"]
    sales_asins = sales_asins.sort_values(
        ["reporting week", "unitsOrdered"], ascending=(True, False)
    )

    full_sales = pd.merge(
        sales_asins,
        changes_asins,
        how="outer",
        on=["reporting week", "asin", "collection", "size", "color"],
        validate="1:1",
    )
    id_vars = ["reporting week", "asin", "collection", "size", "color"]
    metric_columns = [col for col in full_sales.columns if col not in id_vars]

    melted_df = full_sales.melt(
        id_vars=id_vars,
        value_vars=metric_columns,
        var_name="metrics",  # Name of the new column that holds the metric names
        value_name="value",  # Name of the new column that holds the actual metric values
    )

    reshaped_df = melted_df.pivot_table(
        index=["asin", "collection", "size", "color", "metrics"],
        columns="reporting week",
        values="value",
        aggfunc="first",  # Use 'first' to handle string values and avoid aggregation errors
    )
    sorted_columns = sorted(
        reshaped_df.columns.tolist(),
        key=lambda x: (int(x.split("-")[0]), int(x.split("-")[1].split("\n")[0])),
    )
    reshaped_df = reshaped_df[sorted_columns].reset_index().copy()

    with pd.ExcelWriter(
        "/home/misunderstood/temp/changes.xlsx", engine="xlsxwriter"
    ) as writer:
        workbook = writer.book

        perc_format = workbook.add_format({"num_format": "0.00%"})  # type: ignore
        curr_format = workbook.add_format({"num_format": "$#,##0.00"})  # type: ignore
        date_format = workbook.add_format({"num_format": "mm/dd/yy"})  # type: ignore
        num_format = workbook.add_format({"num_format": "#,##0"})  # type: ignore
        color_format = workbook.add_format({"bold": True, "bg_color": "#DAEEF3", "font_size": 7, "text_wrap": True})  # type: ignore
        conditional_format = {
            "type": "3_color_scale",
            "min_color": "#FF0000",  # Red for lowest
            "mid_color": "#FFFF00",  # Yellow for midpoint
            "max_color": "#00FF00",  # Green for highest
            "min_type": "min",  # Lowest value in range
            "mid_type": "percentile",  # Midpoint at 50th percentile
            "mid_value": 50,
            "max_type": "max",  # Highest value in range
        }

        reshaped_df.to_excel(writer, index=False, sheet_name="Price_increase")
        maxrow, maxcol = reshaped_df.shape
        worksheet = writer.sheets["Price_increase"]

        format_rows(reshaped_df, "metrics", worksheet, ["conversion"], perc_format)
        format_rows(
            reshaped_df,
            "metrics",
            worksheet,
            [
                "browserSessions",
                "browserSessionsB2B",
                "mobileAppSessions",
                "mobileAppSessionsB2B",
                "sessions",
                "unitsOrdered",
                "unitsOrderedB2B",
            ],
            num_format,
        )
        format_rows(
            reshaped_df,
            "metrics",
            worksheet,
            ["orderedProductSales", "orderedProductSalesB2B"],
            curr_format,
        )
        format_rows(reshaped_df, "metrics", worksheet, ["change"], color_format)

        for row in reshaped_df.index:
            worksheet.conditional_format(
                row + 1, 5, row + 1, maxcol, conditional_format
            )
        format_header(reshaped_df, writer, "Price_increase")


if __name__ == "__main__":
    include_events = input("Include events? (yes/no): ").strip().lower()
    if include_events == "yes":
        main(events=True)  # , change_list = [])
    else:
        main(events=False)
