from common import user_folder
from utils.mellanni_modules import week_number, open_file_folder
from connectors import gcloud as gc
import pandas as pd
import os
import datetime
import easygui


def get_orders():
    query = """
    SELECT
        DATETIME(purchase_date, "America/Los_Angeles") AS pacific_date,
        item_price as sales, item_promotion_discount as promo_discount, sales_channel
    FROM
        `mellanni-project-da.reports.all_orders`
    WHERE
        DATE(purchase_date, "America/Los_Angeles") BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND CURRENT_DATE()
    ORDER BY
        pacific_date ASC, sku ASC
        """
    with gc.gcloud_connect() as client:
        orders = client.query(query).to_dataframe()

    orders["pacific_date"] = pd.to_datetime(
        orders["pacific_date"], format="%Y-%m-%d"
    ).dt.date
    # orders = orders[
    #     ~orders["sales_channel"].isin(["Amazon.com", "Amazon.ca", "Amazon.com.mx"])
    # ]
    orders = orders.sort_values(by="pacific_date", ascending=False)
    return orders


def get_weekly_promos(orders):
    # method 1 = last sunday
    last_sunday = (
        datetime.datetime.now()
        - datetime.timedelta(days=datetime.datetime.now().weekday() + 8)
    ).date()

    sundays = [last_sunday]

    for _ in range(3):
        last_sunday = last_sunday - datetime.timedelta(days=7)
        sundays.append(last_sunday)

    weeks = []
    for sunday in sundays:
        last_week = pd.date_range(start=sunday, periods=7)
        weeks.append(last_week)

    weekly_orders = []
    for week in weeks:
        weekly_file = orders[orders["pacific_date"].isin(week)]
        weekly_file = (
            weekly_file.groupby("sales_channel")[["sales", "promo_discount"]]
            .agg("sum")
            .reset_index()
        )
        weekly_file["week"] = week[0]
        weekly_orders.append(weekly_file)
    return pd.concat(weekly_orders, ignore_index=True)


def get_weekly_promos2(orders):
    # method 2 = week number
    orders["week"] = orders["pacific_date"].apply(week_number)
    reporting_weeks = sorted(orders["week"].unique(), reverse=True)[1:5]
    orders = orders[orders["week"].isin(reporting_weeks)]
    weekly_promos = (
        orders.groupby(["sales_channel", "week"])[["sales", "promo_discount"]]
        .agg("sum")
        .reset_index()
    )
    weekly_promos = weekly_promos.sort_values(
        by=["week", "sales_channel"], ascending=[False, True]
    )
    return weekly_promos


def main():
    try:
        orders = get_orders()
        weekly_promos_df = get_weekly_promos2(orders)

        file_name = "weekly_marketplace_promos.csv"
        file_path = os.path.join(user_folder, file_name)

        weekly_promos_df.to_csv(file_path, index=False)

        easygui.msgbox(
            msg=f"Successfully saved the report to {file_path}", title="Success"
        )
        open_file_folder(file_path)
    except Exception as e:
        easygui.exceptionbox(msg=f"Error: {e}", title="Error")
