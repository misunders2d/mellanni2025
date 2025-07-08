from common import user_folder
from utils.mellanni_modules import week_number
import pandas as pd
import os
import datetime

file_path = os.path.join(user_folder, 'dataset', 'orders.csv')

if not os.path.exists(file_path):
    raise FileNotFoundError(f"\nFile not found: {file_path}\nDon't forget to run the Restock script first.")

orders = pd.read_csv(file_path, usecols=['pacific_date', 'sales', 'promo_discount','sales_channel'])
orders['pacific_date'] = pd.to_datetime(orders['pacific_date'], format='%Y-%m-%d')
orders = orders[~orders['sales_channel'].isin(['Amazon.com', 'Amazon.ca', 'Amazon.com.mx'])]
orders = orders.sort_values(by='pacific_date', ascending=False)

def get_weekly_promos():
    # method 1 = last sunday
    last_sunday = (datetime.datetime.now() - datetime.timedelta(days = datetime.datetime.now().weekday()+8)).date()

    sundays = [last_sunday]

    for i in range(3):
        last_sunday = last_sunday - datetime.timedelta(days=7)
        sundays.append(last_sunday)

    weeks = []
    for sunday in sundays:
        last_week = pd.date_range(start=sunday, periods=7)
        weeks.append(last_week)

    weekly_orders = []
    for week in weeks:
        weekly_file = orders[orders['pacific_date'].isin(week)]
        weekly_file = weekly_file.groupby('sales_channel')[['sales','promo_discount']].agg('sum').reset_index()
        weekly_file['week'] = week[0]
        weekly_orders.append(weekly_file)
    return pd.concat(weekly_orders, ignore_index=True)

def get_weekly_promos2(orders):
    # method 2 = week number
    orders['week'] = orders['pacific_date'].apply(week_number)
    reporting_weeks = sorted(orders['week'].unique(), reverse=True)[1:5]
    orders = orders[orders['week'].isin(reporting_weeks)]
    weekly_promos = orders.groupby(['sales_channel', 'week'])[['sales', 'promo_discount']].agg('sum').reset_index()
    weekly_promos = weekly_promos.sort_values(by=['week', 'sales_channel'], ascending=[False, True])
    return weekly_promos


def main():
 
    print(get_weekly_promos2(orders))