import database_tools
import pandas as pd
import sqlite3
from common import user_folder
import os

start_date = pd.to_datetime('today').date() - pd.Timedelta(days=181)
start_date = "2025-05-30" if start_date < pd.to_datetime("2025-05-30").date() else start_date

end_date = pd.to_datetime('today').date() - pd.Timedelta(days=1)

inv = database_tools.read_database('fba_inventory', start_date, end_date)
sales = database_tools.read_database('sales', start_date, end_date)

def calculate_inventory_isr(inventory):
    filter = inventory['sku'].str.endswith('-CA') # Create a filter for Canadian SKUs ONLY

    inventory = inventory[filter][['snapshot-date', 'asin', 'inventory_supply_at_fba']] # apply the filter and select relevant columns
    inventory_grouped = inventory.groupby(['snapshot-date','asin']).agg('sum').reset_index()
    total_days = inventory_grouped['snapshot-date'].nunique()

    inventory_grouped['in_stock?'] = inventory_grouped['inventory_supply_at_fba'] > 0
    asin_isr = inventory_grouped.groupby('asin')[['in_stock?']].agg('mean').reset_index()
    return asin_isr, total_days


def get_asin_sales(sales, total_day):
    sales_filtered = sales[sales['sku'].str.endswith('-CA')][['date','(child)_asin','units_ordered','sessions_-_total']].copy()
    sales_daily = sales_filtered.groupby(['date','(child)_asin']).agg('sum').reset_index()
    total_sales = sales_daily.groupby('(child)_asin')[['units_ordered','sessions_-_total']].agg('sum').reset_index()
    total_sales['average_daily_units'] = total_sales['units_ordered'] / total_day
    total_sales['average_daily_sessions'] = total_sales['sessions_-_total'] / total_day
    total_sales = total_sales.rename(columns={'(child)_asin': 'asin'})
    return total_sales


def main():
    asin_isr, total_day = calculate_inventory_isr(inv[['snapshot-date', 'sku', 'asin', 'inventory_supply_at_fba']].copy())
    total_sales = get_asin_sales(sales, total_day)
    result = pd.merge(asin_isr, total_sales, on='asin', how='outer')
    result['average_corrected'] = result['average_daily_units'] / result['in_stock?']
    result.to_excel(os.path.join(user_folder, 'inventory_restock.xlsx'), index=False)


if __name__ == "__main__":
    main()