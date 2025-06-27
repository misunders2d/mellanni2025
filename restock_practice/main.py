import database_tools
import pandas as pd
import sqlite3


start_date = pd.to_datetime('today').date() - pd.Timedelta(days=181)
start_date = "2025-05-30" if start_date < pd.to_datetime("2025-05-30").date() else start_date

end_date = pd.to_datetime('today').date() - pd.Timedelta(days=1)

inv = database_tools.read_database('fba_inventory', start_date, end_date)
sales = database_tools.read_database('sales', start_date, end_date)

query = ''' SELECT "snapshot-date", asin, inventory_supply_at_fba
            FROM fba_inventory
            WHERE LOWER(condition) = "new"
            '''
with sqlite3.connect('restock_canada.db') as conn:
    result = pd.read_sql(query, conn)

inventory_grouped = result.groupby(['snapshot-date','asin']).agg('sum').reset_index()

inventory_grouped['in_stock?'] = inventory_grouped['inventory_supply_at_fba'] > 0
asin_isr = inventory_grouped.groupby('asin')[['in_stock?']].agg('mean').reset_index()

asin_isr.to_excel('/home/misunderstood/temp/asins.xlsx', index=False)
