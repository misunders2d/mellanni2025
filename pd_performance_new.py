import pandas as pd
from functools import reduce
from classes.dataset import Dataset
events = {
            'PD22':['2022-07-12','2022-07-13'],
            'PFE22':['2022-10-11','2022-10-12'],
            'BFCM22':['2022-11-25','2022-11-28'],
            
            'PD23':['2023-07-11','2023-07-12'],
            'PFE23':['2023-10-10','2023-10-11'],
            'BFCM23':['2023-11-24','2023-11-27'],

            'PD24':['2024-07-16','2024-07-17'],
            'PFE24':['2024-10-08','2024-10-09'],
            'BFCM24':['2024-11-21','2024-12-02'],

            'BigSpringSale25':['2025-03-25','2025-03-31']
    }

def create_event_date_ranges(row):
    event_name = row[0]
    event_dates = row[1]
    event_date_range = pd.date_range(row[1][0], row[1][1])
    return {event_name: event_date_range}

def create_date_ranges(row):
    return pd.date_range(row[1][0], row[1][1])

event_date_ranges = list(map(create_event_date_ranges, events.items()))
date_ranges = list(map(create_date_ranges, events.items()))
date_ranges = ([date for date_range in date_ranges for date in date_range])

d = Dataset(start="2022-01-01", end="2025-12-31", market="US", local_data = True, save=False)

d.pull_br_data()
sales = d.br.copy()

sales['date'] = pd.to_datetime(sales['date'])
event_sales = sales[sales['date'].isin(date_ranges)]
non_event_sales = sales[~sales['date'].isin(date_ranges)]

final = pd.DataFrame(columns=['sku'])

for event in event_date_ranges:
    event_name = list(event.keys())[0]
    event_dates = list(event.values())[0]
    pre_event_sales = non_event_sales[non_event_sales['date'].between(event_dates[0]-pd.Timedelta(days=61), event_dates[0]-pd.Timedelta(days=1))]
    sku_pre_event_average = pre_event_sales.groupby('sku')['unitsOrdered'].agg('mean').reset_index()
    sku_pre_event_average = sku_pre_event_average.rename(columns = {'unitsOrdered':f'pre-{event_name} avg units'})

    specific_event_sales = event_sales[event_sales['date'].isin(event_dates)]
    sku_event_sales = specific_event_sales.groupby('sku')['unitsOrdered'].agg('sum').reset_index()
    sku_event_sales = sku_event_sales.rename(columns = {'unitsOrdered':f'{event_name} total units'})
    
    event_days = len(event_dates)
    sku_event_sales[f'{event_name} average'] = sku_event_sales[f'{event_name} total units']/event_days
    
    total_event_stats = pd.merge(sku_pre_event_average, sku_event_sales, how = 'outer', on ='sku')
    
    total_event_stats[f'{event_name} X increase'] = total_event_stats[f'{event_name} average'] / total_event_stats[f'pre-{event_name} avg units']

    final = pd.merge(final, total_event_stats, how = 'outer', on = 'sku')

final.to_excel('/home/misunderstood/temp/event_stats.xlsx', index = False)