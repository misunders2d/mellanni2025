from classes.dataset import Dataset
from utils.mellanni_modules import export_to_excel
import pandas as pd

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
    }

event_dates = []
for date_range in events.values():
    dates = pd.date_range(date_range[0], date_range[-1])
    for date in dates:
        event_dates.append(date.date())

d = Dataset(start="2022-01-01", end="2025-12-31", market="US", local_data = True, save=False)

d.pull_br_data()
sales = d.br.copy()

df = sales[['date', 'sku', 'unitsOrdered']].copy()
df['date'] = pd.to_datetime(df['date']).dt.date
df = df.sort_values('date')


#calculate event sales
event_sales = pd.DataFrame()
for event, dates in events.items():
    temp_file = df[df['date'].between(pd.to_datetime(dates[0]).date(), pd.to_datetime(dates[-1]).date())]
    event_sales = pd.concat([event_sales, temp_file])


non_event_sales = df[~df['date'].isin(event_dates)]


event_sales_performance = pd.DataFrame()

for event, dates in events.items():
    temp_file = df[df['date'].between(pd.to_datetime(dates[0]).date(), pd.to_datetime(dates[-1]).date())]
    temp_non_event = non_event_sales[
        non_event_sales['date'].between(pd.to_datetime(dates[0]).date() - pd.Timedelta(days=60), pd.to_datetime(dates[0]).date())
        ]
    sku_file = temp_file.groupby('sku')['unitsOrdered'].agg('sum').reset_index()
    sku_file['event'] = event
    sku_non_event = temp_non_event.groupby('sku')['unitsOrdered'].agg('sum').reset_index()
    sku_non_event['event'] = f'pre-{event}'    
    
    result = pd.concat([sku_file, sku_non_event])
    result = result.groupby(['event', 'sku'])['unitsOrdered'].agg('mean').reset_index()
    event_sales_performance = pd.concat([event_sales_performance, result])


export_to_excel(dfs=[event_sales_performance],sheet_names=['pd_performance'],filename='pd.xlsx')