import pandas as pd
import customtkinter as ctk
from tkcalendar import Calendar
import datetime, os, time
from numpy import inf
from io import BytesIO
from matplotlib import pyplot as plt
from matplotlib.ticker import PercentFormatter
import threading

from connectors import gcloud as gc
from utils import mellanni_modules as mm
from common import user_folder

end_of_year = {datetime.datetime.strptime(x, '%Y-%m-%d').date() for x in {'2024-12-29','2024-12-30','2024-12-31'}}
results={}

def week_number(date: datetime.date) -> int:
    '''returns a week number for weeks starting with Sunday'''
    if not isinstance(date, datetime.date):
        try:
            date = datetime.datetime.strptime(date,"%Y-%m-%d")
        except:
            raise BaseException("Date format not recognized")
    return date.isocalendar().week + 1 if date.weekday() == 6 else date.isocalendar().week

def pull_sales(start: str, market: str, report:str='business') -> pd.DataFrame:
    if report == 'business_asin':
        query = f"""SELECT DATE(date) AS date, childAsin AS asin, unitsOrdered AS units, sessions
                    FROM `reports.business_report_asin`
                    WHERE DATE(date) >= DATE("{start}")
                    AND country_code = "{market}"
                    """
    elif report == 'business':
        query = f"""SELECT DATE(date) AS date, sku, childAsin AS asin, unitsOrdered AS units, sessions
                    FROM `reports.business_report`
                    WHERE DATE(date) >= DATE("{start}")
                    AND country_code = "{market}"
                    """
    client = gc.gcloud_connect()
    result = client.query(query).to_dataframe()
    result['year'] = pd.to_datetime(result['date']).dt.year
    result['week'] = pd.to_datetime(result['date']).apply(week_number)
    result.loc[result['date'].isin(end_of_year),'year']=datetime.datetime.now().year
    result.loc[result['date'].isin(end_of_year),'week']=1
    result['date'] = pd.to_datetime(result['date']).dt.date
    last_date=datetime.datetime.now() - datetime.timedelta(datetime.datetime.now().weekday()+1)
    results['sales']=result
    return result[result['date']<=last_date.date()]

def pull_dictionary(market: str) -> pd.DataFrame:
    d_name = 'dictionary' if market == 'US' else 'dictionary_ca'
    query = f"""SELECT sku, asin, collection, sub_collection
                FROM `auxillary_development.{d_name}`
                """
    client = gc.gcloud_connect()
    result = client.query(query).to_dataframe()
    results['dictionary']=result
    return result

def pull_changes(start:str, market:str) -> pd.DataFrame:
    changes_name = 'sku_changelog' if market == "US" else 'sku_changelog_ca'
    query = f"""SELECT DATE(date) AS date, sku, change_type, notes
                FROM `auxillary_development.{changes_name}`
                WHERE DATE(date) >= DATE("{start}")
                """
    client = gc.gcloud_connect()
    result = client.query(query).to_dataframe()
    result['year'] = pd.to_datetime(result['date']).dt.year
    result['week'] = pd.to_datetime(result['date']).apply(week_number)
    result.loc[result['date'].isin(end_of_year),'year']=datetime.datetime.now().year
    result.loc[result['date'].isin(end_of_year),'week']=1
    result['date'] = pd.to_datetime(result['date']).dt.date
    results['changes']=result
    return result

def clean_sales(sales:pd.DataFrame, dictionary:pd.DataFrame) -> pd.DataFrame:
    """compress sales to have only collection, subcollection and sales data"""
    dictionary_refined = dictionary.drop_duplicates('asin')
    df = pd.merge(sales, dictionary_refined, how='left', on='asin')
    sales_refined = df.pivot_table(
        values=['units','sessions'],
        index=['year','week','collection', 'sub_collection'],
        aggfunc='sum'
    ).reset_index()
    return sales_refined

def clean_changes(changes:pd.DataFrame, dictionary:pd.DataFrame) -> pd.DataFrame:
    """compress changes to have only collection, subcollection and change type"""
    df = pd.merge(changes, dictionary, how='left', on='sku')
    if len(df) == 0:
        return df
    changes_refined = df.pivot_table(
        values='change_type',
        index=['year','week','collection', 'sub_collection'],
        aggfunc=lambda x: ' | '.join(x.unique())
    ).reset_index()
    return changes_refined

def break_by_week(result:pd.DataFrame) -> pd.DataFrame:
    """reshape df to have weeks in columns"""
    result['conversion'] = result['units'] / result['sessions']
    result_refined = pd.DataFrame(columns = ['collection','sub_collection'])
    result['reporting_week']=result['year'].astype(str)+'-'+result['week'].astype(str)
    last_week=week_number(datetime.datetime.now())-1
    if last_week==0:
        last_week = week_number(datetime.datetime.now() - datetime.timedelta(days=7))
    two_weeks = last_week-1 if last_week>1 else 52
    result = result[result['week'].isin([last_week, two_weeks])]
    weeks=sorted(result['reporting_week'].unique(), reverse = True)
    for week in weeks:
        temp = result[result['reporting_week']==week][['collection', 'sub_collection', 'sessions', 'units',
               'conversion','change_type']]
        temp = temp.rename(
            columns={
                'sessions':f'sessions week {week}',
                'units':f'units week {week}',
                'change_type':f'changes week {week}',
                'conversion':f'conversion week {week}'
                }
            )
        result_refined = pd.merge(result_refined, temp, how = 'outer', on = ['collection','sub_collection'])
    return result_refined

def add_totals(result_refined:pd.DataFrame) -> pd.DataFrame:
    """add a "totals" row"""
    num_rows = result_refined.shape[0]
    sum_cols = [x for x in result_refined.columns if any(['sessions' in x,'units' in x, 'conversion' in x])]
    
    total_row = pd.DataFrame([
        ['Total',
         f'=SUBTOTAL(9,C1:C{num_rows+1})',
         f'=SUBTOTAL(9,D1:D{num_rows+1})',
         f'=D{num_rows+2}/C{num_rows+2}',
         f'=SUBTOTAL(9,G1:G{num_rows+1})',
         f'=SUBTOTAL(9,H1:H{num_rows+1})',
         f'=H{num_rows+2}/G{num_rows+2}'
         ]
        ],
        columns = ['collection']+sum_cols)
    
    result_total = pd.concat([result_refined, total_row])
    return result_total
    
def plot_data(df:pd.DataFrame, num_weeks:int=2) -> BytesIO:
    weeks = sorted(df['reporting_week'].unique().tolist(), reverse=False)
    combined = df.pivot_table(
        values = ['sessions', 'units'],
        index = 'reporting_week',
        aggfunc='sum'
        ).reset_index()
    combined['conversion'] = combined['units']/combined['sessions']
    fig, ax = plt.subplots(1,1, figsize=(10,6))
    ax.bar(combined['reporting_week'],combined['units'])
    ax1 = ax.twinx()
    ax1.plot(combined['reporting_week'], combined['conversion'], color='red')
    for week in weeks:
        units=combined.loc[combined['reporting_week']==week, 'units'].values[0]
        ax.text(week, units, f'{units:,.0f}\nunits', ha='center', va='center')
        conversion=combined.loc[combined['reporting_week']==week,'conversion'].values[0]
        ax1.text(week, conversion, f'{conversion:.2%}', ha='center')
    ax1.yaxis.set_major_formatter(PercentFormatter(1))
    ax.set_ylabel('Units sold')
    ax1.set_ylabel('Conversion')
    ax.set_xlabel('Week')
    plt.tight_layout()
    buf = BytesIO()
    plt.savefig(buf, format='png')
    plt.close()
    buf.seek(0)
    return buf

def export_to_excel(df:pd.DataFrame, plot_buf:BytesIO, market, target=None) -> None:
    """export formatted dataframe to Excel"""
    if not target:
        target=ctk.filedialog.askdirectory(
            title="Select a folder to save the result",
            initialdir = os.path.join(os.path.expanduser('~'),'temp'))
    
    file_name=os.path.join(target, f'Weekly conversion_{market}.xlsx')
    perc_cols = [x for x in df.columns if 'conversion' in x]
    num_cols = [x for x in df.columns if any(['sessions' in x, 'units' in x])]

    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        workbook = writer.book
        perc_format = workbook.add_format({'num_format': '0.00%'})
        num_format = workbook.add_format({'num_format':'#,##0'})
        df.to_excel(writer, sheet_name='Weekly conversion', index=False)
        
        worksheet = writer.sheets['Weekly conversion']
        #apply percentage format
        for col in perc_cols:
            for cell, value in enumerate(df[col]):
                if not any([isinstance(value, pd._libs.missing.NAType), value == inf, pd.isna(value)]):
                    worksheet.write(cell+1, df.columns.tolist().index(col), value, perc_format)

        #apply numeric format
        for col in num_cols:
            for cell, value in enumerate(df[col]):
                if not any([isinstance(value, pd._libs.missing.NAType), value == inf, pd.isna(value)]):
                    worksheet.write(cell+1, df.columns.tolist().index(col), value, num_format)
        
        mm.format_header(df, writer, 'Weekly conversion')
        worksheet.insert_image('K2', 'plot.png', {'image_data': plot_buf})
        mm.open_file_folder(target)
    return None

def process_data(start, market):
    sales = pull_sales(start, market)
    print(sales['units'].sum())
    dictionary = pull_dictionary(market)
    changes = pull_changes(start, market)

    sales_refined = clean_sales(sales, dictionary)
    changes_refined = clean_changes(changes, dictionary)
    
    result = pd.merge(sales_refined, changes_refined, how = 'left', on = ['year','week','collection','sub_collection'])
    
    print(result['units'].sum())
    result_refined = break_by_week(result)
    result_total = add_totals(result_refined)
    plot_buf = plot_data(result)
    _ = export_to_excel(result_total, plot_buf, market, target=user_folder)

def process_data_threaded(start, market):
    print(start, market)
    threads = [
        threading.Thread(target=pull_sales, args=(start, market, 'business_asin')),
        threading.Thread(target=pull_dictionary, args=(market,)),
        threading.Thread(target=pull_changes, args=(start, market)),
        ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
        
    sales = results.get('sales')
    dictionary = results.get('dictionary')
    changes = results.get('changes')
    if any([sales is None,dictionary is None,changes is None]):
        raise BaseException('Dataframe can not be None')
    sales_refined = clean_sales(sales, dictionary)
    changes_refined = clean_changes(changes, dictionary)
    
    result = pd.merge(sales_refined, changes_refined, how = 'left', on = ['year','week','collection','sub_collection'])
    
    print(result['units'].sum())
    result_refined = break_by_week(result)
    result_total = add_totals(result_refined)
    plot_buf = plot_data(result)
    _ = export_to_excel(result_total, plot_buf, market, target = user_folder)

def main():
    app = ctk.CTk()
    app.geometry('400x200')
    app.title('Weekly conversion checker')


    def calendar_popup():
        def return_date():
            date_entry.delete(0,ctk.END)
            date_entry.insert(0,date_field.get_date())
            calendar_window.destroy()

        calendar_window = ctk.CTkToplevel()
        date_field = Calendar(calendar_window, date_pattern='yyyy-mm-dd')
        date_field.pack(pady=10)
        select_button = ctk.CTkButton(calendar_window, text='OK', command=return_date)
        select_button.pack(pady=10)

    date_entry = ctk.CTkEntry(app, width=200, height=20)
    date_entry.insert(0,pd.to_datetime('today').date())
    date_entry.grid(row=0, column=0, padx=10, pady=10)

    calendar_button = ctk.CTkButton(app, text='Select start date', command=calendar_popup)
    calendar_button.grid(row=0, column=1, padx=10, pady=10)

    radio_var = ctk.StringVar(value='US')
    us_radio = ctk.CTkRadioButton(app, text='US', value='US', variable=radio_var)
    us_radio.select()
    us_radio.grid(row=1, column=0, padx=10, pady=10)

    ca_radio = ctk.CTkRadioButton(app, text='CA', value='CA', variable=radio_var)
    ca_radio.grid(row=1, column=1, padx=10, pady=10)

    submit_button = ctk.CTkButton(
        app,
        text='Submit',
        command=lambda:process_data_threaded(date_entry.get(), radio_var.get()))
    submit_button.grid(row=2, column=0, columnspan=2, padx=10, pady=10)

    app.mainloop()

if __name__ == '__main__':
    main()

    # begin = time.perf_counter()
    # # process_data()
    # process_data_threaded()
    # print(f'Processed data in {time.perf_counter()-begin:.1f} seconds')




