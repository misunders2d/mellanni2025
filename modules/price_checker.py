from common import excluded_collections, user_folder

import pandas as pd
import os, numpy as np
import threading
from concurrent.futures import ThreadPoolExecutor
from connectors.gcloud import gcloud_connect
from connectors import gdrive as dm
import customtkinter as ctk
from utils import mellanni_modules as mm

BQ_BUSINESS_REPORT:str = 'reports.business_report'
BQ_DICTIONARY:str = 'auxillary_development.dictionary'
BQ_ORDERS:str = 'reports.all_orders'
FBA_INVENTORY:str = 'reports.fba_inventory_planning'
SALE_FILE:str = '1iB1CmY_XdOVA4FvLMPeiEGEcxiVEH3Bgp4FJs1iNmQs'
PRICELIST_ID:str = '1VGZ5VGsQiYgX9X6PxrRREj265gMzVQu9UQMKnT_014o'
EVENT_FILE:str = '1XcrMgklKRvElCb8vZI5r6j0P9ha4wmlPZdpU9MWq7QA'
DICTIONARY_FILENAME:str = 'Dictionary.xlsx'
marketplace:str = 'US'

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        self.geometry('400x350')
        self.title('Pricelist checker')
        
        self.client = gcloud_connect()
        self.executor = ThreadPoolExecutor()
        
        self.result_files = {}
        
        self.print_area = ctk.CTkTextbox(self, width=380)
        self.print_area.grid(row=0, column=0, columnspan=2, pady=10, padx=10)
        
        self.progress = ctk.CTkProgressBar(self, mode='indeterminate')
        self.progress.grid(row=1, column=0, columnspan=2, pady=10, padx=10, sticky='ew')

        self.button = ctk.CTkButton(self, text='GO', command=self.process_prices)
        self.button.grid(row=2, column=0, pady=10, padx=10)

        self.custom_file_checkbox = ctk.CTkCheckBox(self, text='Use .csv inventory file instead')
        self.custom_file_checkbox.grid(row=2, column=1, pady=10, padx=10)

        self.update_button = ctk.CTkButton(self, text='Update', fg_color='gray', command=self.update)
        self.update_button.grid(row=3, column=0, pady=20)

    def update(self):
        import subprocess
        subprocess.call(['git','pull'])
        subprocess.call(['pip','install','-r','requirements.txt'])

    def download_sale_file(self):
        # get information from pricing (sales) file
        sale_file = dm.download_gspread(spreadsheet_id=SALE_FILE)
        sale_file = sale_file[['SKU','Full price','Sale price', 'Status']]
        self.result_files['sale_file'] = sale_file
        self.print_area.insert(ctk.END, text='Downloaded sale file\n')
        return
    
    def download_fba_inventory(self):
        if self.custom_file_checkbox.get():
            fba_inv_filename = ctk.filedialog.askopenfilename(title="Select FBA Inventory.csv file", initialdir=user_folder)
            fba_inv = pd.read_csv(
                fba_inv_filename,usecols=[
                    'snapshot-date','sku','available','your-price','sales-price'
                    ]                
                )
            fba_inv = fba_inv.rename(columns={
                'snapshot-date':'snapshot_date',
                'sku':'SKU',
                'your-price':'your_price',
                'sales-price':'sales_price'
                })
        else:
            fba_query = f'''SELECT DATE(snapshot_date) as snapshot_date, sku as SKU, available, your_price, sales_price
                            FROM {FBA_INVENTORY}
                            WHERE DATE(snapshot_date) = (
                                SELECT MAX(DATE(snapshot_date)) FROM {FBA_INVENTORY} WHERE marketplace = "{marketplace}"
                                )
                            AND marketplace = "{marketplace}"
                            '''
            fba_inv = self.client.query(fba_query).to_dataframe()
        self.result_files['fba_inventory'] = fba_inv
        self.print_area.insert(ctk.END, text='fba file downloaded\n')
        return
    
    def download_dictionary(self):
        dictionary_id = dm.find_file_id(folder_id = '1zIHmbWcRRVyCTtuB9Atzam7IhAs8Ymx4', drive_id = '0AMdx9NlXacARUk9PVA', filename=DICTIONARY_FILENAME)
        dictionary = pd.read_excel(dm.download_file(dictionary_id), usecols=['SKU','Collection','Sub-collection','Size Map', 'Color'])
        dictionary = dictionary[~dictionary['Collection'].isin(excluded_collections)]
        del dictionary['Collection']
        dictionary = dictionary.rename(columns={'Sub-collection':'collection', 'Size Map':'size'})
        wrong_sub_cols = [
            '1800 Bed Sheet Set - Striped','1800 Bed Sheet Set - Printed',
            '1800 Bed Sheet Set - Solid - White','1800 Bed Sheet Set - Solid',
            '1800 Bed Sheet Set - Solid - Light Gray','1800 Bed Sheet Set - Solid - Gray',
            '1800 Bed Sheet Set - Solid - RV Sheets'
            ]
        dictionary.loc[dictionary['collection'].isin(wrong_sub_cols),'collection'] = '1800 Bed Sheets'
        self.result_files['dictionary'] = dictionary
        self.print_area.insert(ctk.END, text='dictionary downloaded\n')
        return
    
    def download_price_list(self):
        price_list = dm.download_gspread(spreadsheet_id=PRICELIST_ID)[['Sub-collection','Size','Price','MSRP']]
        price_list = price_list.rename(columns = {'Sub-collection':'collection', 'Size':'size'})
        self.result_files['price_list'] = price_list
        self.print_area.insert(ctk.END, text='price list downloaded\n')
        return
    
    def merge_files(self, dictionary, price_list, fba_inventory, sale_file, event_file):
        self.print_area.insert(ctk.END, text='Merging files\n')
        price_check = pd.merge(dictionary, price_list, how = 'left', on=['collection','size'])
        price_check = pd.merge(price_check, fba_inventory, how = 'outer', on = 'SKU')
        price_check = pd.merge(price_check, sale_file, how = 'outer', on = 'SKU')
        return price_check
    
    def process_file(self, price_check):
        self.print_area.insert(ctk.END, text='processing file\n')
        price_check_refined = price_check.copy()
        price_check_refined.loc[price_check_refined['Status'].isin(["Selling","TEST"]),'Target current price'] = price_check_refined['Sale price']
        price_check_refined.loc[~price_check_refined['Status'].isin(["Selling","TEST"]),'Target current price'] = price_check_refined['Full price']
    
        str_columns = ['your_price','sales_price','Full price','Sale price','Target current price']
        for str_column in str_columns:
            self.str_to_float(price_check_refined, str_column)
        price_check_refined.loc[price_check_refined['Status'].isin(["Selling","TEST"]),'Price_diff'] = price_check_refined['Target current price'] - price_check_refined['your_price']
        price_check_refined.loc[~price_check_refined['Status'].isin(["Selling","TEST"]),'Price_diff'] = price_check_refined['Target current price'] - price_check_refined['your_price']
        return price_check_refined
    
    def str_to_float(self, df, column_name):
        try:
            df[column_name] = df[column_name].str.replace('$',"").replace('',np.nan)
        except AttributeError:
            pass
        except Exception as e:
            print(e, column_name)
        df[column_name] = df[column_name].astype(float)
    
    def export_to_excel(self, price_check_refined):
        mm.export_to_excel(
            dfs=[price_check_refined],
            sheet_names=['price check'],
            filename='Pricelist checker.xlsx',
            out_folder=user_folder
            )
        mm.open_file_folder(user_folder)
        
    def process_prices(self):
        self.print_area.delete(0.0,ctk.END)
        self.executor.submit(self.main)
        
    def main(self):
        self.progress.start()        
        threads = [
            threading.Thread(target=self.download_sale_file, args=()),
            threading.Thread(target=self.download_fba_inventory, args=()),
            threading.Thread(target=self.download_dictionary, args=()),
            threading.Thread(target=self.download_price_list, args=()),
            ]

        for thread in threads:
            thread.start()
            
        for thread in threads:
            thread.join()
            
        sale_file = self.result_files.get('sale_file')
        event_file = self.result_files.get('event_file')
        fba_inventory = self.result_files.get('fba_inventory')
        dictionary = self.result_files.get('dictionary')
        price_list = self.result_files.get('price_list')
            
        price_check = self.merge_files(dictionary, price_list, fba_inventory, sale_file, event_file)
        price_check_refined = self.process_file(price_check)
        _ = self.export_to_excel(price_check_refined)
        self.progress.stop()        

def main():
    app = App()
    app.mainloop()     

if __name__ == '__main__':
    main()