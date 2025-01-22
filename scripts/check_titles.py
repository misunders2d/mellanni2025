from utils import mellanni_modules as mm
import os
import pandas as pd
import customtkinter as ctk
from concurrent.futures import ThreadPoolExecutor

from common import user_folder
from connectors import gcloud as gc

output = user_folder

class TitleChecker(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.geometry('400x300')
        self.title('Title checker')
        self.executor = ThreadPoolExecutor()
        self.client = gc.gcloud_connect()
        self.file_dir = None
        self.market = ctk.StringVar(self,value='US')
        self.query = f'''SELECT sku, product_name
                        FROM `reports.fba_inventory_planning`
                        WHERE marketplace = "{self.market.get()}"
                        AND snapshot_date = (
                            SELECT MAX (snapshot_date) FROM `reports.fba_inventory_planning` WHERE marketplace = "{self.market.get()}"
                            )
                        '''
        self.market_radio_us = ctk.CTkRadioButton(self, text='US', value="US", variable=self.market)
        self.market_radio_us.pack(pady=10)

        self.market_radio_ca = ctk.CTkRadioButton(self, text='CA', value="CA",variable=self.market)
        self.market_radio_ca.pack(pady=10)

        self.folder_label = ctk.CTkLabel(self, text='No folder selected')
        self.folder_label.pack(pady=10)

        self.folder_button = ctk.CTkButton(self, text='1. Select folder with flat files', command=self.select_folder)
        self.folder_button.pack(pady=10)

        self.ok_button = ctk.CTkButton(self, text='2. GO', command=self.start)
        self.ok_button.pack(pady=10)

        self.progresbar = ctk.CTkProgressBar(self, mode='indeterminate')
        self.progresbar.pack(pady=10)

        self.warning_label = ctk.CTkLabel(self, text='')
        self.warning_label.pack(pady=10)

    def start(self):
        self.executor.submit(self.process_files)
        self.progresbar.start()

    def process_files(self):
        try:
            inv_file = self.read_inventory()
            full_list = self.read_flat_files()
            dictionary = self.read_dictionary()
            result = pd.merge(full_list, inv_file, how = 'left', on = 'sku')
            result = result[~result['sku'].str.lower().str.contains('parent')]
            mismatch = result[result['Flat File title'] != result['Listing title']]
            mismatch = mismatch.dropna()
            if len(mismatch) > 0:
                mismatch = pd.merge(mismatch, dictionary, how ='left', on='sku')
                mm.export_to_excel([mismatch],['mismatched_titles'],'title_check.xlsx', out_folder=user_folder)
                mm.open_file_folder(user_folder)
            else:
                self.warning_label.configure(text='No mismatch found')
        except Exception as e:
            print(e)
        self.progresbar.stop()


    def select_folder(self):
        self.file_dir = ctk.filedialog.askdirectory(initialdir=user_folder)
        self.folder_label.configure(text=self.file_dir)

    def read_flat_files(self):
        if not self.file_dir:
            return
        files = [x for x in os.listdir(self.file_dir) if '.xlsm' in os.path.basename(x) and '~' not in x]
        if len(files) == 0:
            print('No files found, exiting')
        self.warning_label.configure(text=f'Reading {len(files)} flat files')
        full_list = pd.DataFrame()
        for file in files:
            try:
                column_file = pd.read_excel(os.path.join(self.file_dir, file), sheet_name='Template', skiprows=4, nrows=1)
                cols_to_use = [x for x in column_file.columns if any(['contribution_sku' in x, 'item_name' in x])]
                temp_file = pd.read_excel(os.path.join(self.file_dir, file), sheet_name='Template', skiprows=4, usecols=cols_to_use)
                full_list = pd.concat([full_list, temp_file])
            except Exception as e:
                print(e)
        sku_col = [x for x in cols_to_use if 'sku' in x][0]
        name_col = [x for x in cols_to_use if 'item_name' in x][0]
        full_list = full_list.rename(columns = {sku_col:'sku',name_col:'Flat File title'})
        duplicates = full_list['sku'].duplicated().sum()
        if duplicates > 0:
            self.warning_label.configure(text=f'There are {duplicates} duplicates in the folder, please check.')
        return full_list

    def read_inventory(self):
        self.warning_label.configure(text='Reading inventory file')
        inv_file: pd.DataFrame = self.client.query(self.query).to_dataframe()
        inv_file = inv_file.rename(columns = {'product_name':'Listing title'})
        return inv_file

    def read_dictionary(self):
        self.warning_label.configure(text='Reading dictionary')
        query = 'SELECT sku, collection FROM `auxillary_development.dictionary`'
        client = gc.gcloud_connect()
        dictionary = client.query(query).to_dataframe()
        return dictionary

def main():
    app = TitleChecker()
    app.mainloop()

if __name__ == '__main__':
    main()