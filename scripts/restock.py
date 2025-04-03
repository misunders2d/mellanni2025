import pandas as pd, time, pickle, os, re
import customtkinter as ctk
import tkinter as tk
from ctk_gui.ctk_windows import PopupGetDate
from utils.mellanni_modules import user_folder, open_file_folder
from common.events import event_dates
from common import excluded_collections

from concurrent.futures import ThreadPoolExecutor
import asyncio
import warnings
warnings.filterwarnings('ignore')

from classes.dataset import Dataset
from classes.product import Product

if not os.path.exists('credentials'):
    raise BaseException('Credentials folder not found!')

method_list = {name:func for name, func in Dataset.__dict__.items() if name.startswith('pull')}

start_date = (pd.to_datetime('today') - pd.DateOffset(days=181)).date()
end_date = (pd.to_datetime('today') - pd.DateOffset(days=1)).date()
default_market_list = ["US", "CA", "GB", "UK", "MX", "FR", "DE", "IT", "ES"]

ctk.set_appearance_mode('dark')
ctk.set_default_color_theme('blue')

class App(ctk.CTk):
    def __init__(self, width=1080, height=720):
        super().__init__()
        self.title('Restock')
        self.geometry(f'{width}x{height}')
        self.executor = ThreadPoolExecutor()
        self.markets = []
        self.dataset = None
        self.run_params = {}
        
        # top frame ##################################
        self.controls_frame = ctk.CTkFrame(self, width = width, height=110)
        self.controls_frame.grid_propagate(False)
        self.controls_frame.pack()
        #dates
        self.start_date_label = ctk.CTkLabel(self.controls_frame, text='Start date')
        self.start_date_label.grid(row=0, column=0, padx = 10, pady=10)
        self.start_date = ctk.CTkEntry(self.controls_frame, placeholder_text='Enter start date')
        self.start_date.bind("<Button-1>", lambda event: self.on_date_click(event, target='start_date'))
        self.start_date.insert(0, start_date)
        self.start_date.grid(row=0, column=1)

        self.end_date_label = ctk.CTkLabel(self.controls_frame, text='End date')
        self.end_date_label.grid(row=1, column=0)
        self.end_date = ctk.CTkEntry(self.controls_frame, placeholder_text='Enter end date')
        self.end_date.bind("<Button-1>", lambda event: self.on_date_click(event, target='end_date'))
        self.end_date.insert(0, end_date)
        self.end_date.grid(row=1, column=1)

        #local and save
        self.data_selector = ctk.CTkSwitch(
            self.controls_frame,
            width=120,
            text='Local data',
            progress_color='red',
            fg_color='green',
            command=self.__data_selection__)
        self.data_selector.grid(row=0, column=2, padx=50)

        #mode selection
        self.mode_selector = ctk.CTkSwitch(
            self.controls_frame,
            width=120,
            text='Restock',
            progress_color='red',
            fg_color='green',
            command=self.__mode_set__)
        self.mode_selector.grid(row=1, column=2, padx=50)

        #additional options for restock
        self.include_empty = ctk.CTkCheckBox(self.controls_frame, text='Include all SKUs in restock')
        self.include_empty.grid(row=2, column=2)


        #markets
        self.__place_labels__(default_market_list)

        #query_button
        self.dataset_button = ctk.CTkButton(self.controls_frame, text='Load\ndataset', height=40, command=lambda: self.query_dataset(method='query'))
        # self.dataset_button = ctk.CTkButton(self.controls_frame, text='Load\ndataset', height=40, command=self.dataset_query_loop)
        
        self.dataset_button.grid(row=0, column = len(self.markets)//2 + 4, rowspan = 2)

        # mid frame ########################################
        self.mid_frame = ctk.CTkFrame(self, width=width, height=550)
        self.mid_frame.grid_propagate(False)
        self.mid_frame.pack(pady=10)

        self.partial_update_label = ctk.CTkLabel(self.mid_frame, text='Update specific part')
        self.partial_update_label.grid(row=0, column=0, pady=(0, 10), padx=10, sticky='n')
        self.dataset_methods = ctk.CTkComboBox(
            self.mid_frame,
            width=200,
            values=sorted(list(method_list.keys())),
            command=lambda x: self.query_dataset(method=x))
        self.dataset_methods.grid(row=1, column=0, padx=10, sticky='n')


        self.collections_select = ctk.CTkCheckBox(self.mid_frame, text='Select all products', command=lambda: self.__select_all__('products'),state='disabled')
        self.collections_select.grid(row=0, column=1)
        #collection
        self.collection_label = ctk.CTkLabel(self.mid_frame, text='Collection')
        self.collection_label.grid(row=1, column=1)
        self.collections = tk.Listbox(
            self.mid_frame,
            activestyle='none',
            fg='whitesmoke',
            relief='flat',
            font=ctk.CTkFont(family='Helvetica',size=12),
            width=40,
            height=30,
            border=0,
            borderwidth=0,
            background='#2e2e2e',
            selectmode='multiple',
            selectforeground='black',
            selectbackground='lightblue',
            exportselection=False)
        self.collections.bind('<<ListboxSelect>>', self.__on_collection_select__)
        self.collections.grid(row=2, column=1, rowspan=3, padx=5)

        #size
        self.size_select = ctk.CTkCheckBox(self.mid_frame, text='select all sizes', command=lambda: self.__select_all__('sizes'),state='disabled')
        self.size_select.grid(row=1, column=2)
        self.sizes = tk.Listbox(
            self.mid_frame,
            activestyle='none',
            fg='whitesmoke',
            relief='flat',
            font=ctk.CTkFont(family='Helvetica',size=12),
            width=40,
            height=12,
            border=0,
            borderwidth=0,
            background='#2e2e2e',
            selectmode='multiple',
            selectforeground='black',
            selectbackground='lightblue',
            exportselection=False)
        self.sizes.bind('<<ListboxSelect>>', self.__on_size_select__)
        self.sizes.grid(row=2, column=2, sticky='n', padx=5)

        #color
        self.color_select = ctk.CTkCheckBox(self.mid_frame, text='select all colors', command=lambda: self.__select_all__('colors'),state='disabled')
        self.color_select.grid(row=3, column=2)
        self.colors = tk.Listbox(
            self.mid_frame,
            activestyle='none',
            fg='whitesmoke',
            relief='flat',
            font=ctk.CTkFont(family='Helvetica',size=12),
            width=40,
            height=13,
            border=0,
            borderwidth=0,
            background='#2e2e2e',
            selectmode='multiple',
            selectforeground='black',
            selectbackground='lightblue',
            exportselection=False)
        # self.colors.bind('<<ListboxSelect>>', self.__on_color_select__)
        self.colors.grid(row=4, column=2, sticky='s', padx=5)

        self.skus_label = ctk.CTkLabel(self.mid_frame, text='SKU/ASIN search')
        self.skus_label.grid(row=1, column=3)
        self.skus_input = ctk.CTkTextbox(self.mid_frame, height=400)
        self.skus_input.grid(row=2, column=3,columnspan=2, rowspan=3, sticky='n', padx=5)

        self.product_button = ctk.CTkButton(
            self.mid_frame,
            text='No dataset detected',
            state='disabled',
            text_color_disabled='gray',
            command=self.run_product_export)
        self.product_button.grid(row=0, column=2)

        self.product_date_from = ctk.CTkEntry(self.mid_frame, placeholder_text='Product date from', width=80)
        self.product_date_from.bind("<Button-1>", lambda event: self.on_date_click(event, target='product_start_date'))
        self.product_date_from.insert(0, start_date)
        self.product_date_from.grid(row=0, column=3)

        self.product_date_to = ctk.CTkEntry(self.mid_frame, placeholder_text='Product date to', width=80)
        self.product_date_to.bind("<Button-1>", lambda event: self.on_date_click(event, target='product_end_date'))
        self.product_date_to.insert(0, end_date)
        self.product_date_to.grid(row=0, column=4)


        # bottom frame ####################################
        self.bottom_frame = ctk.CTkFrame(self, width=width, height=100)
        self.bottom_frame.pack_propagate(False)
        self.bottom_frame.pack()
        self.status_label = ctk.CTkLabel(self.bottom_frame, text='', pady=10)
        self.status_label.pack()
        self.progress = ctk.CTkProgressBar(self.bottom_frame, width=int(width*0.8), mode='indeterminate')
        self.progress.pack()

    def run_product_export(self):
        self.executor.submit(self.export_product)

    def export_product(self):
        export_mode = 'stats' if self.mode_selector.get() else 'restock'
        selected_skus = [x for x in re.split(r'[,\n\r\t]+', self.skus_input.get(0.0, ctk.END).strip()) if x]
        selected_collections = [self.collections.get(x) for x in self.collections.curselection()]
        selected_sizes = [self.sizes.get(x) for x in self.sizes.curselection()]
        selected_colors = [self.colors.get(x) for x in self.colors.curselection()]
        selected_asins = self.dataset.dictionary[
            (self.dataset.dictionary['collection'].isin(selected_collections))
            &
            (self.dataset.dictionary['size'].isin(selected_sizes))
            &
            (self.dataset.dictionary['color'].isin(selected_colors))
            ]['asin'].unique().tolist()
        if any([selected_skus, selected_asins]):
            self.progress.start()
            self.status_label.configure(text='Please wait, processing product(s)...')
            date_from = self.product_date_from.get()
            date_to = self.product_date_to.get()

            if selected_skus:
                asins = self.dataset.dictionary[
                    (self.dataset.dictionary['sku'].isin(selected_skus)) | (self.dataset.dictionary['asin'].isin(selected_skus))
                    ]['asin'].unique().tolist()
            elif selected_asins:
                asins = selected_asins
            product = Product(asin=asins, dataset=self.dataset, start=date_from, end=date_to)
            product.populate_loop()
            # product.save_to_file()
            product.calculate_loop()
            if export_mode == 'stats':
                product.summarize()
            elif export_mode == 'restock':
                product.restock(include_empty=self.include_empty.get())
            product.export(mode=export_mode)
            self.progress.stop()
            self.status_label.configure(text=f'Done, exported data for {len(asins)} items')

    def dataset_query_loop(self):
        self.start = time.perf_counter()
        self.dataset = Dataset(
            start=self.start_date.get(),
            end=self.end_date.get(),
            market=[x.cget('text') for x in self.markets if x.get()],
            local_data = not self.data_selector.get(),
            save = self.data_selector.get())
        self.dataset.query_sync()
        self.update_status()

    def query_dataset(self, *args, **kwargs):
        self.run_params['method']=kwargs['method']
        if self.data_selector.get():
            self.executor.submit(self.run_dataset_query)
        else:
            self.dataset_query_loop()

    def run_dataset_query(self):
        self.start = time.perf_counter()
        method = self.run_params.get('method')
        def call_method(obj, method_name):
            method = getattr(obj, method_name)  # Get the method by name
            return method()
        self.progress.start()
        self.status_label.configure(text='Please wait, pulling data...')
        self.dataset = Dataset(
            start=self.start_date.get(),
            end=self.end_date.get(),
            market=[x.cget('text') for x in self.markets if x.get()],
            local_data = not self.data_selector.get(),
            save = self.data_selector.get())
        
        call_method(self.dataset, method) #call a selected method from the dropdown on dataset
        self.update_status()
        open_file_folder(user_folder)

    def update_status(self):
        total_time = time.perf_counter() - self.start
        self.progress.stop()
        self.status_label.configure(
            text=f'''Dataset queried for {self.start_date.get()} - {self.end_date.get()}, markets: {', '.join([x.cget('text') for x in self.markets if x.get()])} from {self.data_selector.cget('text')} in {total_time:.1f} seconds'''
            )
        self.product_button.configure(state='normal', text='Download\nproduct')
        self.collections_select.configure(state='normal')
        self.color_select.configure(state='normal')
        self.size_select.configure(state='normal')
        if self.run_params.get('method') == 'query':
            collections = sorted(self.dataset.dictionary['collection'].unique().tolist())
            collections = [x for x in collections if x not in excluded_collections]
            self.collections.delete(0, tk.END)
            [self.collections.insert(tk.END, c) for c in collections]

    def __on_collection_select__(self, *args):
        selected_collections = [self.collections.get(x) for x in self.collections.curselection()]
        if selected_collections:
            potential_sizes = self.dataset.dictionary[self.dataset.dictionary['collection'].isin(selected_collections)]['size'].unique().tolist()
            self.sizes.delete(0, ctk.END)
            if potential_sizes:
                [self.sizes.insert(tk.END, ps) for ps in sorted(potential_sizes)]

    def __on_size_select__(self, *args):
        selected_collections = [self.collections.get(x) for x in self.collections.curselection()]
        selected_sizes = [self.sizes.get(x) for x in self.sizes.curselection()]
        if selected_sizes:
            potential_colors = self.dataset.dictionary[
                (self.dataset.dictionary['collection'].isin(selected_collections)) & (self.dataset.dictionary['size'].isin(selected_sizes))
                ]['color'].unique().tolist()
            self.colors.delete(0, ctk.END)
            if potential_colors:
                [self.colors.insert(tk.END, color) for color in sorted(potential_colors)]

    def __on_color_select__(self, *args):
        selected_collections = [self.collections.get(x) for x in self.collections.curselection()]
        selected_sizes = [self.sizes.get(x) for x in self.sizes.curselection()]
        selected_colors = [self.colors.get(x) for x in self.colors.curselection()]
        selected_asins = self.dataset.dictionary[
            (self.dataset.dictionary['collection'].isin(selected_collections))
            &
            (self.dataset.dictionary['size'].isin(selected_sizes))
            &
            (self.dataset.dictionary['color'].isin(selected_colors))
            ]['asin'].unique().tolist()


    def __data_selection__(self):
        if self.data_selector.get():
            self.data_selector.configure(text="Cloud data")
            self.start_date.configure(state='normal')
        else:
            self.data_selector.configure(text="Local data")
            self.start_date.configure(state='disabled')

    def __mode_set__(self):
        if self.mode_selector.get():
            self.mode_selector.configure(text="Full stats")
            self.include_empty.grid_remove()
        else:
            self.mode_selector.configure(text="Restock")
            self.include_empty.grid()

    def __place_labels__(self, market_list):
        row, column = 0, 3
        for market in market_list:
            market_checkbox = ctk.CTkCheckBox(self.controls_frame, text = market)
            market_checkbox.grid(row = row, column = column)
            if market == 'US':
                market_checkbox.select()
            self.markets.append(market_checkbox)
            row += 1
            if row == 2:
                row = 0
                column += 1
        self.select_all = ctk.CTkCheckBox(self.controls_frame, text='All', command=lambda: self.__select_all__('markets'))
        self.select_all.grid(row = row, column = column)

    def __select_all__(self, target):
        if target=='markets':
            [x.select() if self.select_all.get() else x.deselect() for x in self.markets ]
        elif target=='products':
            if self.collections_select.get():
                self.collections.select_set(0, tk.END)
                self.__on_collection_select__()
            else:
                self.collections.select_clear(0, tk.END)
                self.__on_collection_select__()
        elif target=='sizes':
            if self.size_select.get():
                self.sizes.select_set(0, tk.END)
                self.__on_size_select__()
            else:
                self.sizes.select_clear(0, tk.END)
                self.__on_size_select__()
        elif target=='colors':
            if self.color_select.get():
                self.colors.select_set(0, tk.END)
                # self.__on_color_select__()
            else:
                self.colors.select_clear(0, tk.END)
                # self.__on_color_select__()
    
    def on_date_click(self, event, target):
        if target == 'start_date':
            widget = self.start_date
        elif target == 'end_date':
            widget = self.end_date
        elif target == 'product_start_date':
            widget = self.product_date_from
        elif target == 'product_end_date':
            widget = self.product_date_to
        if widget.cget('state') =='normal':
            selected_date = PopupGetDate().get_date()
            if selected_date:
                widget.delete(0, ctk.END)
                widget.insert(0,selected_date)

def get_event_sales():
    """calculates pre-event and event averages for all SKUs since 2022"""
    all_files = []

    def process_sku(sku):
        print(f"\r{' '*150}\r{len(all_files)} out of {len(skus)} processed, working on {sku}", end='', flush=True)
        sku_event_sales = event_sales[event_sales['sku'] == sku]
        sku_non_event_sales = non_event_sales[non_event_sales['sku'] == sku]
        
        sku_file = pd.DataFrame(data=[sku], columns=['sku'])
        for event, dates in event_dates.items():
            pre_event_sales = sku_non_event_sales[sku_non_event_sales['date'].between(min(dates)-pd.Timedelta(days=61), min(dates), inclusive='left')]
            the_event_sales = sku_event_sales[sku_event_sales['date'].between(min(dates), max(dates), inclusive='both')]
            sku_file[f'pre-{event} average sales'] = pre_event_sales['unitsOrdered'].mean()
            sku_file[f'{event} units sold'] = the_event_sales['unitsOrdered'].sum()
            sku_file[f'{event} duration, days'] = len(dates)
            sku_file[f'{event} average sales'] = sku_file[f'{event} units sold'] /  sku_file[f'{event} duration, days']
            sku_file[f'{event} X increase'] = sku_file[f'{event} average sales'] / sku_file[f'pre-{event} average sales']
    
        # result = pd.concat([result, sku_file])
        all_files.append(sku_file)

    dataset = Dataset(start="2022-01-01", end="2025-12-31", market="US", local_data=False, save=False)
    dataset.pull_br_data()
    sales = dataset.br.copy()

    sales = sales[['date','sku','unitsOrdered']]
    sales['date'] = pd.to_datetime(sales['date']).dt.date
    sales = sales.sort_values(['date','sku'])
    sales = sales[~sales['sku'].str.lower().str.contains('.missing|.found')]
    sales = sales.groupby(['date','sku']).agg('sum').reset_index()
    non_event_sales = sales[~sales['date'].isin([d for value in event_dates.values() for d in value])]
    event_sales = sales[sales['date'].isin([d for value in event_dates.values() for d in value])]

    skus = sales['sku'].unique().tolist()

    with ThreadPoolExecutor() as pool:
        pool.map(process_sku, skus)

    print(f'\n{len(all_files)} processed')
    result = pd.concat(all_files)

    result.to_excel(os.path.join(user_folder, 'event_sales_test.xlsx'), index=False)    

def main():
    app = App()
    app.mainloop()

if __name__ == "__main__":
    main()

