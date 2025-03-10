import pandas as pd, time, pickle
import customtkinter as ctk
import tkinter as tk
from ctk_gui.ctk_windows import PopupGetDate
from utils.mellanni_modules import user_folder
from common import excluded_collections

from concurrent.futures import ThreadPoolExecutor
import asyncio
import warnings
warnings.filterwarnings('ignore')

from classes.dataset import Dataset
from classes.product import Product
method_list = {name:func for name, func in Dataset.__dict__.items() if name.startswith('pull')}

start_date = "2025-01-01"
end_date = "2025-12-31"
default_market_list = ["US", "CA", "GB", "UK", "MX", "FR", "DE", "IT", "ES"]

class Restock(ctk.CTk):
    def __init__(self, width=1080, height=720):
        super().__init__()
        self.title('Restock')
        self.geometry(f'{width}x{height}')
        self.executor = ThreadPoolExecutor()
        self.markets = []
        self.dataset = None
        self.run_params = {}
        
        # top frame ##################################
        self.controls_frame = ctk.CTkFrame(self, width = width, height=80)
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


        self.collections_select = ctk.CTkCheckBox(self.mid_frame, text='Select all products', command=lambda: self.__select_all__('products'))
        self.collections_select.grid(row=0, column=1)
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
            selectbackground='lightblue')
        self.collections.grid(row=1, column=1)

        self.product_button = ctk.CTkButton(
            self.mid_frame,
            text='No dataset detected',
            state='disabled',
            text_color_disabled='gray',
            command=self.run_product_export)
        self.product_button.grid(row=0, column=2)

        self.product_date_from = ctk.CTkEntry(self.mid_frame, placeholder_text='Product date from')
        self.product_date_from.bind("<Button-1>", lambda event: self.on_date_click(event, target='product_start_date'))
        self.product_date_from.insert(0, start_date)
        self.product_date_from.grid(row=0, column=3)

        self.product_date_to = ctk.CTkEntry(self.mid_frame, placeholder_text='Product date to')
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
        selected = self.collections.curselection()
        selected_collections = [self.collections.get(x) for x in selected]
        if selected_collections:
            self.progress.start()
            self.status_label.configure(text='Please wait, processing product(s)...')
            asins = self.dataset.dictionary[self.dataset.dictionary['collection'].isin(selected_collections)]['asin'].unique().tolist()
            date_from = self.product_date_from.get()
            date_to = self.product_date_to.get()
            product = Product(asin=asins, dataset=self.dataset)
            product.populate_loop()
            product.calculate_loop(start=date_from, end=date_to)
            product.export()
            self.progress.stop()
            self.status_label.configure(text=f'Done, exported data for {len(asins)} items')

    def dataset_query_loop(self):
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

    def update_status(self):
        self.progress.stop()
        self.status_label.configure(
            text=f'''Dataset queried for {self.start_date.get()} - {self.end_date.get()}, markets: {', '.join([x.cget('text') for x in self.markets if x.get()])} from {self.data_selector.cget('text')}'''
            )
        self.product_button.configure(state='normal', text='Download\nproduct')
        if self.run_params.get('method') == 'query':
            collections = self.dataset.dictionary['collection'].unique().tolist()
            collections = [x for x in collections if x not in excluded_collections]
            self.collections.delete(0, tk.END)
            [self.collections.insert(tk.END, c) for c in collections]

    def __data_selection__(self):
        self.data_selector.configure(text="Cloud data") if self.data_selector.get() else self.data_selector.configure(text="Local data")

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
            self.collections.select_set(0, tk.END) if self.collections_select.get() else self.collections.select_clear(0, tk.END)
    
    def on_date_click(self, event, target):
        selected_date = PopupGetDate().get_date()
        if selected_date:
            if target == 'start_date':
                widget = self.start_date
            elif target == 'end_date':
                widget = self.end_date
            elif target == 'product_start_date':
                widget = self.product_date_from
            elif target == 'product_end_date':
                widget = self.product_date_to

            widget.delete(0, ctk.END)
            widget.insert(0,selected_date)


def main():
    app = Restock()
    app.mainloop()

if __name__ == "__main__":
    main()


def temp():
    start = time.perf_counter()
    dataset = Dataset(start=start_date, end=end_date, local_data=True, save=False, market="*")
    # dataset.query_sync()
    dataset.query()
    # dataset.pull_advertised_product_data()
    # dataset.pull_purchased_product_data()
    # dataset.pull_changelog()
    # dataset.warehouse.to_excel('/home/misunderstood/temp/wh.xlsx', index = False)
    # dataset.pull_fees_dimensions()
    # dataset.pull_pricing()
    # dataset.pull_cogs()
    # dataset.pull_promotions()
    # dataset.pull_inventory_history()
    # dataset.pull_dictionary()
    # dataset.inventory.to_excel('/home/misunderstood/inventory.xlsx')


    def aggregate_ppc_data(dataset):
        # advertised = dataset.
        pass



    # asins = dataset.dictionary[dataset.dictionary['collection'].str.lower().str.contains('zipper')]['asin'].unique().tolist()
    # asins = dataset.dictionary['asin'].unique().tolist()

    # products = Product(asin = asins, dataset = dataset)
    products = Product(sku = ['FAUX-FUR-TH_BLANKET-60X80-TH-BLACK','FAUX-FUR-TH_BLANKET-50X60-TH-BLACK','M-Flat-Sheet-F-Gray-FBA',
                            'M-VELVET-ULTRASONIC-K-CK-BLUSH-PINK','M-Flat-Sheet-Q-Royal-Blue-FBA','M-21-FITTED-SHEET-K-IMPERIAL-BLUE',
                            'M-Flat-Sheet-F-Beige-FBA','M-21-FITTED-SHEET-CK-IMPERIAL-BLUE','M-21-FITTED-SHEET-CK-BEIGE',
                            'M-21-FITTED-SHEET-CK-BURGUNDY'],
                            dataset=dataset)
    # products = [Product(asin=asin, dataset = dataset) for asin in asins]
    # products = products[100:300]

    def process_single_prduct(products, start = start_date, end = end_date):
        products.populate_loop()
        # products._calculate_incoming(start, end)
        products.calculate_loop(start, end)
        products.export()

    def process_products(products, start = start_date, end = end_date):
        """a set of async functions that run "populate" and "calculcate" methods on each product"""
        
        async def populate_product(product: Product): #async populate a list of products
            await product.populate()
        async def populate_products(products):
            total_products = len(products)
            tasks = [populate_product(product) for product in products]
            for task in asyncio.as_completed(tasks):
                await task
                total_products -= 1
                print(f"\r{' ' * 150}\rProducts to populate remaining: {total_products}", end='', flush=True)
            print()
            # await asyncio.gather(*tasks)

        async def calculcate_product(product: Product):
            await product.calculate(start, end)
        async def calculcate_products(products):
            total_products = len(products)
            tasks = [calculcate_product(product) for product in products]
            for task in asyncio.as_completed(tasks):
                await task
                total_products -= 1
                print(f"\r{' ' * 150}\rProducts to calculate remaining: {total_products}", end='', flush=True)
            print()

        asyncio.run(populate_products(products))
        asyncio.run(calculcate_products(products))

    process_single_prduct(products, start="2025-02-15")
    # process_products(products)



        # product.calculate_loop(start_date, end_date)
    # products.export()
    # product = Product(asin='B0822X1VP7', dataset=dataset)
    # product._calculate_inventory_history(start=start_date, end=end_date)
    # product._pull_inventory_history()
    # products.populate_loop()
    # products.calculate_loop("2025-02-28", end_date)
    # product.inventory_history_df.to_excel('/home/misunderstood/temp/inventory_history.xlsx')

    print(f'Total time: {time.perf_counter() - start:.1f} seconds')

    # with open('/home/misunderstood/temp/products.pkl','rb') as f:
    #     products = pickle.load(f)

    # for product in products:
    #     try:
    #         # product.populate_loop()
    #         product._calculate_inventory_history(start=start_date, end=end_date)
    #     except Exception as e:
    #         print(f'Population error with {product.asins}: {e}')
    # with open('/home/misunderstood/temp/products.pkl','wb') as f:
    #     pickle.dump(products, f)
    # try:
    #     product.calculate_loop(start_date, end_date)
    # except Exception as e:
    #     print(f'Calculation error with {product.asins}: {e}')
    # print(product.stats)


    # async def populate_product(product:Product):
    #     await asyncio.to_thread(product.populate_loop)

    # async def main():
    #     tasks = [populate_product(product) for product in products]
    #     await asyncio.gather(*tasks)

    # start = time.perf_counter()
    # asyncio.run(main())
    # end = time.perf_counter() - start
    # print('async loop finished in ', round(end, 3), ' seconds')



    # product = [x for x in products if 'B00NQDGAP2' in x.asins][0]
    # # product = Product(sku='M-BEDSHEETSET-T-OXP-SAGE', dataset=dataset)
    # # # print(product.orders_df)
    # product.populate_loop()
    # # # product.calculate_loop(start_date, end_date)

    # product._calculate_inventory(start_date, end_date)
    # # print(product.stats)
    # print(product.inventory)
    # product.inventory.to_excel('/home/misunderstood/temp/product_inventory.xlsx', index=False)
