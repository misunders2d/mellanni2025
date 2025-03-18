
from classes.dataset import Dataset
import pandas as pd
from utils import mellanni_modules as mm
from utils.decorators import error_checker

from common import user_folder
import os, pickle
import asyncio
from numpy import nan
import pandas as pd

class Product:
    dataset = None
    def __init__(self,
                 asin:str|list=None,
                 sku:str|list=None,
                 dataset:Dataset=None,
                 start="2025-01-01",
                 end="2025-12-31"):
        if not (asin or sku):
            raise ValueError("Either 'asin' or 'sku' must be provided.")
        if asin and sku:
            raise ValueError("Only one of 'asin' or 'sku' must be provided.")
        if not dataset:
            raise ValueError("A dataset must be provided.")
        
        self.start = pd.to_datetime(start).date()
        self.end = pd.to_datetime(end).date()

        self.asins = set()
        self.skus = set()
        self.collections = set()
        self.sub_collections = set()
        self.sizes = set()
        self.colors = set()
        Product.dataset = dataset
        self.dataset_delete = False # delete the lines for this specific set of asins from Product.dataset
        self.combined_dfs = {}
        self.stats = {}
        if asin and isinstance(asin, str):
            self.asins.add(asin)
        elif asin and isinstance(asin, list):
            self.asins.update(asin)
        if sku and isinstance(sku, str):
            self.skus.add(sku)
        elif sku and isinstance(sku, list):
            self.skus.update(sku)
        self._init_skus()

    def _init_skus(self):
        dictionary = Product.dataset.dictionary
        # pull initial dictionary data for the provided asin or sku
        id_list = dictionary[
            (dictionary['asin'].isin(self.asins)) | (dictionary['sku'].isin(self.skus))
            ][['sku', 'asin', 'collection','sub-collection','size','color']].values.tolist()
        self.skus.update({x[0] for x in id_list})
        self.asins.update({x[1] for x in id_list})

        # pull additional dictionary data based on the updated asins and skus
        id_list = dictionary[
            (dictionary['asin'].isin(self.asins)) | (dictionary['sku'].isin(self.skus))
            ][['sku', 'asin', 'collection','sub-collection','size','color']].values.tolist()
        self.skus.update({x[0] for x in id_list})
        self.asins.update({x[1] for x in id_list})
        self.collections.update({x[2] for x in id_list})
        self.sub_collections.update({x[3] for x in id_list})
        self.sizes.update({x[4] for x in id_list})
        self.colors.update({x[5] for x in id_list})
    
    def _update_ids(self, df):
        if 'sku' in df.columns:
            self.skus.update(df['sku'].unique())
        if 'asin' in df.columns:
            self.asins.update(df['asin'].unique())

    def __attach_collection__(self, df):
        product_dict = Product.dataset.dictionary[
            (Product.dataset.dictionary['sku'].isin(self.skus))
            |
            (Product.dataset.dictionary['asin'].isin(self.asins))
            ][['sku', 'asin', 'collection', 'sub-collection', 'size', 'color']]

        result = df.copy()
        
        if 'sku' in df.columns:
            merged_sku = pd.merge(
                df,
                product_dict[['sku', 'collection', 'sub-collection', 'size', 'color']].drop_duplicates('sku'),
                on='sku',
                how='left'
                )
            result[['collection', 'sub-collection', 'size', 'color']]\
                = merged_sku[['collection','sub-collection', 'size', 'color']].values
                
        if 'asin' in df.columns:
            merged_asin = pd.merge(
                df,
                product_dict[['asin', 'collection', 'sub-collection', 'size', 'color']].drop_duplicates('asin'),
                on='asin',
                how='left')
            result[['collection', 'sub-collection', 'size', 'color']]\
                = merged_asin[['collection','sub-collection', 'size', 'color']].values
        
        if all(['sku' in df.columns, 'asin' in df.columns]):
            result[['collection', 'sub-collection', 'size', 'color']]\
                =merged_sku[
                    ['collection','sub-collection', 'size', 'color']
                    ].fillna(merged_asin[
                        ['collection', 'sub-collection', 'size', 'color']
                        ]).values
        return result

    def __attach_marketplace__(self, df, channel_column):
        df['marketplace'] = df[channel_column].apply(
            lambda x: [key for key,value in Product.dataset.channels_mapping.items() if x.lower()==value.lower()][0]
            )
        return df
    
    def __str__(self):
        collections = ' | '.join(self.collections)
        sub_collections = ' | '.join(self.sub_collections)
        sizes = ' | '.join(self.sizes)
        colors = ' | '.join(self.colors)
        skus = ', '.join(self.skus)
        asins = ', '.join(self.asins)
        return f'Collection: {collections}\nSub-collection: {sub_collections}\nSize: {sizes} - Color: {colors}\nSKUs: {skus}\nASINs: {asins}'

    ### Populate data section ###
    def _pull_orders(self):
        self.orders_df = Product.dataset.orders[(Product.dataset.orders['sku'].isin(self.skus)) | (Product.dataset.orders['asin'].isin(self.asins))]
        if self.dataset_delete:
            Product.dataset.orders = Product.dataset.orders[~Product.dataset.orders.index.isin(self.orders_df.index)]
        self._update_ids(self.orders_df)

    def _pull_br(self):
        self.br_df = Product.dataset.br[(Product.dataset.br['sku'].isin(self.skus)) | (Product.dataset.br['asin'].isin(self.asins))]
        if self.dataset_delete:
            Product.dataset.br = Product.dataset.br[~Product.dataset.br.index.isin(self.br_df.index)]
        self._update_ids(self.br_df)

    def _pull_br_asin(self):
        self.br_asin_df = Product.dataset.br_asin[Product.dataset.br_asin['asin'].isin(self.asins)]
        if self.dataset_delete:
            Product.dataset.br_asin = Product.dataset.br_asin[~Product.dataset.br_asin.index.isin(self.br_asin_df.index)]
        self._update_ids(self.br_asin_df)

    def _pull_inventory(self):
        self.inventory_df = Product.dataset.inventory[(Product.dataset.inventory['sku'].isin(self.skus)) | (Product.dataset.inventory['asin'].isin(self.asins))]
        if self.dataset_delete:
            Product.dataset.inventory = Product.dataset.inventory[~Product.dataset.inventory.index.isin(self.inventory_df.index)]
        self._update_ids(self.inventory_df)

    def _pull_inventory_history(self):
        self.inventory_history_df = Product.dataset.inventory_history[(Product.dataset.inventory_history['sku'].isin(self.skus)) | (Product.dataset.inventory_history['asin'].isin(self.asins))]
        if self.dataset_delete:
            Product.dataset.inventory_history = Product.dataset.inventory_history[~Product.dataset.inventory_history.index.isin(self.inventory_history_df.index)]
        self._update_ids(self.inventory_history_df)

    def _pull_advertised_product(self):
        self.advertised_product_df = Product.dataset.advertised_product[(Product.dataset.advertised_product['sku'].isin(self.skus)) | (Product.dataset.advertised_product['asin'].isin(self.asins))]
        if self.dataset_delete:
            Product.dataset.advertised_product = Product.dataset.advertised_product[~Product.dataset.advertised_product.index.isin(self.advertised_product_df.index)]
        self._update_ids(self.advertised_product_df)

    def _pull_purchased_product(self):
        self.purchased_product_df = Product.dataset.purchased_product[
            (Product.dataset.purchased_product['sku'].isin(self.skus)) |
            (Product.dataset.purchased_product['asin'].isin(self.asins)) |
            (Product.dataset.purchased_product['purchasedAsin'].isin(self.asins))
            ]

    def _pull_promotions(self):
        self.promotions_df = Product.dataset.promotions[(Product.dataset.promotions['sku'].isin(self.skus))]
        if self.dataset_delete:
            Product.dataset.promotions = Product.dataset.promotions[~Product.dataset.promotions.index.isin(self.promotions_df.index)]
        self._update_ids(self.promotions_df)

    def _pull_returns(self):
        self.returns_df = Product.dataset.returns[(Product.dataset.returns['sku'].isin(self.skus)) | (Product.dataset.returns['asin'].isin(self.asins))]
        if self.dataset_delete:
            Product.dataset.returns = Product.dataset.returns[~Product.dataset.returns.index.isin(self.returns_df.index)]
        self._update_ids(self.returns_df)

    def _pull_fees_dimensions(self):
        self.fees_dimensions_df = Product.dataset.fees[(Product.dataset.fees['sku'].isin(self.skus)) | (Product.dataset.fees['asin'].isin(self.asins))]
        if self.dataset_delete:
            Product.dataset.fees = Product.dataset.fees[~Product.dataset.fees.index.isin(self.fees_dimensions_df.index)]
        self._update_ids(self.fees_dimensions_df)

    def _pull_warehouse(self):
        self.warehouse_df = Product.dataset.warehouse[Product.dataset.warehouse['sku'].isin(self.skus)]
        if self.dataset_delete:
            Product.dataset.warehouse = Product.dataset.warehouse[~Product.dataset.warehouse.index.isin(self.warehouse_df.index)]
        self._update_ids(self.warehouse_df)
    
    def _pull_changelog(self):
        self.changelog_df = Product.dataset.changelog[(Product.dataset.changelog['sku'].isin(self.skus))]
        if self.dataset_delete:
            Product.dataset.changelog = Product.dataset.changelog[~Product.dataset.changelog.index.isin(self.changelog_df.index)]
    
    def _pull_incoming(self):
        self.incoming_df = Product.dataset.incoming[Product.dataset.incoming['sku'].isin(self.skus)]
        if self.dataset_delete:
            Product.dataset.incoming = Product.dataset.incoming[~Product.dataset.incoming.index.isin(self.incoming_df.index)]

    def _pull_pricing(self):
        self.pricing_df = Product.dataset.pricing[Product.dataset.pricing['sku'].isin(self.skus)]
        if self.dataset_delete:
            Product.dataset.pricing = Product.dataset.pricing[~Product.dataset.pricing.index.isin(self.pricing_df.index)]

    def _pull_cogs(self):
        self.cogs_df = Product.dataset.cogs[Product.dataset.cogs['sku'].isin(self.skus)]
        if self.dataset_delete:
            Product.dataset.cogs = Product.dataset.cogs[~Product.dataset.cogs.index.isin(self.cogs_df.index)]

    ### async section ###
    async def _pull_data(self, pull_function):
        await asyncio.to_thread(pull_function)

    async def populate(self):
        """ function that pulls all data for a product from the provided Product.dataset """
        tasks = [
            self._pull_data(self._pull_orders),
            self._pull_data(self._pull_br),
            self._pull_data(self._pull_br_asin),
            self._pull_data(self._pull_inventory),
            self._pull_data(self._pull_inventory_history),
            self._pull_data(self._pull_advertised_product),
            self._pull_data(self._pull_purchased_product),
            self._pull_data(self._pull_promotions),
            self._pull_data(self._pull_returns),
            self._pull_data(self._pull_fees_dimensions),
            self._pull_data(self._pull_warehouse),
            self._pull_data(self._pull_changelog),
            self._pull_data(self._pull_incoming),
            self._pull_data(self._pull_pricing),
            self._pull_data(self._pull_cogs)
        ]
        await asyncio.gather(*tasks)

    ### normal loop section ###
    def populate_loop(self):
        """ function that pulls all data for a product from the provided Product.dataset """
        self._pull_orders()
        self._pull_br()
        self._pull_br_asin()
        self._pull_inventory()
        self._pull_inventory_history()
        self._pull_advertised_product()
        self._pull_purchased_product()
        self._pull_promotions()
        self._pull_returns()
        self._pull_fees_dimensions()
        self._pull_warehouse()
        self._pull_changelog()
        self._pull_incoming()
        self._pull_pricing()
        self._pull_cogs()


    ### calculations section ###########################################################################################
    def _calculate_br_asin(self):
        self.br_asin_df['date'] = pd.to_datetime(self.br_asin_df['date']).dt.date
        br_asin = self.br_asin_df[
            ['date', 'asin', 'browserSessions','browserSessionsB2B', 'mobileAppSessions',
             'mobileAppSessionsB2B','sessions', 'sessionsB2B', 'browserPageViews', 'country_code',
             'browserPageViewsB2B','mobileAppPageViews', 'mobileAppPageViewsB2B', 'pageViews','pageViewsB2B']
             ].copy()

        br_asin = br_asin[(br_asin['date'] >= self.start) & (br_asin['date'] <= self.end)]
        br_asin = br_asin.groupby(['country_code','date', 'asin']).agg('sum').reset_index()
        sum_cols = br_asin.columns[3:]
        agg_dict = {col:'sum' for col in sum_cols}
        agg_dict['date'] = lambda x: [len(x),min(x), max(x)]
        br_asin_sum = br_asin.groupby(['country_code', 'asin']).agg(agg_dict).reset_index()
        if br_asin_sum['date'].tolist():
            br_asin_sum[['# days','min_date','max_date']] = br_asin_sum['date'].tolist()
        else:
            br_asin_sum[['# days','min_date','max_date']] = [[0,0,0]]
        del br_asin_sum['date']
        self.combined_dfs['br_asin'] = br_asin
        self.stats['br_asin_detailed'] = br_asin_sum
        br_asin_total = br_asin_sum[
            ['country_code','browserSessions', 'browserSessionsB2B','mobileAppSessions',
             'mobileAppSessionsB2B', 'sessions', 'sessionsB2B','browserPageViews',
             'browserPageViewsB2B', 'mobileAppPageViews','mobileAppPageViewsB2B',
             'pageViews', 'pageViewsB2B', '# days']
            ]
        self.stats['br_asin_total'] = br_asin_total.groupby(['country_code']).agg('sum').reset_index()

    def _calculate_br(self):
        self.br_df['date'] = pd.to_datetime(self.br_df['date']).dt.date
        br = self.br_df[
            ['date', 'sku', 'asin', 'unitsOrdered', 'unitsOrderedB2B',
             'orderedProductSales', 'orderedProductSalesB2B','country_code']
            ].copy()

        br = br[(br['date'] >= self.start) & (br['date'] <= self.end)]
        br = br.groupby(['country_code','date', 'sku', 'asin']).agg('sum').reset_index()
        
        br_sum = br.groupby(['country_code', 'sku', 'asin']).agg(
            {'unitsOrdered':'sum','unitsOrderedB2B':'sum',
             'orderedProductSales':'sum', 'orderedProductSalesB2B':'sum',
             'date':lambda x: [len(x),min(x), max(x)]}).reset_index()
        if br_sum['date'].tolist():
            br_sum[['# days','min_date','max_date']] = br_sum['date'].tolist()
        else:
            br_sum[['# days','min_date','max_date']] = [[0,0,0]]
        del br_sum['date']
        self.combined_dfs['br'] = br
        self.stats['br_detailed'] = br_sum
        br_total = br_sum[['country_code', 'unitsOrdered', 'unitsOrderedB2B',
                       'orderedProductSales','orderedProductSalesB2B', '# days']]
        self.stats['br_total'] = br_total.groupby(['country_code']).agg('sum').reset_index()

    def _calculate_orders(self):
        self.orders_df['pacific_date'] = pd.to_datetime(self.orders_df['pacific_date']).dt.date
        orders = self.orders_df[(self.orders_df['pacific_date'] >= self.start) & (self.orders_df['pacific_date'] <= self.end)]
        orders = orders.groupby(['pacific_date','sku','sales_channel']).agg({'units_sold':'sum', 'sales':'sum'}).reset_index()
        orders_sum = orders.groupby(['sales_channel','sku']).agg(
            {'units_sold':'sum', 'sales':'sum', 'pacific_date':lambda x: [len(x),min(x), max(x)]}).reset_index()
        if orders_sum['pacific_date'].tolist():
            orders_sum[['# days','min_date','max_date']] = orders_sum['pacific_date'].tolist()
        else:
            orders_sum[['# days','min_date','max_date']] = [[0,0,0]]
        del orders_sum['pacific_date']
        self.combined_dfs['orders'] = orders
        self.stats['orders_detailed'] = orders_sum
        self.stats['orders_total'] = orders_sum.groupby('sales_channel')[['units_sold', 'sales', '# days']].agg('sum').reset_index()
        
    def _calculate_inventory(self):
        self.inventory_df['date'] = pd.to_datetime(self.inventory_df['date']).dt.date
        inventory = self.inventory_df[(self.inventory_df['date'] >= self.start) & (self.inventory_df['date'] <= self.end)]
        self.stats['inventory_detailed'] = inventory
        inventory_total = inventory.drop(
            [
                'date', 'sku', 'asin','storage_type','sales_rank','your_price','sales_price','sell_through',
                'fba_inventory_level_health_status'
                ], axis=1)
        self.stats['inventory_total'] = inventory_total.groupby('marketplace').agg('sum').reset_index()

    def _calculate_inventory_history(self):
        start, end, today = pd.to_datetime(self.start).date(), pd.to_datetime(self.end).date(), (pd.to_datetime('today')-pd.Timedelta(days=1)).date()
        if not 'inventory_history_df' in self.__dict__:
            self._pull_inventory_history()
        self.inventory_history_df['date'] = pd.to_datetime(self.inventory_history_df['date']).dt.date
        self.inventory_history = self.inventory_history_df[(self.inventory_history_df['date'] >= start) & (self.inventory_history_df['date'] <= end)]
        self.inventory_history = self.inventory_history.groupby(
            ['date','sku','asin','marketplace']).agg('sum').reset_index()
        self.combined_dfs['inventory_history'] = self.inventory_history
        isr_df = self.inventory_history.copy()
        n_days = (min(end, today) - start).days
        isr_df['in stock'] = isr_df['Inventory_Supply_at_FBA'] > 2
        self.stats['isr_sku'] = isr_df.groupby(['sku','asin','marketplace']).agg({'in stock':'mean'}).reset_index() # replace with lambda x: sum(x)/n_days}) later
        self.stats['isr_total'] = isr_df.groupby(['marketplace']).agg({'in stock':'mean'}).reset_index() # replace with lambda x: sum(x)/n_days}) later

    def _calculate_advertised_product(self):
        self.advertised_product_df['date'] = pd.to_datetime(self.advertised_product_df['date']).dt.date
        advertised_product = self.advertised_product_df[(self.advertised_product_df['date'] >= self.start) & (self.advertised_product_df['date'] <= self.end)]
        self.stats['advertised_product_detailed'] = advertised_product
        self.stats['advertised_product_breakdown'] = advertised_product.groupby(['country_code','sku','asin'])[
           ['clicks', 'impressions', 'spend', 'sameSkuUnits','sameSkuSales']].agg('sum').reset_index().sort_values(['country_code','asin','sku'], ascending = False)        
        self.stats['advertised_product_total'] = advertised_product.groupby('country_code')[
            ['clicks', 'impressions', 'spend', 'sameSkuUnits','sameSkuSales']].agg('sum').reset_index()

    def _calculate_purchased_product(self):
        self.purchased_product_df['date'] = pd.to_datetime(self.purchased_product_df['date']).dt.date
        purchased_product = self.purchased_product_df[(self.purchased_product_df['date'] >= self.start) & (self.purchased_product_df['date'] <= self.end)]
        self.combined_dfs['purchased_product'] = purchased_product
        self.stats['purchased_product_detailed'] = purchased_product.groupby(['country_code','date','sku','asin','purchasedAsin'])[
            ['otherSkuUnits','otherSkuSales']].agg('sum').reset_index()
        self.stats['purchased_product_breakdown'] = self.stats['purchased_product_detailed'].groupby(['country_code','purchasedAsin'])[
           ['otherSkuUnits','otherSkuSales']].agg('sum').reset_index().sort_values(['country_code','otherSkuSales'], ascending = False)
        self.stats['purchased_product_total'] = self.stats['purchased_product_detailed'].groupby('country_code')[
           ['otherSkuUnits','otherSkuSales']].agg('sum').reset_index().sort_values('otherSkuSales', ascending = False)

    def _calculate_promotions(self):
        self.promotions_df['pacific_date'] = pd.to_datetime(self.promotions_df['pacific_date']).dt.date
        promotions = self.promotions_df[(self.promotions_df['pacific_date'] >= self.start) & (self.promotions_df['pacific_date'] <= self.end)]
        self.combined_dfs['promotions'] = promotions
        self.stats['promotions_detaied'] = promotions.groupby(['sales_channel','sku','description'])[['item_promotion_discount','units_sold','sales']].agg('sum').reset_index()
        self.stats['promotions_sku'] = promotions.groupby(['sales_channel','sku'])[['item_promotion_discount','units_sold','sales']].agg('sum').reset_index()
        self.stats['promotions_total'] = promotions.groupby(['sales_channel'])[['item_promotion_discount','units_sold','sales']].agg('sum').reset_index()

    def _calculate_returns(self):
        self.returns_df['return_date'] = pd.to_datetime(self.returns_df['return_date']).dt.date
        returns = self.returns_df[(self.returns_df['return_date'] >= self.start) & (self.returns_df['return_date'] <= self.end)]
        self.combined_dfs['returns'] = returns
        self.stats['returns_detaied'] = returns.groupby(['country_code','sku','status','detailed_disposition']).agg({'quantity':'sum'}).reset_index()
        self.stats['returns_sku'] = returns.groupby(['country_code','sku']).agg({'quantity':'sum'}).reset_index()
        self.stats['returns_total'] = returns.groupby(['country_code']).agg({'quantity':'sum'}).reset_index()

    def _calculate_fees_dimensions(self): #TODO conditionally
        pass

    def _calculate_warehouse(self):
        self.warehouse_df['date'] = pd.to_datetime(self.warehouse_df['date']).dt.date
        warehouse = self.warehouse_df[(self.warehouse_df['date'] >= self.start) & (self.warehouse_df['date'] <= self.end)]
        self.stats['warehouse_total'] = warehouse[['total_wh','QtyPhysical','total_receiving']].sum().to_frame().T
    
    def _calculate_changelog(self):
        self.changelog_df['date'] = pd.to_datetime(self.changelog_df['date']).dt.date
        changes = self.changelog_df[(self.changelog_df['date'] >= self.start) & (self.changelog_df['date'] <= self.end)]
        changes['change_type'] = changes['change_type'].replace('Other, please specify in notes', nan)

        changes['description'] = changes.apply(
            lambda row: f"{row['change_type']} : {row['notes']}" if pd.notnull(row['change_type']) and pd.notnull(row['notes'])\
                else row['change_type'] if pd.notnull(row['change_type'])\
                    else row['notes'],
            axis=1)

        self.stats['changes_daily'] = changes[['country_code','date','description']].drop_duplicates().copy()
        self.stats['changes_total'] = changes[['country_code','description']].value_counts().reset_index()

    def _calculate_incoming(self):
        self.stats['incoming'] = self.incoming_df.pivot_table(values='QtyOrdered', index='sku', columns='year-week', aggfunc='sum', margins=True, margins_name='total').reset_index()


    ### async section ###
    async def _calculate_data(self, pull_function):
        await asyncio.to_thread(pull_function)

    async def calculate(self):
        """ function that pulls all data for a product from the provided Product.dataset """
        tasks = [
            self._calculate_data(self._calculate_br_asin),
            self._calculate_data(self._calculate_br),
            self._calculate_data(self._calculate_orders),
            self._calculate_data(self._calculate_inventory),
            self._calculate_data(self._calculate_inventory_history),
            self._calculate_data(self._calculate_advertised_product),
            self._calculate_data(self._calculate_purchased_product),
            self._calculate_data(self._calculate_promotions),
            self._calculate_data(self._calculate_returns),
            self._calculate_data(self._calculate_fees_dimensions),
            self._calculate_data(self._calculate_warehouse),
            self._calculate_data(self._calculate_changelog),
            self._calculate_data(self._calculate_incoming),

            
                ]
        await asyncio.gather(*tasks)


    def calculate_loop(self):
        self._calculate_orders()
        self._calculate_br()
        self._calculate_br_asin()
        self._calculate_inventory()
        self._calculate_inventory_history()
        self._calculate_advertised_product()
        self._calculate_purchased_product()
        self._calculate_promotions(),
        self._calculate_returns(),
        self._calculate_fees_dimensions(),
        self._calculate_warehouse(),
        self._calculate_changelog(),
        self._calculate_incoming()

    def save_to_file(self):
        file_path = os.path.join(user_folder, 'product.pkl')
        with open(file_path, 'wb') as pkl:
            pickle.dump(self, pkl)

    @error_checker
    def export(self):
        file_path_stats = os.path.join(user_folder, 'product_stats.xlsx')
        file_path_details = os.path.join(user_folder, 'product_details.xlsx')
        file_summary_path = os.path.join(user_folder, 'product_summary.xlsx')
        with pd.ExcelWriter(file_path_stats, engine='xlsxwriter') as writer:
            for key, df in self.stats.items():
                df.to_excel(writer, sheet_name=key, index=False)
                mm.format_header(df, writer, key)

            for key, df in {'fees':self.fees_dimensions_df, 'prices':self.pricing_df, 'cogs':self.cogs_df}.items():
                df.to_excel(writer, sheet_name=key, index = False)
                mm.format_header(df, writer, key)

        with pd.ExcelWriter(file_path_details, engine='xlsxwriter') as writer:
            for key, df in self.combined_dfs.items():
                df.to_excel(writer, sheet_name=key, index=False)
                mm.format_header(df, writer, key)
        
        if 'sales_summary' not in self.__dict__:
            self.summarize()
        with pd.ExcelWriter(file_summary_path, engine='xlsxwriter') as writer:
            self.sales_summary.to_excel(writer, sheet_name='sales_summary', index=False)
            mm.format_header(self.sales_summary, writer, 'sales_summary')
        mm.open_file_folder(os.path.dirname(file_path_stats))

    # summary and restock section
    def map_collections(self):
        pass
    
    @error_checker
    def summarize(self):
        #storage, fba fee, referral fee, ad spend, cogs
        #calculate units ordered daily and referral fee
        sales = self.orders_df.groupby(['pacific_date','sku','asin','sales_channel'])[
            ['units_sold', 'sales', 'promo_discount']
            ].agg('sum').reset_index()
        sales = sales.rename(columns = {'pacific_date':'date'})
        sales['date'] = pd.to_datetime(sales['date'])
        sales = sales[sales['date'].dt.date.between(self.start, self.end)]
        sales['net_sales'] = sales['sales'] - sales['promo_discount']
        sales['referral_fee'] = sales['net_sales'] * 0.15
        
        sales = self.__attach_marketplace__(sales, 'sales_channel')
        sales = self.__attach_collection__(sales)
        sales = sales.groupby(
            ['date','sku','marketplace','collection', 'sub-collection', 'size','color']
            )[['units_sold', 'sales','net_sales','referral_fee','promo_discount']].agg('sum').reset_index()
       
        #storage fees
        storage = self.inventory_history_df.groupby(
            ['date', 'sku','asin','marketplace']
            ).agg('sum').reset_index()
        storage = self.__attach_collection__(storage)
        storage['date'] = pd.to_datetime(storage['date'])
        storage = storage[storage['date'].dt.date.between(self.start, self.end)]
        del storage['asin']
        storage = storage.groupby(
            ['date', 'sku','collection', 'sub-collection', 'size',
            'color','marketplace']
            ).agg('sum').reset_index()
        storage['storage'] = storage['estimated_storage_cost_next_month'] / 30
        storage['excess_storage'] = storage[
            ['estimated_ais_181_210_days',
            'estimated_ais_211_240_days', 'estimated_ais_241_270_days',
            'estimated_ais_271_300_days', 'estimated_ais_301_330_days',
            'estimated_ais_331_365_days', 'estimated_ais_365_plus_days']
            ].sum(axis=1) / 30
        storage = storage[
            ['date', 'sku','collection', 'sub-collection', 'size',
            'color', 'marketplace', 'storage','excess_storage']
            ]
        
        cogs = self.cogs_df.copy()
        cogs['date'] = pd.to_datetime(cogs['date'])
        cogs = cogs[cogs['date'].dt.date.between(self.start-pd.Timedelta(days=30), self.end)]

        cogs['year-month'] = cogs['date'].dt.year.astype('str') + '-' + cogs['date'].dt.month.astype('str')
        
        sales['year-month'] = sales['date'].dt.year.astype('str') + '-' + sales['date'].dt.month.astype('str')
        cogs = self.__attach_marketplace__(cogs, 'channel')
        cogs = self.__attach_collection__(cogs)

        sales = pd.merge(
            sales,
            cogs[
                ['year-month', 'marketplace', 'collection', 'sub-collection',
                 'size','color','product_cost']
                ],
            how='left',
            on=['year-month','marketplace','collection', 'sub-collection', 'size','color']
            )
        sales['product_cost'] = sales['product_cost'] * sales['units_sold']
        
        fees = self.fees_dimensions_df[['sku','fba_fee','sales_channel']].copy()
        fees = self.__attach_marketplace__(fees, 'sales_channel')
        fees = self.__attach_collection__(fees)
        
        sales = pd.merge(
            sales,
            fees[['fba_fee', 'marketplace', 'collection',
                   'sub-collection', 'size', 'color']],
            how='left',
            on=['marketplace', 'collection','sub-collection', 'size', 'color']
            )
        
        sales['fba_fee'] = sales['fba_fee'] * sales['units_sold']
        
        ad_spend = self.advertised_product_df.groupby(
            ['date','sku','asin','country_code']
            )['spend'].agg('sum').reset_index()
        ad_spend['date'] = pd.to_datetime(ad_spend['date'])
        ad_spend = ad_spend[ad_spend['date'].dt.date.between(self.start, self.end)]
        ad_spend = ad_spend.rename(columns={'country_code':'marketplace'})
        ad_spend = self.__attach_collection__(ad_spend)
        del ad_spend['asin']
        ad_spend = ad_spend.groupby(['date', 'sku', 'marketplace', 'spend', 'collection',
               'sub-collection', 'size', 'color']).agg('sum').reset_index()
        
        sales = pd.merge(
            sales,
            ad_spend[['date','sku','collection','sub-collection', 'size',
            'color','spend','marketplace']],
            how='outer',
            on=['date','marketplace','sku','collection','sub-collection', 'size',
            'color']
            )

        sales = pd.merge(
            sales,
            storage,
            how='outer', on=['date', 'sku','collection', 'sub-collection', 'size',
            'color','marketplace'])
        
        sales = sales.fillna(0)
        sales['marketplace'] = sales['marketplace'].replace('UK','GB')
        
        sales['profit w/o overhead'] = sales['net_sales']-sales['referral_fee']-sales['fba_fee']-sales['product_cost']-sales['spend']-sales['storage']
        self.sales_summary = sales.copy()
    
# dataset = Dataset(
#     start="2025-01-01", end="2025-12-31",
#     market=["US", "CA", "GB", "UK","MX", "FR", "DE", "IT", "ES"],
#     local_data=True,save=False)
# dataset.query_sync()

# skus = dataset.dictionary[dataset.dictionary['collection']=='Microfiber Pillow Covers w/zipper']['sku'].unique().tolist()

# product = Product(dataset=dataset, sku=skus, start="2025-02-28", end="2025-12-31")
# product.populate_loop()    
    
# self = product
