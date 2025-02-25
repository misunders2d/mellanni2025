
from utils.dataset import Dataset
import pandas as pd
import asyncio

class Product:
    dataset = None
    def __init__(self,
                 asin=None,
                 sku=None,
                 dataset:Dataset=None):
        if not (asin or sku):
            raise ValueError("Either 'asin' or 'sku' must be provided.")
        if asin and sku:
            raise ValueError("Only one of 'asin' or 'sku' must be provided.")
        if not dataset:
            raise ValueError("A dataset must be provided.")

        self.asins = set()
        self.skus = set()
        self.collections = set()
        self.sub_collections = set()
        self.sizes = set()
        self.colors = set()
        Product.dataset = dataset
        self.stats = {}
        if asin:
            self.asins.add(asin)
        if sku:
            self.skus.add(sku)
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
        Product.dataset.orders = Product.dataset.orders[~Product.dataset.orders.index.isin(self.orders_df.index)]
        self._update_ids(self.orders_df)

    def _pull_br(self):
        self.br_df = Product.dataset.br[(Product.dataset.br['sku'].isin(self.skus)) | (Product.dataset.br['asin'].isin(self.asins))]
        Product.dataset.br = Product.dataset.br[~Product.dataset.br.index.isin(self.br_df.index)]
        self._update_ids(self.br_df)

    def _pull_br_asin(self):
        self.br_asin_df = Product.dataset.br_asin[Product.dataset.br_asin['asin'].isin(self.asins)]
        Product.dataset.br_asin = Product.dataset.br_asin[~Product.dataset.br_asin.index.isin(self.br_asin_df.index)]
        self._update_ids(self.br_asin_df)

    def _pull_inventory(self):
        self.inventory_df = Product.dataset.inventory[(Product.dataset.inventory['sku'].isin(self.skus)) | (Product.dataset.inventory['asin'].isin(self.asins))]
        Product.dataset.inventory = Product.dataset.inventory[~Product.dataset.inventory.index.isin(self.inventory_df.index)]
        self._update_ids(self.inventory_df)

    def _pull_inventory_history(self):
        self.inventory_history_df = Product.dataset.inventory_history[(Product.dataset.inventory_history['sku'].isin(self.skus)) | (Product.dataset.inventory_history['asin'].isin(self.asins))]
        Product.dataset.inventory_history = Product.dataset.inventory_history[~Product.dataset.inventory_history.index.isin(self.inventory_history_df.index)]
        self._update_ids(self.inventory_history_df)

    def _pull_advertised_product(self):
        self.advertised_product_df = Product.dataset.advertised_product[(Product.dataset.advertised_product['sku'].isin(self.skus)) | (Product.dataset.advertised_product['asin'].isin(self.asins))]
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
        Product.dataset.promotions = Product.dataset.promotions[~Product.dataset.promotions.index.isin(self.promotions_df.index)]
        self._update_ids(self.promotions_df)

    def _pull_returns(self):
        self.returns_df = Product.dataset.returns[(Product.dataset.returns['sku'].isin(self.skus)) | (Product.dataset.returns['asin'].isin(self.asins))]
        Product.dataset.returns = Product.dataset.returns[~Product.dataset.returns.index.isin(self.returns_df.index)]
        self._update_ids(self.returns_df)

    def _pull_fees_dimensions(self):
        self.fees_dimensions_df = Product.dataset.fees[(Product.dataset.fees['sku'].isin(self.skus)) | (Product.dataset.fees['asin'].isin(self.asins))]
        Product.dataset.fees = Product.dataset.fees[~Product.dataset.fees.index.isin(self.fees_dimensions_df.index)]
        self._update_ids(self.fees_dimensions_df)

    def _pull_warehouse(self):
        self.warehouse_df = Product.dataset.warehouse[Product.dataset.warehouse['sku'].isin(self.skus)]
        Product.dataset.warehouse = Product.dataset.warehouse[~Product.dataset.warehouse.index.isin(self.warehouse_df.index)]
        self._update_ids(self.warehouse_df)
    
    def _pull_changelog(self):
        self.changelog_df = Product.dataset.changelog[(Product.dataset.changelog['sku'].isin(self.skus)) | (Product.dataset.changelog['asin'].isin(self.asins))]
        Product.dataset.changelog = Product.dataset.changelog[~Product.dataset.changelog.index.isin(self.changelog_df.index)]
        self._update_ids(self.changelog_df)
    
    def _pull_incoming(self):
        self.incoming_df = Product.dataset.incoming[Product.dataset.incoming['sku'].isin(self.skus)]
        Product.dataset.incoming = Product.dataset.incoming[~Product.dataset.incoming.index.isin(self.incoming_df.index)]
        self._update_ids(self.incoming_df)

    def _pull_pricing(self):
        self.pricing_df = Product.dataset.pricing[Product.dataset.pricing['sku'].isin(self.skus)]
        Product.dataset.pricing = Product.dataset.pricing[~Product.dataset.pricing.index.isin(self.pricing_df.index)]
        self._update_ids(self.pricing_df)

    def _pull_cogs(self):
        self.cogs_df = Product.dataset.cogs[Product.dataset.cogs['sku'].isin(self.skus)]
        Product.dataset.cogs = Product.dataset.cogs[~Product.dataset.cogs.index.isin(self.cogs_df.index)]
        self._update_ids(self.cogs_df)

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
    def _calculate_br_asin(self, start, end):
        start, end = pd.to_datetime(start).date(), pd.to_datetime(end).date()
        self.br_asin_df['date'] = pd.to_datetime(self.br_asin_df['date']).dt.date
        self.br_asin = self.br_asin_df[
            ['date', 'asin', 'browserSessions','browserSessionsB2B', 'mobileAppSessions',
             'mobileAppSessionsB2B','sessions', 'sessionsB2B', 'browserPageViews', 'country_code',
             'browserPageViewsB2B','mobileAppPageViews', 'mobileAppPageViewsB2B', 'pageViews','pageViewsB2B']
             ].copy()

        self.br_asin = self.br_asin[(self.br_asin['date'] >= start) & (self.br_asin['date'] <= end)]
        self.br_asin = self.br_asin.groupby(['country_code','date', 'asin']).agg('sum').reset_index()
        sum_cols = self.br_asin.columns[3:]
        agg_dict = {col:'sum' for col in sum_cols}
        agg_dict['date'] = lambda x: [len(x),min(x), max(x)]
        br_asin_sum = self.br_asin.groupby(['country_code', 'asin']).agg(agg_dict)
        br_asin_sum[['# days','min_date','max_date']] = br_asin_sum['date'].tolist()
        del br_asin_sum['date']
        self.stats['br_asin'] = br_asin_sum

    def _calculate_br(self, start, end):
        start, end = pd.to_datetime(start).date(), pd.to_datetime(end).date()
        self.br_df['date'] = pd.to_datetime(self.br_df['date']).dt.date
        self.br = self.br_df[
            ['date', 'sku', 'asin', 'unitsOrdered', 'unitsOrderedB2B',
             'orderedProductSales', 'orderedProductSalesB2B','country_code']
            ].copy()

        self.br = self.br[(self.br['date'] >= start) & (self.br['date'] <= end)]
        self.br = self.br.groupby(['country_code','date', 'sku', 'asin']).agg('sum').reset_index()
        
        br_sum = self.br.groupby(['country_code', 'sku', 'asin']).agg(
            {'unitsOrdered':'sum','unitsOrderedB2B':'sum',
             'orderedProductSales':'sum', 'orderedProductSalesB2B':'sum',
             'date':lambda x: [len(x),min(x), max(x)]}).reset_index()
        br_sum[['# days','min_date','max_date']] = br_sum['date'].tolist()
        del br_sum['date']
        self.stats['br'] = br_sum

    def _calculate_orders(self, start, end):
        start, end = pd.to_datetime(start).date(), pd.to_datetime(end).date()
        self.orders_df['pacific_date'] = pd.to_datetime(self.orders_df['pacific_date']).dt.date
        self.orders = self.orders_df[(self.orders_df['pacific_date'] >= start) & (self.orders_df['pacific_date'] <= end)]
        self.orders = self.orders.groupby(['pacific_date','sku','sales_channel']).agg({'units_sold':'sum', 'sales':'sum'}).reset_index()
        orders_sum = self.orders.groupby(['sales_channel','sku']).agg(
            {'units_sold':'sum', 'sales':'sum', 'pacific_date':lambda x: [len(x),min(x), max(x)]}).reset_index()
        orders_sum[['# days','min_date','max_date']] = orders_sum['pacific_date'].tolist()
        del orders_sum['pacific_date']
        self.stats['orders'] = orders_sum
        
    def _calculate_inventory(self, start, end):
        start, end = pd.to_datetime(start).date(), pd.to_datetime(end).date()
        self.inventory_df['date'] = pd.to_datetime(self.inventory_df['date']).dt.date
        self.inventory = self.inventory_df[(self.inventory_df['date'] >= start) & (self.inventory_df['date'] <= end)]
        self.stats['inventory'] = self.inventory

    def _calculate_inventory_history(self, start, end):
        start, end = pd.to_datetime(start).date(), pd.to_datetime(end).date()
        self.inventory_history_df['date'] = pd.to_datetime(self.inventory_history_df['date']).dt.date
        self.inventory_history = self.inventory_history_df[(self.inventory_history_df['date'] >= start) & (self.inventory_history_df['date'] <= end)]
        self.inventory_history = self.inventory_history.groupby(
            ['date','sku','asin','marketplace']).agg('sum').reset_index()
        self.stats['inventory'] = self.inventory_history

    def calculate_loop(self, start, end):
        self._calculate_orders(start, end)
        self._calculate_br(start, end)
        self._calculate_br_asin(start, end)
        self._calculate_inventory(start, end)
        self._calculate_inventory_history(start, end)
