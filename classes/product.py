
from classes.dataset import Dataset
import pandas as pd
from utils import mellanni_modules as mm
from utils.decorators import error_checker

from common import user_folder, event_dates
event_dates_complete = [date for daterange in event_dates.values() for date in daterange]

import os, pickle, re
import asyncio
from numpy import nan, ceil, floor

import warnings
warnings.filterwarnings("ignore")

class Product:
    dataset = None
    def __init__(self,
                 dataset:Dataset,
                 asin:str|list="",
                 sku:str|list="",
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
        
        self.individual_products = {}
        
        self.dataset = dataset
        self.dataset_delete = False # delete the lines for this specific set of asins from self.dataset
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
        dictionary = self.dataset.dictionary
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
        self.__update_product__(dictionary)
    
    def _update_ids(self, df):
        if 'sku' in df.columns:
            self.skus.update(df['sku'].unique())
        if 'asin' in df.columns:
            self.asins.update(df['asin'].unique())

    def __update_product__(self, df):
        if not all([x in df.columns.tolist() for x in ('collection','sub-collection','size','color','marketplace')]):
            try:
                df = self.__attach_collection__(df)
            except Exception as e:
                print(f'Error while updating individual product: {e}')
        if 'sku' in df.columns:
            sku_df = df.groupby(['collection','sub-collection','size','color','marketplace']).agg({'sku':lambda x: x.unique().tolist()}).reset_index()
            for i, row in sku_df.iterrows():
                individual_product = (
                    row['collection'],row['sub-collection'],row['size'],row['color'],row['marketplace']
                    )
                if individual_product in self.individual_products:
                    self.individual_products[individual_product]['sku'].update(set(row['sku']))
                else:
                    self.individual_products[individual_product] = {'sku':set(), 'asin':set()}
                    self.individual_products[individual_product]['sku'] = set(row['sku'])
                    
        if 'asin' in df.columns:
            asin_df = df.groupby(['collection','sub-collection','size','color','marketplace']).agg({'asin':lambda x: x.unique().tolist()}).reset_index()
            for i, row in asin_df.iterrows():
                individual_product = (
                    row['collection'],row['sub-collection'],row['size'],row['color'],row['marketplace']
                    )
                if individual_product in self.individual_products:
                    self.individual_products[individual_product]['asin'].update(set(row['asin']))
                else:
                    self.individual_products[individual_product] = {'sku':set(),'asin':set()}
                    self.individual_products[individual_product]['asin'] = set(row['asin'])

    def __attach_ids__(self, df):
        if not all([x in df.columns.tolist() for x in ('collection','sub-collection','size','color','marketplace')]):
            raise BaseException("wrong file submitted")
        all_rows = []
        for i, row in df.iterrows():
            product = tuple(row[['collection','sub-collection','size','color','marketplace']].values.tolist())
            row['asin'] = '\n'.join([x for x in self.individual_products[product]['asin']])
            row['sku'] = '\n'.join([x for x in self.individual_products[product]['sku']])
            all_rows.append(row)
        new_df = pd.DataFrame(data=all_rows)
        return new_df
            

    def __attach_collection__(self, df):
        product_dict = self.dataset.dictionary[
            (self.dataset.dictionary['sku'].isin(self.skus))
            |
            (self.dataset.dictionary['asin'].isin(self.asins))
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
            lambda x: [key for key,value in self.dataset.channels_mapping.items() if x.lower()==value.lower()][0]
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
        self.orders_df = self.dataset.orders[(self.dataset.orders['sku'].isin(self.skus)) | (self.dataset.orders['asin'].isin(self.asins))]
        if self.dataset_delete:
            self.dataset.orders = self.dataset.orders[~self.dataset.orders.index.isin(self.orders_df.index)]
        self._update_ids(self.orders_df)

    def _pull_br(self):
        self.br_df = self.dataset.br[(self.dataset.br['sku'].isin(self.skus)) | (self.dataset.br['asin'].isin(self.asins))]
        if self.dataset_delete:
            self.dataset.br = self.dataset.br[~self.dataset.br.index.isin(self.br_df.index)]
        self._update_ids(self.br_df)

    def _pull_br_asin(self):
        self.br_asin_df = self.dataset.br_asin[self.dataset.br_asin['asin'].isin(self.asins)]
        if self.dataset_delete:
            self.dataset.br_asin = self.dataset.br_asin[~self.dataset.br_asin.index.isin(self.br_asin_df.index)]
        self._update_ids(self.br_asin_df)

    def _pull_inventory(self):
        self.inventory_df = self.dataset.inventory[(self.dataset.inventory['sku'].isin(self.skus)) | (self.dataset.inventory['asin'].isin(self.asins))]
        if self.dataset_delete:
            self.dataset.inventory = self.dataset.inventory[~self.dataset.inventory.index.isin(self.inventory_df.index)]
        self._update_ids(self.inventory_df)

    def _pull_inventory_history(self):
        self.inventory_history_df = self.dataset.inventory_history[(self.dataset.inventory_history['sku'].isin(self.skus)) | (self.dataset.inventory_history['asin'].isin(self.asins))]
        if self.dataset_delete:
            self.dataset.inventory_history = self.dataset.inventory_history[~self.dataset.inventory_history.index.isin(self.inventory_history_df.index)]
        self._update_ids(self.inventory_history_df)

    def _pull_advertised_product(self):
        self.advertised_product_df = self.dataset.advertised_product[(self.dataset.advertised_product['sku'].isin(self.skus)) | (self.dataset.advertised_product['asin'].isin(self.asins))]
        if self.dataset_delete:
            self.dataset.advertised_product = self.dataset.advertised_product[~self.dataset.advertised_product.index.isin(self.advertised_product_df.index)]
        self._update_ids(self.advertised_product_df)

    def _pull_purchased_product(self):
        self.purchased_product_df = self.dataset.purchased_product[
            (self.dataset.purchased_product['sku'].isin(self.skus)) |
            (self.dataset.purchased_product['asin'].isin(self.asins)) |
            (self.dataset.purchased_product['purchasedAsin'].isin(self.asins))
            ]

    def _pull_promotions(self):
        self.promotions_df = self.dataset.promotions[(self.dataset.promotions['sku'].isin(self.skus))]
        if self.dataset_delete:
            self.dataset.promotions = self.dataset.promotions[~self.dataset.promotions.index.isin(self.promotions_df.index)]
        self._update_ids(self.promotions_df)

    def _pull_returns(self):
        self.returns_df = self.dataset.returns[(self.dataset.returns['sku'].isin(self.skus)) | (self.dataset.returns['asin'].isin(self.asins))]
        if self.dataset_delete:
            self.dataset.returns = self.dataset.returns[~self.dataset.returns.index.isin(self.returns_df.index)]
        self._update_ids(self.returns_df)

    def _pull_fees_dimensions(self):
        self.fees_dimensions_df = self.dataset.fees[(self.dataset.fees['sku'].isin(self.skus)) | (self.dataset.fees['asin'].isin(self.asins))]
        if self.dataset_delete:
            self.dataset.fees = self.dataset.fees[~self.dataset.fees.index.isin(self.fees_dimensions_df.index)]
        self._update_ids(self.fees_dimensions_df)

    def _pull_warehouse(self):
        self.warehouse_df = self.dataset.warehouse[self.dataset.warehouse['sku'].isin(self.skus)]
        if self.dataset_delete:
            self.dataset.warehouse = self.dataset.warehouse[~self.dataset.warehouse.index.isin(self.warehouse_df.index)]
        self._update_ids(self.warehouse_df)
    
    def _pull_changelog(self):
        self.changelog_df = self.dataset.changelog[(self.dataset.changelog['sku'].isin(self.skus))]
        if self.dataset_delete:
            self.dataset.changelog = self.dataset.changelog[~self.dataset.changelog.index.isin(self.changelog_df.index)]
    
    def _pull_incoming(self):
        self.incoming_df = self.dataset.incoming[self.dataset.incoming['sku'].isin(self.skus)]
        if self.dataset_delete:
            self.dataset.incoming = self.dataset.incoming[~self.dataset.incoming.index.isin(self.incoming_df.index)]

    def _pull_pricing(self):
        self.pricing_df = self.dataset.pricing[self.dataset.pricing['sku'].isin(self.skus)]
        if self.dataset_delete:
            self.dataset.pricing = self.dataset.pricing[~self.dataset.pricing.index.isin(self.pricing_df.index)]

    def _pull_cogs(self):
        self.cogs_df = self.dataset.cogs[self.dataset.cogs['sku'].isin(self.skus)]
        if self.dataset_delete:
            self.dataset.cogs = self.dataset.cogs[~self.dataset.cogs.index.isin(self.cogs_df.index)]

    ### async section ###
    async def _pull_data(self, pull_function):
        await asyncio.to_thread(pull_function)

    async def populate(self):
        """ function that pulls all data for a product from the provided self.dataset """
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
        """ function that pulls all data for a product from the provided self.dataset """
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
        orders = orders.groupby(['pacific_date','sku','asin','sales_channel']).agg({'units_sold':'sum', 'sales':'sum'}).reset_index()
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
        n_days = (min(end, today) - start).days #will be used lated to calculate isr more precisely
        
        aggfunc = {'in stock':'mean'} #replace with lambda x: sum(x)/n_days
        isr_df['in stock'] = isr_df['available'] > 2 #replace with 'Inventory_Supply_at_FBA' to calculate total isr including inbound
        self.stats['isr_sku'] = isr_df.groupby(['sku','marketplace']).agg(aggfunc).reset_index() 
        self.stats['isr_sku_asin'] = isr_df.groupby(['sku','asin','marketplace']).agg(aggfunc).reset_index()
        self.stats['isr_total'] = isr_df.groupby(['marketplace']).agg(aggfunc).reset_index()

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
        """ function that pulls all data for a product from the provided self.dataset """
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
    def export(self, mode = 'stats'):
        file_path_stats = os.path.join(user_folder, 'product_stats.xlsx')
        file_path_details = os.path.join(user_folder, 'product_details.xlsx')
        file_summary_path = os.path.join(user_folder, 'product_summary.xlsx')
        if mode == 'stats':
            if self.stats:
                with pd.ExcelWriter(file_path_stats, engine='xlsxwriter') as writer:
                    for key, df in self.stats.items():
                        df.to_excel(writer, sheet_name=key, index=False)
                        mm.format_header(df, writer, key)
    
                    for key, df in {'fees':self.fees_dimensions_df, 'prices':self.pricing_df, 'cogs':self.cogs_df}.items():
                        df.to_excel(writer, sheet_name=key, index = False)
                        mm.format_header(df, writer, key)
    
            if self.combined_dfs:
                with pd.ExcelWriter(file_path_details, engine='xlsxwriter') as writer:
                    for key, df in self.combined_dfs.items():
                        df.to_excel(writer, sheet_name=key, index=False)
                        mm.format_header(df, writer, key)
                
            if 'sales_summary' in self.__dict__:
                with pd.ExcelWriter(file_summary_path, engine='xlsxwriter') as writer:
                    self.sales_summary.to_excel(writer, sheet_name='sales_summary', index=False)
                    mm.format_header(self.sales_summary, writer, 'sales_summary')
        
        elif mode == 'restock':
            if not 'restock_summary' in self.__dict__:
                self.restock()    
            
            mm.export_to_excel(
                [self.restock_summary, self.to_ship, self.wh_situation],
                ['restock', 'to ship', 'warehouse'],
                filename='restock.xlsx',
                out_folder=user_folder
                )
                
        mm.open_file_folder(os.path.dirname(file_path_stats))

    # summary and restock section
    def map_collections(self):
        pass
    
    @error_checker
    def summarize(self):
        #storage, fba fee, referral fee, ad spend, cogs
        def __summarize_sales__():
            sales = self.orders_df.groupby(['pacific_date','sku','asin','sales_channel'])[
                ['units_sold', 'sales', 'promo_discount']
                ].agg('sum').reset_index()
            sales = sales.rename(columns = {'pacific_date':'date'})
            sales['date'] = pd.to_datetime(sales['date'])
            sales = sales[sales['date'].dt.date.between(self.start, self.end)]
            sales['promo_discount'] = -sales['promo_discount']
            sales['referral_fee'] = -(sales['sales'] + sales['promo_discount']) * 0.15
            
            sales = self.__attach_marketplace__(sales, 'sales_channel')
            sales = self.__attach_collection__(sales)
            sales['marketplace'] = sales['marketplace'].replace('UK','GB')
            
            sales = sales.groupby(
                ['date','sku','marketplace','collection', 'sub-collection', 'size','color']
                )[['units_sold', 'sales','referral_fee','promo_discount']].agg('sum').reset_index()
            return sales
        
        def __summarize_storage_fees__():
            storage = self.inventory_history_df.groupby(
                ['date', 'sku','asin','marketplace']
                ).agg('sum').reset_index()
            storage['marketplace'] = storage['marketplace'].replace('UK','GB')
            storage = self.__attach_collection__(storage)
            storage['date'] = pd.to_datetime(storage['date'])
            storage = storage[storage['date'].dt.date.between(self.start, self.end)]
            del storage['asin']
    
            storage = storage.groupby(
                ['date', 'sku','collection', 'sub-collection', 'size',
                'color','marketplace']
                ).agg('sum').reset_index()
            storage['storage'] = -storage['estimated_storage_cost_next_month'] / 30
            storage['excess_storage'] = -storage[
                ['estimated_ais_181_210_days',
                'estimated_ais_211_240_days', 'estimated_ais_241_270_days',
                'estimated_ais_271_300_days', 'estimated_ais_301_330_days',
                'estimated_ais_331_365_days', 'estimated_ais_365_plus_days']
                ].sum(axis=1) / 30
            storage = storage[
                ['date', 'sku','collection', 'sub-collection', 'size',
                'color', 'marketplace', 'storage','excess_storage','Inventory_Supply_at_FBA','available']
                ]
            return storage
        
        def __summarize_cogs__():
            cogs = self.cogs_df.copy()
            cogs['date'] = pd.to_datetime(cogs['date'])
            cogs = cogs[cogs['date'].dt.date.between(self.start-pd.Timedelta(days=30), self.end)]
    
            cogs['year-month'] = cogs['date'].dt.year.astype('str') + '-' + cogs['date'].dt.month.astype('str')
            
            cogs['product_cost'] = -cogs['product_cost']
            cogs = self.__attach_marketplace__(cogs, 'channel')
            cogs = self.__attach_collection__(cogs)
    
            cogs['marketplace'] = cogs['marketplace'].replace('UK','GB')
    
            cogs = cogs.groupby(['sku', 'year-month', 'marketplace', 'collection', 'sub-collection', 'size',
                   'color'])[['product_cost', 'product_cost_local']].agg('max').reset_index()
            return cogs

        def __summarize_fees__():
            fees = self.fees_dimensions_df[['sku','fba_fee','sales_channel']].copy()
            fees = self.__attach_marketplace__(fees, 'sales_channel')
            fees = self.__attach_collection__(fees)
            fees['marketplace'] = fees['marketplace'].replace('UK','GB')
            
            fees = fees.groupby(
                ['marketplace', 'collection','sub-collection', 'size', 'color']
                )['fba_fee'].agg('max').reset_index()
            return fees

        def __summarize_ad_spend__():        
            ad_spend = self.advertised_product_df.groupby(
                ['date','sku','asin','country_code']
                )[['spend','sameSkuUnits','sameSkuSales','clicks', 'impressions']].agg('sum').reset_index()
            ad_spend = ad_spend.rename(columns = {
                'sameSkuUnits':'unitsSoldOwnPPC',
                'sameSkuSales':'salesOwnPPC',
                'country_code':'marketplace'
                })
            ad_spend['date'] = pd.to_datetime(ad_spend['date'])
            ad_spend = ad_spend[ad_spend['date'].dt.date.between(self.start, self.end)]
            ad_spend = self.__attach_collection__(ad_spend)
            del ad_spend['asin']
            ad_spend['marketplace'] = ad_spend['marketplace'].replace('UK','GB')
            ad_spend = ad_spend.groupby(['date', 'sku', 'marketplace', 'collection',
                   'sub-collection', 'size', 'color']).agg('sum').reset_index()
            ad_spend['spend'] = -ad_spend['spend']
            return ad_spend

        def __summarize_purchased_product__():
            purchased = self.purchased_product_df.groupby(
                ['date', 'purchasedAsin','country_code']
                )[['otherSkuUnits','otherSkuSales']].agg('sum').reset_index()
            purchased = purchased.rename(columns={
                'otherSkuUnits':'unitsSoldOtherPPC',
                'otherSkuSales':'salesOtherPPC',
                'purchasedAsin':'asin',
                'country_code':'marketplace'
                })
            purchased['date'] = pd.to_datetime(purchased['date'])
            purchased = purchased[purchased['date'].dt.date.between(self.start, self.end)]
            purchased = self.__attach_collection__(purchased)
            purchased['marketplace'] = purchased['marketplace'].replace('UK','GB')
            purchased = purchased.groupby(['date','marketplace', 'collection',
                   'sub-collection', 'size', 'color']).agg('sum').reset_index()
            return purchased
        
        def __summarize_br__():
            br = self.br_asin_df.groupby(
                ['date', 'asin', 'country_code']
                )[
                    ['browserSessions','browserSessionsB2B', 'mobileAppSessions',
                     'mobileAppSessionsB2B','sessions', 'sessionsB2B',
                     'browserPageViews', 'browserPageViewsB2B',
                     'mobileAppPageViews','mobileAppPageViewsB2B',
                     'pageViews','pageViewsB2B']
                    ].agg('sum').reset_index()
            br = br.rename(columns={'country_code':'marketplace'})
            br['marketplace'] = br['marketplace'].replace('UK','GB')
            br['date'] = pd.to_datetime(br['date'])
            br = br[br['date'].dt.date.between(self.start, self.end)]
            
            br = self.__attach_collection__(br)
            br = br.groupby(
                ['date','marketplace','collection','sub-collection','size','color']
                ).agg('sum').reset_index()
            return br

        def __summarize_changelog__():
            changes = self.changelog_df.copy()
            changes = changes.rename(columns={'country_code':'marketplace'})
            changes['marketplace'] = changes['marketplace'].replace('UK','GB')
            changes['date'] = pd.to_datetime(changes['date'])
            changes = changes[changes['date'].dt.date.between(self.start, self.end)]
            
            changes['change_type'] = changes['change_type'].replace('Other, please specify in notes', nan)
    
            changes['changes'] = changes.apply(
                lambda row: f"{row['change_type']} : {row['notes']}" if pd.notnull(row['change_type']) and pd.notnull(row['notes'])\
                    else row['change_type'] if pd.notnull(row['change_type'])\
                        else row['notes'],
                axis=1)
            changes = self.__attach_collection__(changes)
            changes['changes'] = changes['changes'].astype('str').replace('nan','')
            changes = changes.groupby(
                ['date','marketplace','collection','sub-collection','size','color']
                )['changes'].agg(lambda x: ' | '.join(x.unique().tolist())).reset_index()
            return changes
        
        def __summarize_non_product_ads__():
            if not isinstance(self.dataset, Dataset):
                raise BaseException("SBA not defined)")
            sba = self.dataset.sba[['date','cost','unitsSold14d','attributedSales14d', 'country_code']].groupby(
                ['date','country_code']
                ).agg('sum').reset_index()
            
            sba = sba.rename(
                columns={
                    'country_code':'marketplace',
                    'cost':'sba_spend',
                    'unitsSold14d':'sba_unitsSold',
                    'attributedSales14d':'sba_Sales'}
                )
            sba['marketplace'] = sba['marketplace'].replace('UK','GB')
            sba['date'] = pd.to_datetime(sba['date'])
            sba = sba[sba['date'].dt.date.between(self.start, self.end)]
            
            
            dsp = self.dataset.dsp[
                ['date', 'total_cost', 'totalUnitsSold','totalSales']
                ].groupby('date').agg('sum').reset_index()
            dsp['date'] = pd.to_datetime(dsp['date'])
            dsp = dsp[dsp['date'].dt.date.between(self.start, self.end)]

            dsp = dsp.rename(
                columns={
                    'total_cost':'dsp_spend',
                    'totalUnitsSold':'dsp_unitsSold',
                    'totalSales':'dsp_Sales'}
                )
            dsp['marketplace'] = 'US'
            
            result = pd.merge(sba, dsp, how = 'outer', on = ['date','marketplace'])
            
            result[['sba_spend','dsp_spend']] = -result[['sba_spend','dsp_spend']]
            result['collection'] = '[sb and dsp campaigns]'
            return result

        def __summarize_returns__():
            orders = self.orders_df[['pacific_date', 'amazon_order_id','sales_channel']].copy()
            orders = orders.rename(columns={'pacific_date':'date'})
            orders['date'] = pd.to_datetime(orders['date'])
            orders = orders[orders['date'].dt.date.between(self.start, self.end)]
            orders = orders.groupby('amazon_order_id').agg('min').reset_index()
            
            returns = self.returns_df[['order_id', 'sku', 'asin', 'quantity']].copy()
            returns = returns.rename(columns={'order_id':'amazon_order_id','quantity':'returned_units'})
            result = pd.merge(returns, orders, how='inner', on='amazon_order_id')
            result = self.__attach_marketplace__(result, 'sales_channel')
            result = self.__attach_collection__(result)
            result = result.groupby(
                ['date', 'marketplace', 'collection', 'sub-collection', 'size', 'color']
                )['returned_units'].agg('sum').reset_index()
            return result
            
        def __merge_summary__(
                sales, cogs, fees, ad_spend, purchased, storage,
                br, changes, sba, returns
                ):
            sales['year-month'] = sales['date'].dt.year.astype('str') + '-' + sales['date'].dt.month.astype('str')
            
            sales['cogs_market'] = sales['marketplace']\
                    .replace('FR','EU')\
                    .replace('DE','EU')\
                    .replace('ES','EU')\
                    .replace('IT','EU')
            cogs = cogs.rename(columns={'marketplace':'cogs_market'})
            sales = pd.merge(
                sales,
                cogs[
                    ['year-month', 'sku','cogs_market', 'collection', 'sub-collection',
                      'size','color','product_cost']
                    ],
                how='left',
                on=['year-month','sku','cogs_market','collection', 'sub-collection', 'size','color'],
                validate="many_to_one"
                )
            sales['product_cost'] = sales['product_cost'] * sales['units_sold']
            del sales['cogs_market']
            
            
            sales = pd.merge(
                sales,
                fees[['fba_fee', 'marketplace', 'collection',
                        'sub-collection', 'size', 'color']],
                how='left',
                on=['marketplace', 'collection','sub-collection', 'size', 'color']
                )
            
            sales['fba_fee'] = -sales['fba_fee'] * sales['units_sold']
            
            sales = pd.merge(
                sales,
                ad_spend[['date','sku','collection','sub-collection', 'size',
                'color','spend','marketplace','unitsSoldOwnPPC','salesOwnPPC',
                'clicks','impressions']],
                how='outer',
                on=['date','sku','marketplace','collection','sub-collection', 'size',
                'color'],
                validate="one_to_one"
                )

            sales = pd.merge(
                sales,
                storage,
                how='outer', on=['date', 'sku','collection', 'sub-collection', 'size',
                'color','marketplace'])
            sales['marketplace'] = sales['marketplace'].replace('UK','GB')
            
            del sales['year-month']
            
            skus_group = sales.groupby(
                ['date', 'marketplace', 'collection', 'sub-collection', 'size','color']
                )['sku'].agg(lambda x: ', '.join(x.tolist())).reset_index()
    
            del sales['sku']
            sales = sales.groupby(
                ['date', 'marketplace', 'collection', 'sub-collection', 'size','color']
                ).agg('sum').reset_index()
    
            sales['sku'] = skus_group['sku']
            
            sales = pd.merge(
                sales,
                purchased[['date','asin','collection','sub-collection', 'size',
                'color','marketplace','unitsSoldOtherPPC','salesOtherPPC']],
                how='outer',
                on=['date','marketplace','collection','sub-collection', 'size',
                'color'],
                validate="one_to_one"
                )
            
            sales['sku'] = sales['sku'].fillna(sales['asin'])
            del sales['asin']
    
            sales = sales.fillna(0)

            sales = pd.merge(
                sales,
                br,
                how = 'outer',
                on=['date', 'marketplace', 'collection', 'sub-collection', 'size', 'color'],
                validate="one_to_one"
                )
            sales['sku'] = sales['sku'].fillna(sales['asin'])
            del sales['asin']
            
            #sponsored brands section
            sales = pd.merge(sales, sba, how='outer', on=['date', 'marketplace','collection'])
            
            sales = sales.fillna(0)

            sales = pd.merge(
                sales,
                changes,
                how='left',
                on=['date', 'marketplace', 'collection', 'sub-collection', 'size', 'color'],
                validate='one_to_one'
                )
            
            sales['profit w/o overhead'] = sales[
                ['sales','promo_discount','referral_fee','fba_fee',
                 'product_cost','spend','storage','sba_spend', 'dsp_spend']
                ].sum(axis=1)
            sales['ad_units'] = sales[['unitsSoldOwnPPC','unitsSoldOtherPPC','sba_unitsSold','dsp_unitsSold']].sum(axis=1)
            sales['ad_sales'] = sales[['salesOwnPPC','salesOtherPPC','sba_Sales','dsp_Sales']].sum(axis=1)
            sales['organic_units'] = sales['units_sold'] - sales['ad_units']
            sales['organic_sales'] = sales['sales'] - sales['ad_sales']
            
            sales = pd.merge(
                sales,
                returns,
                how = 'outer',
                on = ['date', 'marketplace', 'collection', 'sub-collection', 'size', 'color'],
                validate='one_to_one'
                )
            
            sales['date'] = sales['date'].dt.date
            sales = sales.replace(0,nan)
            return sales
        
        sales = __summarize_sales__()
        cogs = __summarize_cogs__()
        fees = __summarize_fees__()
        ad_spend = __summarize_ad_spend__()
        purchased = __summarize_purchased_product__()
        storage = __summarize_storage_fees__()
        br = __summarize_br__()
        changes = __summarize_changelog__()
        sba = __summarize_non_product_ads__()
        returns = __summarize_returns__()
        
        self.sales_summary = __merge_summary__(
            sales, cogs, fees, ad_spend, purchased, storage, br, changes, sba, returns
            )
        
    @error_checker
    def restock(self, stock_days=49, max_days=90, include_empty:int | str = 0):
        start, end, today = pd.to_datetime(self.start).date(), pd.to_datetime(self.end).date(), (pd.to_datetime('today')-pd.Timedelta(days=1)).date()
        two_weeks_back = (min(end,today) - pd.DateOffset(days=13)).date()
        
        full_range = [x.date() for x in pd.date_range(start, min(end, today))]
        two_weeks = [x.date() for x in pd.date_range(two_weeks_back, min(end, today))]
        calc_range = [x for x in full_range if x not in event_dates_complete]
        calc_range_two_weeks = [x for x in two_weeks if x not in event_dates_complete]
        num_days = len(calc_range)
        two_weeks_days = len(calc_range_two_weeks)
        
        long_term_sales_col = f'units sold {num_days} days'
        short_term_sales_col = f'units sold {two_weeks_days} days'
        long_term_average_sales = f'average sales {num_days} days'
        short_term_average_sales = f'average sales {two_weeks_days} days'

        def get_product_isr():
            isr = self.combined_dfs['inventory_history'].copy()
            isr['date'] = pd.to_datetime(isr['date']).dt.date
            isr = isr[~isr['date'].isin(event_dates_complete)]
            isr = self.__attach_collection__(isr)
            self.__update_product__(isr)
            isr = isr.groupby(
                ['date','marketplace','collection', 'sub-collection', 'size','color']
                ).agg({'available':'sum'}).reset_index()
            isr['isr'] = isr['available'] > 2
            isr = isr.groupby(
                ['marketplace','collection', 'sub-collection', 'size','color']
                ).agg({'isr':'mean'}).reset_index()
            return isr
            
        def get_product_sales():
            sales = self.combined_dfs['orders'].copy()
            sales = sales[~sales['pacific_date'].isin(event_dates_complete)]
            sales = self.__attach_marketplace__(sales, 'sales_channel')        
            sales = self.__attach_collection__(sales)
            self.__update_product__(sales)
            sales_two_weeks = sales[sales['pacific_date'].isin(calc_range_two_weeks)]
            sales = sales.groupby(
                ['marketplace','collection', 'sub-collection', 'size','color']
                ).agg({'units_sold':'sum'}).reset_index()
            sales = sales.rename(columns={'units_sold':long_term_sales_col})
            sales_two_weeks = sales_two_weeks.groupby(
                ['marketplace','collection', 'sub-collection', 'size','color']
                ).agg({'units_sold':'sum'}).reset_index()
            sales_two_weeks = sales_two_weeks.rename(columns={'units_sold':short_term_sales_col})
            total_sales = pd.merge(
                sales, sales_two_weeks, how = 'outer', on = ['marketplace','collection', 'sub-collection', 'size','color'],
                validate='one_to_one')
            return total_sales
        
        def get_inventory():
            overstock_cols = ['quantity_to_be_charged_ais_181_210_days',
                'quantity_to_be_charged_ais_211_240_days',
                'quantity_to_be_charged_ais_241_270_days',
                'quantity_to_be_charged_ais_271_300_days',
                'quantity_to_be_charged_ais_301_330_days',
                'quantity_to_be_charged_ais_331_365_days',
                'quantity_to_be_charged_ais_365_PLUS_days']
            excess_fee_cols = ['estimated_ais_181_210_days', 'estimated_ais_211_240_days',
                'estimated_ais_241_270_days', 'estimated_ais_271_300_days',
                'estimated_ais_301_330_days', 'estimated_ais_331_365_days',
                'estimated_ais_365_plus_days']
            inv_age_cols = ['inv_age_0_to_30_days',
                'inv_age_31_to_60_days', 'inv_age_61_to_90_days',
                'inv_age_91_to_180_days', 'inv_age_181_to_270_days',
                'inv_age_271_to_365_days', 'inv_age_365_plus_days',
                'inv_age_181_to_330_days', 'inv_age_331_to_365_days']
            
            inv_columns = ['sku', 'asin', 'available', 'units_shipped_t7',
                   'units_shipped_t30', 'units_shipped_t60', 'units_shipped_t90',
                   'estimated_excess_quantity'] + inv_age_cols +[
                   'estimated_storage_cost_next_month', 'inbound_quantity',
                   'inbound_working', 'inbound_shipped', 'inbound_received',
                   'reserved_quantity'] + overstock_cols + excess_fee_cols +[
                   'Recommended_ship_in_quantity',
                   'Recommended_ship_in_date','Inventory_Supply_at_FBA',
                   'Reserved_FC_Transfer','Reserved_FC_Processing',
                   'Reserved_Customer_Order','total_days_of_supply_with_open_shipments',
                   'marketplace']
            inv = self.stats['inventory_detailed'][inv_columns].copy()
            inv_price = self.stats['inventory_detailed'][['sku', 'asin','your_price','marketplace']].copy()
            inv = self.__attach_collection__(inv)
            inv_price = self.__attach_collection__(inv_price)
            self.__update_product__(inv)

            for col in ('sku','asin'):
                del inv[col]
                del inv_price[col]

            inv = inv.groupby(['marketplace', 'collection', 'sub-collection', 'size', 'color']).agg('sum').reset_index()
            inv_price = inv_price.groupby(['marketplace', 'collection', 'sub-collection', 'size', 'color']).agg('min').reset_index()
            
            total_inv = pd.merge(inv, inv_price, how='outer', on=['marketplace', 'collection', 'sub-collection', 'size', 'color'], validate='one_to_one')
            total_inv['overstock_units'] = total_inv[overstock_cols].sum(axis=1)
            total_inv['overstock_fees'] = total_inv[excess_fee_cols].sum(axis=1)
            for col in (overstock_cols+excess_fee_cols+inv_age_cols):
                del total_inv[col]
            return total_inv
            
        def get_warehouse():
            incoming = self.incoming_df.pivot_table(values='QtyOrdered', index='sku', columns='year-week', aggfunc='sum').reset_index()
            # incoming['marketplace'] = 'US'
            wh = self.warehouse_df[['sku', 'total_wh', 'QtyPhysical', 'total_receiving']].copy()
            if wh['sku'].duplicated().sum() > 0:
                raise BaseException('Check for duplicate SKUs in warehouse df')
            wh['marketplace'] = 'US'
            case_pack = self.fees_dimensions_df[['sku','sets in a box']].copy()
            wh = pd.merge(wh, case_pack, how='left', on='sku', validate='one_to_one')
            total_wh = pd.merge(wh, incoming, how='outer', on='sku', validate='one_to_one')
            
            dictionary = self.dataset.dictionary[
                [
                    'sku','collection', 'sub-collection','size', 'color','actuality', 'life stage', 'restockable'
                    ]
                ].drop_duplicates('sku').copy()
            us_wh = pd.merge(total_wh, dictionary, how='left', on='sku', validate='one_to_one')
            ca_wh = us_wh.copy()
            ca_wh['marketplace'] = 'CA'
            total_wh = pd.concat([us_wh, ca_wh])
            return total_wh
        
        def calculate_shipment(result, warehouse):
            if not self.dataset:
                raise BaseException("Dictionary not defined for the product")
            dictionary = self.dataset.dictionary[['sku','asin']]
            dictionary = dictionary.drop_duplicates(['sku','asin'])
            wh_columns = warehouse.columns.tolist()
            incoming_columns = [x for x in wh_columns if re.match('2[0-9]{3}-[0-9]{1,2}', x)]
            to_ship = result[
                ['marketplace', 'collection', 'sub-collection', 'size', 'color',
                 long_term_average_sales,short_term_average_sales, 'average corrected',
                 'dos available', 'dos inbound',
                 'available','Inventory_Supply_at_FBA','estimated_excess_quantity']].fillna(0).copy()
            to_ship = to_ship[to_ship['marketplace'].isin(['US','CA'])]
            low_stock = result['dos available'] < 21
            future_sales = to_ship['average corrected'] * stock_days
            max_sales = to_ship['average corrected'] * max_days
            
            to_ship['to ship, units'] = future_sales - to_ship['Inventory_Supply_at_FBA']
            to_ship.loc[low_stock, 'to ship, units'] = max_sales-to_ship['Inventory_Supply_at_FBA']
            to_ship.loc[to_ship['to ship, units']<0, 'to ship, units']=0
            
            total = pd.merge(to_ship, warehouse, how='outer', on=['marketplace', 'collection', 'sub-collection', 'size', 'color'])
            no_sales = (total['available']==0) & (total['average corrected'] == 0) & (total['total_wh']>0)
            total.loc[no_sales, 'to ship, units']=1
            
            
            total = total.sort_values(
                ['marketplace', 'collection', 'sub-collection', 'size', 'color','Inventory_Supply_at_FBA','total_wh'],
                ascending = [True,True,True,True,True,False,False]
                )
            # duplicates = total[['marketplace', 'collection', 'sub-collection', 'size', 'color']].duplicated()
            total.loc[total['restockable'] == "Do not ship to Amazon", 'to ship, units'] = 0
            total.loc[total['total_wh'] == 0, 'to ship, units'] = 0
            # total.loc[duplicates, 'to ship, units'] = 0
            if not include_empty:
                total = total[(total['to ship, units']>0) & (total['to ship, units'].notnull())]
            
            total['to ship, units'] = ceil(total['to ship, units'])
            total['to ship, boxes'] = total['to ship, units'] / total['sets in a box']            
            total['to ship, boxes'] = ceil(total['to ship, boxes'])
            
            total['dos shipped'] = '=(R:R*Q:Q+N:N)/J:J'

            total = total.dropna(subset='sku')
            total = pd.merge(total, dictionary, how = 'left', on = 'sku', validate='m:1')
            
            # total['dos shipped'] = (total['to ship, boxes'] * total['sets in a box'] + total['Inventory_Supply_at_FBA'])/total['average corrected']
            duplicate_products = total[['marketplace', 'collection', 'size', 'color']].duplicated(keep=False)
            if len(duplicate_products) > 0:
                total.loc[duplicate_products, 'potential duplicate'] ='caution, duplicate'
            else:
                total['potential duplicate'] = ""
            cols_reordered = [
                'marketplace', 'collection', 'sub-collection', 'size', 'color','sku','asin',
                long_term_average_sales, short_term_average_sales, 'average corrected',
                'dos available', 'dos inbound', 'available', 'Inventory_Supply_at_FBA',
                'estimated_excess_quantity', 'to ship, units','sets in a box', 
                'to ship, boxes', 'dos shipped', 'total_wh',
                'QtyPhysical', 'total_receiving'] + incoming_columns + ['actuality', 'life stage', 'restockable',
                'potential duplicate']
            
            total = total[cols_reordered]
            return total
            
        
        isr = get_product_isr()
        sales = get_product_sales()
        inventory = get_inventory()
        warehouse = get_warehouse()
        
        
        result = pd.merge(isr, sales, how='outer', on=['marketplace', 'collection', 'sub-collection', 'size', 'color'], validate='one_to_one')
        result['isr'] = result['isr'].fillna(0)
        result[long_term_average_sales] = (result[long_term_sales_col]/num_days/result['isr']).fillna(0)
        result.loc[result['isr']<0.3, long_term_average_sales] = (result[long_term_sales_col]/num_days).fillna(0)
        result[short_term_average_sales] = (result[short_term_sales_col]/two_weeks_days).fillna(0)
        result.loc[result['isr']<0.3, short_term_average_sales] = (result[short_term_sales_col]/two_weeks_days).fillna(0)
        
        
        avg1 = result[long_term_average_sales] * 0.3 + result[short_term_average_sales] * 0.7
        avg2 = result[long_term_average_sales] * 0.7 + result[short_term_average_sales] * 0.3
        
        result['average corrected'] = pd.DataFrame(data={'avg1':avg1, 'avg2':avg2}).min(axis=1)
        result = pd.merge(result, inventory, how='outer', on=['marketplace', 'collection', 'sub-collection', 'size', 'color'], validate='one_to_one')
        result['dos available'] = result['available'].fillna(0) / result['average corrected']
        result['dos inbound'] = result['Inventory_Supply_at_FBA'].fillna(0) / result['average corrected']
        result = result.sort_values(['marketplace', 'collection', 'sub-collection', 'size', 'color'])
        cols_ordered = [
            'marketplace', 'collection', 'sub-collection', 'size', 'color', 'asin','sku',
            long_term_sales_col, short_term_sales_col,'isr', long_term_average_sales,
            short_term_average_sales, 'average corrected', 'available','Inventory_Supply_at_FBA',
            'dos available','dos inbound','units_shipped_t7', 'units_shipped_t30', 'units_shipped_t60',
            'units_shipped_t90', 'estimated_excess_quantity',
            'estimated_storage_cost_next_month', 'inbound_quantity',
            'inbound_working', 'inbound_shipped', 'inbound_received',
            'reserved_quantity', 'Recommended_ship_in_quantity',
            'Recommended_ship_in_date', 
            'Reserved_FC_Transfer', 'Reserved_FC_Processing',
            'Reserved_Customer_Order', 'total_days_of_supply_with_open_shipments',
            'your_price', 'overstock_units', 'overstock_fees'
            ]
        result = self.__attach_ids__(result)
        result = result[cols_ordered]
        self.restock_summary = result.copy()

        self.to_ship = calculate_shipment(result, warehouse)

        container_weeks = [x for x in self.to_ship.columns if re.match(r"[0-9]{4}-[0-9]{1,2}", str(x))]
        inv_columns = ['total_wh','total_receiving'] + container_weeks
        wh_situation = self.to_ship.copy()
        wh_situation = wh_situation[wh_situation['marketplace'].isin(['US',nan])]
        self.wh_situation = wh_situation.groupby(
            ['collection','size','color','asin']
            )[inv_columns].agg('sum').reset_index()