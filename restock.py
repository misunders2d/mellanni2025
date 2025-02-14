# try:
#     import fireducks.pandas as pd
# except ImportError:
import pandas as pd

from utils.dataset import Dataset
import sys


dataset = Dataset(start="2024-09-01", end="2024-12-31", local_data=True, save=False)
dataset.query()

class Product:
    def __init__(self,
                 asin="B01E7UJA4U",
                 sku=None):
        if not (asin or sku):
            raise ValueError("Either 'asin' or 'sku' must be provided.")
        if asin and sku:
            raise ValueError("Only one of 'asin' or 'sku' must be provided.")

        self.asins = set()
        self.skus = set()
        self.collections = set()
        self.sub_collections = set()
        self.sizes = set()
        self.colors = set()
        if asin:
            self.asins.add(asin)
        if sku:
            self.skus.add(sku)
        self._init_skus()

    def _init_skus(self):
        dictionary = dataset.dictionary
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
        self.orders_df = dataset.orders[(dataset.orders['sku'].isin(self.skus)) | (dataset.orders['asin'].isin(self.asins))]
        dataset.orders = dataset.orders[~dataset.orders.index.isin(self.orders_df.index)]
        self._update_ids(self.orders_df)

    def _pull_br(self):
        self.br_df = dataset.br[(dataset.br['sku'].isin(self.skus)) | (dataset.br['asin'].isin(self.asins))]
        dataset.br = dataset.br[~dataset.br.index.isin(self.br_df.index)]
        self._update_ids(self.br_df)

    def _pull_br_asin(self):
        self.br_asin_df = dataset.br_asin[dataset.br_asin['asin'].isin(self.asins)]
        dataset.br_asin = dataset.br_asin[~dataset.br_asin.index.isin(self.br_asin_df.index)]
        self._update_ids(self.br_asin_df)

    def _pull_inventory(self):
        self.inventory_df = dataset.inventory[(dataset.inventory['sku'].isin(self.skus)) | (dataset.inventory['asin'].isin(self.asins))]
        dataset.inventory = dataset.inventory[~dataset.inventory.index.isin(self.inventory_df.index)]
        self._update_ids(self.inventory_df)

    def _pull_inventory_history(self):
        self.inventory_history_df = dataset.inventory_history[(dataset.inventory_history['sku'].isin(self.skus)) | (dataset.inventory_history['asin'].isin(self.asins))]
        dataset.inventory_history = dataset.inventory_history[~dataset.inventory_history.index.isin(self.inventory_history_df.index)]
        self._update_ids(self.inventory_history_df)

    def _pull_advertised_product(self):
        self.advertised_product_df = dataset.advertised_product[(dataset.advertised_product['sku'].isin(self.skus)) | (dataset.advertised_product['asin'].isin(self.asins))]
        dataset.advertised_product = dataset.advertised_product[~dataset.advertised_product.index.isin(self.advertised_product_df.index)]
        self._update_ids(self.advertised_product_df)

    def _pull_purchased_product(self):
        self.purchased_product_df = dataset.purchased_product[
            (dataset.purchased_product['sku'].isin(self.skus)) |
            (dataset.purchased_product['asin'].isin(self.asins)) |
            (dataset.purchased_product['purchasedAsin'].isin(self.asins))
            ]

    def _pull_promotions(self):
        self.promotions_df = dataset.promotions[(dataset.promotions['sku'].isin(self.skus))]
        dataset.promotions = dataset.promotions[~dataset.promotions.index.isin(self.promotions_df.index)]
        self._update_ids(self.promotions_df)

    def _pull_fees_dimensions(self):
        self.fees_dimensions_df = dataset.fees[(dataset.fees['sku'].isin(self.skus)) | (dataset.fees['asin'].isin(self.asins))]
        dataset.fees = dataset.fees[~dataset.fees.index.isin(self.fees_dimensions_df.index)]
        self._update_ids(self.fees_dimensions_df)

    def _pull_warehouse(self):
        self.warehouse_df = dataset.warehouse[(dataset.warehouse['sku'].isin(self.skus))]
        dataset.warehouse = dataset.warehouse[~dataset.warehouse.index.isin(self.warehouse_df.index)]
        self._update_ids(self.warehouse_df)

    def populate(self):
        """ function that pulls all data for a product from the provided dataset """
        self._pull_orders()
        self._pull_br()
        self._pull_br_asin()
        self._pull_inventory()
        self._pull_inventory_history()
        self._pull_advertised_product()
        self._pull_purchased_product()
        self._pull_promotions()
        self._pull_fees_dimensions()
        self._pull_warehouse()

asins = ['B00NLLUMOE','B00NQDGAP2','B00O35DAL4','B00O35CWQ8','B00SBZJ8NG','B08RZDBZZJ','B0822X1VP7','B00RKHWJ1O','B00NLLUP4G','B08RZCYC5X','B0822X4TLW','B00NLLUNSE','B00SU0QSZ8','B00NQDGCW8','B01DN0AJXQ','B00RKHX3E6','B00NQDGBTC']

products = [Product(asin=x) for x in asins]
for product in products:
    product.populate()

print(products[-1].warehouse_df)