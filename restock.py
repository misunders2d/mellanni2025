# try:
#     import fireducks.pandas as pd
# except ImportError:
import pandas as pd

from utils.dataset import Dataset


dataset = Dataset(start="2024-07-01", end="2024-12-31", local_data=False, save=True)
dataset.query()

class Product:
    def __init__(self,
                 collection='1800 Bed Sheets',
                 sub_collection='1800 Bed Sheet Set - Solid - White',
                 size='King',
                 color='White'):
        self.collection = collection
        self.sub_collection = sub_collection
        self.size = size
        self.color = color
        self.skus = set()
        self.asins = set()
        self._init_skus()

    def _init_skus(self):
        dictionary = dataset.dictionary.copy()
        dictionary.columns = [x.strip().lower() for x in dictionary.columns]
        id_list = dictionary[
            (dictionary['collection'] == self.collection) &
            (dictionary['sub-collection'] == self.sub_collection) &
            (dictionary['size'] == self.size) &
            (dictionary['color'] == self.color)
            ][['sku', 'asin']].values.tolist()
        self.skus.update({x[0] for x in id_list})
        self.asins.update({x[1] for x in id_list})

    def _update_ids(self, df):
        if 'sku' in df.columns:
            self.skus.update(df['sku'].unique())
        if 'asin' in df.columns:
            self.asins.update(df['asin'].unique())

    def _pull_orders(self):
        self.orders = dataset.orders[(dataset.orders['sku'].isin(self.skus)) | (dataset.orders['asin'].isin(self.asins))]
        dataset.orders = dataset.orders[~dataset.orders.index.isin(self.orders.index)]
        self._update_ids(self.orders)

product = Product()
product._pull_orders()
print(product.asins, product.skus)
