# try:
#     import fireducks.pandas as pd
# except ImportError:
import pandas as pd, time, pickle
import asyncio

from classes.dataset import Dataset
from classes.product import Product

start = time.perf_counter()
dataset = Dataset(start="2025-01-01", end="2025-12-31", local_data=True, save=False, market=['US','CA'])
# dataset.query_sync()
dataset.query()
# dataset.pull_order_data()
# dataset.warehouse.to_excel('/home/misunderstood/temp/wh.xlsx', index = False)
# dataset.pull_fees_dimensions()
# dataset.pull_pricing()
# dataset.pull_cogs()
# dataset.pull_promotions()
# dataset.pull_inventory_history()
# dataset.pull_dictionary()
print(time.perf_counter() - start)
# dataset.inventory.to_excel('/home/misunderstood/inventory.xlsx')


def aggregate_ppc_data(dataset):
    # advertised = dataset.
    pass


# asins = ['B0822Z5KTC',
#  'B0822YWR51',
#  'B0822Z1N5D',
#  'B0822Z86NL',
#  'B0822YM74W',
#  'B082311FB2',
#  'B0822ZMPN8',
#  'B0822YSQQG',
#  'B0822ZK6VY',
#  'B08DRGYGVS',
#  'B0822XSCPT',
#  'B0822YLVJG',
#  'B0822Y149H',
#  'B0822ZK1MH',
#  'B0822XRZG4',
#  'B0822YFZ3Y',
#  'B0822XZ4VD',
#  'B0822Y134L',
#  'B0822ZKT44',
#  'B0B2Q6SD8M',
#  'B0B2QFBJGH',
#  'B0B2QP99QY',
#  'B0B2Q4CQ1K',
#  'B0B2Q3MXRY',
#  'B0B2Q6XM8R',
#  'B0B2Q4T1BH',
#  'B0B2Q5M6KH',
#  'B0B2Q3NLYP',
#  'B0B2Q3FF48',
#  'B0B2Q4BMD1',
#  'B0B2Q6VD38',
#  'B0822YKJGX',
#  'B0822ZKZSY',
#  'B0822Y5XHB',
#  'B0822YQTX5',
#  'B0822YMRLF',
#  'B0822XNZ8M',
#  'B0822Z37LB',
#  'B0823115FS',
#  'B0822YV9ZR',
#  'B0822ZSBN1',
#  'B0822YXV1M',
#  'B0822ZFNGJ',
#  'B0823123JL',
#  'B0822YT8ZZ',
#  'B0822YSV5P',
#  'B082311F9Z',
#  'B0822Z1P7N',
#  'B0822XRHJV',
#  'B0822ZBD5Z',
#  'B0822Z48C9',
#  'B0822XSX2L',
#  'B0822XPVC9',
#  'B0822Z6C7P',
#  'B0822YHVQ4',
#  'B0822Z2JQL',
#  'B0822XYSQR',
#  'B08DRH63Y7',
#  'B0822YVYHQ',
#  'B0822Y48HH',
#  'B0822Y83D3',
#  'B0822Y8J89',
#  'B0822XXQ1H',
#  'B0822XRVC6',
#  'B0822XW5PF',
#  'B0822ZP2SL',
#  'B0822YXD3M',
#  'B0822ZGPDG',
#  'B0822YSHTY',
#  'B0822XBDZW',
#  'B0822YMKR5',
#  'B0822ZZJLL',
#  'B0822Y8RZ1',
#  'B0822YLBZP']
# asins = dataset.dictionary['asin'].unique()

# products = [Product(asin=x, dataset=dataset) for x in asins]
# product = Product(asin='B0822X1VP7', dataset=dataset)
# product._calculate_inventory_history(start="2025-01-01", end="2025-12-31")
# product._pull_inventory_history()
# product.populate_loop()
# product.calculate_loop("2025-01-01", "2025-01-31")
# product.inventory_history_df.to_excel('/home/misunderstood/temp/inventory_history.xlsx')


# with open('/home/misunderstood/temp/products.pkl','rb') as f:
#     products = pickle.load(f)

# for product in products:
#     try:
#         # product.populate_loop()
#         product._calculate_inventory_history(start="2025-01-01", end="2025-12-31")
#     except Exception as e:
#         print(f'Population error with {product.asins}: {e}')
# with open('/home/misunderstood/temp/products.pkl','wb') as f:
#     pickle.dump(products, f)
# try:
#     product.calculate_loop("2025-01-01", "2025-12-31")
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
# # # product.calculate_loop("2025-01-01", "2025-12-31")

# product._calculate_inventory("2025-01-01", "2025-12-31")
# # print(product.stats)
# print(product.inventory)
# product.inventory.to_excel('/home/misunderstood/temp/product_inventory.xlsx', index=False)
