# try:
#     import fireducks.pandas as pd
# except ImportError:
import pandas as pd, time
import asyncio

from utils.dataset import Dataset
from utils.product import Product
import sys

start = time.perf_counter()
dataset = Dataset(start="2025-02-01", end="2025-03-31", local_data=False, save=True, market=['US'])
# dataset.query_sync()
# dataset.query()
# dataset.pull_warehouse()
# dataset.warehouse.to_excel('/home/misunderstood/temp/wh.xlsx', index = False)
# dataset.pull_fees_dimensions()
# dataset.pull_pricing()
# dataset.pull_cogs()
dataset.pull_promotions()
print(time.perf_counter() - start)



asins = ['B00NLLUMOE','B00NQDGAP2','B00O35DAL4','B00O35CWQ8','B00SBZJ8NG','B08RZDBZZJ','B0822X1VP7','B00RKHWJ1O','B00NLLUP4G','B08RZCYC5X','B0822X4TLW','B00NLLUNSE','B00SU0QSZ8','B00NQDGCW8','B01DN0AJXQ','B00RKHX3E6','B00NQDGBTC']
# asins = dataset.dictionary['asin'].unique()

# products = [Product(asin=x, dataset=dataset) for x in asins]
# product = Product(asin='B00NQDGAP2', dataset=dataset)
# product.populate_loop()
# product.calculate_loop("2024-01-01", "2024-01-31")
# print(product.stats['orders'])



# for product in products:
#     try:
#         product.populate_loop()
#     except Exception as e:
#         print(f'Population error with {product.asins}: {e}')
#     try:
#         product.calculate_loop("2025-01-01", "2025-12-31")
#     except Exception as e:
#         print(f'Calculation error with {product.asins}: {e}')
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
