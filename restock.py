# try:
#     import fireducks.pandas as pd
# except ImportError:
import pandas as pd, time
import asyncio

from utils.dataset import Dataset
from utils.product import Product
import sys


dataset = Dataset(start="2024-09-01", end="2025-12-31", local_data=False, save=True)
# dataset.query()
# dataset.pull_promotions()
# dataset.pull_fees_dimensions()
dataset.pull_incoming()



asins = ['B00NLLUMOE','B00NQDGAP2','B00O35DAL4','B00O35CWQ8','B00SBZJ8NG','B08RZDBZZJ','B0822X1VP7','B00RKHWJ1O','B00NLLUP4G','B08RZCYC5X','B0822X4TLW','B00NLLUNSE','B00SU0QSZ8','B00NQDGCW8','B01DN0AJXQ','B00RKHX3E6','B00NQDGBTC']

# products = [Product(asin=x, dataset=dataset) for x in asins]

# for product in products:
#     print(len(Product.dataset.promotions))
#     product._pull_promotions()

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
