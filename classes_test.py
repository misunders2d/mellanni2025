import pandas as pd
import os

# import size_match

from connectors import gcloud as gc
from connectors import gdrive as gd
from common import user_folder #import events, excluded_collections, user_folder

START = "2024-07-01"
END = "2024-12-31"
MARKET = "US"
CHANNEL = "amazon.com"

client = gc.gcloud_connect()

def pull_br_data(start=START, end=END, market=MARKET):
    result = pd.read_csv(os.path.join(user_folder, 'br.csv'))
    query = f'''SELECT DATE(date) as date, sku, childAsin as asin, unitsOrdered, sessions
                FROM `reports.business_report`
                WHERE DATE(date) >= DATE("{start}") AND  DATE(date) <= DATE("{end}")
                AND country_code = "{market}"
                '''
    # result:pd.DataFrame = client.query(query).to_dataframe()
    # result.to_csv(os.path.join(user_folder, 'br.csv'), index=False)
    return result

def pull_order_data(start=START, end=END, channel=CHANNEL):
    result = pd.read_csv(os.path.join(user_folder, 'orders.csv'))
    query = f"""
            SELECT
            DATETIME(purchase_date, "America/Los_Angeles") as pacific_datetime,
            DATE(purchase_date, "America/Los_Angeles") as pacific_date,
            sku,asin,quantity as units_sold,currency, item_price as sales, is_business_order
            FROM `reports.all_orders`
            WHERE (DATE(DATETIME(purchase_date, "America/Los_Angeles")) BETWEEN DATE("{start}") AND DATE("{end}"))
            AND (LOWER(sales_channel) = "{channel}")
            AND (order_status != "Cancelled")
            AND (item_status != "Cancelled")
            """
    # result:pd.DataFrame = client.query(query).to_dataframe()
    # result.to_csv(os.path.join(user_folder, 'orders.csv'), index=False)
    return result
    
def pull_inventory_data(end=END, market=MARKET):
    result = pd.read_csv(os.path.join(user_folder, 'inventory.csv'))
    query = f'''SELECT DATE(snapshot_date) AS date, sku, asin, available,
                units_shipped_t7, units_shipped_t30, units_shipped_t60, units_shipped_t90,
                your_price, sales_price, sell_through, item_volume, storage_type, storage_volume, sales_rank, days_of_supply,
                estimated_excess_quantity,
                inv_age_0_to_30_days, inv_age_31_to_60_days, inv_age_61_to_90_days, inv_age_91_to_180_days,
                inv_age_181_to_270_days, inv_age_271_to_365_days, inv_age_365_plus_days, inv_age_181_to_330_days, inv_age_331_to_365_days,
                estimated_storage_cost_next_month,
                inbound_quantity, inbound_working, inbound_shipped, inbound_received, reserved_quantity,
                quantity_to_be_charged_ais_181_210_days, quantity_to_be_charged_ais_211_240_days, quantity_to_be_charged_ais_241_270_days,
                quantity_to_be_charged_ais_271_300_days, quantity_to_be_charged_ais_301_330_days, quantity_to_be_charged_ais_331_365_days,
                quantity_to_be_charged_ais_365_PLUS_days,
                estimated_ais_181_210_days, estimated_ais_211_240_days, estimated_ais_241_270_days, estimated_ais_271_300_days,
                estimated_ais_301_330_days, estimated_ais_331_365_days, estimated_ais_365_plus_days,
                fba_inventory_level_health_status,
                Recommended_ship_in_quantity, Recommended_ship_in_date,
                Inventory_Supply_at_FBA, Reserved_FC_Transfer, Reserved_FC_Processing, Reserved_Customer_Order, total_days_of_supply_with_open_shipments
                FROM `reports.fba_inventory_planning`
                WHERE DATE(snapshot_date) = LEAST((SELECT MAX(DATE(snapshot_date)) FROM `reports.fba_inventory_planning` WHERE marketplace = "{market}"),DATE("{end}"))
                AND marketplace = "{market}"
                AND LOWER(condition) != "used"
                '''
    # result:pd.DataFrame = client.query(query).to_dataframe()
    # result.to_csv(os.path.join(user_folder, 'inventory.csv'), index=False)
    return result

def pull_dictionary(market=MARKET):
    result = pd.read_csv(os.path.join(user_folder, 'dictionary.csv'))
    # result:pd.DataFrame = gd.download_gspread(spreadsheet_id='1tezZ1Txml4E1YGYnO8-57lSnxlzsJ2boUYb4xmVxmtw', sheet_id='1749064367')
    # result.to_csv(os.path.join(user_folder, 'dictionary.csv'), index=False)
    return result

def pull_advertised_product_data(start=START, end=END, market=MARKET):
    pass

def pull_purchased_product_data(start=START, end=END, market=MARKET):
    pass

def pull_fees_dimensions():
    fees = pd.read_csv(os.path.join(user_folder, 'fees.csv'))
    # fees = size_match.main(out=False)
    # fees.to_csv(os.path.join(user_folder, 'fees.csv'), index=False)
    return fees

dictionary = pull_dictionary()
br = pull_br_data()
orders = pull_order_data()
inventory = pull_inventory_data()
fees = pull_fees_dimensions()

class SheetSet:
    def __init__(self, collection_str, size_str, color_str):
        self.collection = collection_str
        self.size = size_str
        self.color = color_str

    def get_skus(self):
        dict_slice = dictionary[
            (dictionary['Collection']==self.collection) & (dictionary['Size Map']==self.size) & (dictionary['Color']==self.color)
            ]
        self.skus = set(dict_slice['SKU'].unique())
    def get_asins(self):
        dict_slice = dictionary[
            (dictionary['Collection']==self.collection) & (dictionary['Size Map']==self.size) & (dictionary['Color']==self.color)
            ]
        self.asins = set(dict_slice['ASIN'].unique())

    def update_ids(self):
        br_slice = br[
            (br['sku'].isin(self.skus)) | (br['asin'].isin(self.asins))
        ]
        br_asins = br_slice['asin'].unique()
        br_skus = br_slice['sku'].unique()
        self.skus.update(br_skus)
        self.asins.update(br_asins)

    def get_br(self):
        br_slice = br[
            (br['sku'].isin(self.skus)) | (br['asin'].isin(self.asins))
        ]
        self.sales = br_slice

qwhite = SheetSet(collection_str='1800 Bed Sheets', size_str='Queen',color_str='White')
print(qwhite.size)
qwhite.get_skus()
qwhite.get_asins()
qwhite.update_ids()
qwhite.get_br()
print(qwhite.sales)
# print(qwhite.skus, qwhite.asins)
kwhite = SheetSet(collection_str='1800 Bed Sheets', size_str='King',color_str='White')
print(kwhite.size)
kwhite.get_skus()
kwhite.get_asins()
kwhite.update_ids()
kwhite.get_br()
print(kwhite.sales)
# print(kwhite.skus, kwhite.asins)


### Homework for 12/20:
# Create a method to calculate `self.conversion` for any instance of SheetSet class
# We should be able to call `qwhite.conversion` and receive a result









