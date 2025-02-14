# try:
#     import fireducks.pandas as pd
# except ImportError:
import pandas as pd
import pandas_gbq
import os

from scripts import size_match
from utils import mellanni_modules as mm

from connectors import gcloud as gc
from connectors import gdrive as gd
from common import events, excluded_collections, user_folder
import asyncio

START = "2025-02-01"
END = "2025-12-31"
MARKET = "US"
CHANNEL = "amazon.com"
LOCAL = True
SAVE = False

class Dataset:
    def __init__(self, start=START, end=END, market=MARKET, channel=CHANNEL, local_data=LOCAL, save=SAVE):
        self.client = gc.gcloud_connect()
        self.start = start
        self.end = end
        self.market = market
        self.channel = channel
        self.local_data = local_data
        self.save = save
        self.fba_shipments = None

    def pull_br_asin_data(self):
        "pulls sessions data (detailed) per asin for all products regardless of sales"
        if self.local_data:
            result = pd.read_csv(os.path.join(user_folder, 'br_asin.csv'))
        else:
            query = f'''SELECT DATE(date) as date, childAsin as asin,
                        unitsOrdered, unitsOrderedB2B,
                        orderedProductSales, orderedProductSalesB2B,
                        browserSessions, browserSessionsB2B,
                        mobileAppSessions, mobileAppSessionsB2B,
                        sessions, sessionsB2B,
                        browserPageViews, browserPageViewsB2B,
                        mobileAppPageViews, mobileAppPageViewsB2B,
                        pageViews, pageViewsB2B,
                        FROM `reports.business_report_asin`
                        WHERE DATE(date) >= DATE("{self.start}") AND  DATE(date) <= DATE("{self.end}")
                        AND country_code = "{self.market}"
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'br_asin.csv'), index=False)
        self.br_asin = result

    def pull_br_data(self):
        "pulls sales data per sku. only those skus that had at least 1 sale are pulled"
        if self.local_data:
            result = pd.read_csv(os.path.join(user_folder, 'br.csv'))
        else:
            query = f'''SELECT DATE(date) as date, sku, childAsin as asin,
                        unitsOrdered, unitsOrderedB2B,
                        orderedProductSales, orderedProductSalesB2B,
                        browserSessions, browserSessionsB2B,
                        mobileAppSessions, mobileAppSessionsB2B,
                        sessions, sessionsB2B,
                        browserPageViews, browserPageViewsB2B,
                        mobileAppPageViews, mobileAppPageViewsB2B,
                        pageViews, pageViewsB2B,
                        FROM `reports.business_report`
                        WHERE DATE(date) >= DATE("{self.start}") AND  DATE(date) <= DATE("{self.end}")
                        AND country_code = "{self.market}"
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'br.csv'), index=False)
        self.br = result

    def pull_order_data(self):
        "pulls data from order reports, converting datetime to pacific timezone"
        if self.local_data:
            result = pd.read_csv(os.path.join(user_folder, 'orders.csv'))
        else:
            query = f"""
                    SELECT
                    DATETIME(purchase_date, "America/Los_Angeles") as pacific_datetime,
                    DATE(purchase_date, "America/Los_Angeles") as pacific_date,
                    amazon_order_id,
                    sku,asin,quantity as units_sold,currency, item_price as sales, is_business_order,
                    ship_city, ship_state, ship_postal_code, ship_country
                    FROM `reports.all_orders`
                    WHERE (DATE(DATETIME(purchase_date, "America/Los_Angeles")) BETWEEN DATE("{self.start}") AND DATE("{self.end}"))
                    AND (LOWER(sales_channel) = "{self.channel}")
                    AND (order_status != "Cancelled")
                    AND (item_status != "Cancelled")
                    """
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'orders.csv'), index=False)
        self.orders = result
        
    def pull_inventory_data(self):
        "pulls LATEST comprehensive inventory data from the new 'fba inventory' report to show stats for the latest date in the report or request"
        if self.local_data:
            result = pd.read_csv(os.path.join(user_folder, 'inventory.csv'))
        else:
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
                        WHERE DATE(snapshot_date) = LEAST(
                            (SELECT MAX(DATE(snapshot_date)) FROM `reports.fba_inventory_planning` WHERE marketplace = "{self.market}"),DATE("{self.end}")
                        )
                        AND marketplace = "{self.market}"
                        AND LOWER(condition) != "used"
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'inventory.csv'), index=False)
        self.inventory = result

    def pull_inventory_history(self):
        "pulls inventory history (available) from the new 'fba inventory' report for the given period to identify isr"
        if self.local_data:
            result = pd.read_csv(os.path.join(user_folder, 'inventory_history.csv'))
        else:
            query = f'''SELECT DATE(snapshot_date) AS date, sku, asin, available, Inventory_Supply_at_FBA
                        FROM `reports.fba_inventory_planning`
                        WHERE DATE(snapshot_date) BETWEEN DATE("{self.start}") AND DATE("{self.end}")
                        AND marketplace = "{self.market}"
                        AND LOWER(condition) != "used"
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'inventory_history.csv'), index=False)
        self.inventory_history = result

    def pull_dictionary(self): # TODO add conditional to download different self.markets' dictionaries
        if self.local_data:
            result = pd.read_csv(os.path.join(user_folder, 'dictionary.csv'))
        else:
            dictionary_id = gd.find_file_id(folder_id='1zIHmbWcRRVyCTtuB9Atzam7IhAs8Ymx4', filename='Dictionary.xlsx', drive_id='0AMdx9NlXacARUk9PVA')
            result:pd.DataFrame = pd.read_excel(gd.download_file(file_id=dictionary_id))
            if self.save:
                result.to_csv(os.path.join(user_folder, 'dictionary.csv'), index=False)
        self.dictionary = result
        self.dictionary.columns = [x.strip().lower() for x in self.dictionary.columns]

    def pull_advertised_product_data(self):
        if self.local_data:
            result = pd.read_csv(os.path.join(user_folder, 'advertised_product.csv'))
        else:
            query = f'''SELECT DATE(date) AS date, advertisedSku AS sku, advertisedAsin as asin,
                        SUM(clicks) AS clicks, SUM(impressions) as impressions, SUM(spend) AS spend,
                        SUM(unitsSoldSameSku14d) AS sameSkuUnits,
                        SUM(attributedSalesSameSku14d) as sameSkuSales,
                        FROM `reports.AdvertisedProduct`
                        WHERE DATE(date) BETWEEN DATE("{self.start}") AND DATE("{self.end}")
                        AND UPPER(country_code) = "{self.market}"
                        GROUP BY date, advertisedSku, advertisedAsin ORDER BY date, advertisedSku, advertisedAsin
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'advertised_product.csv'), index=False)
        self.advertised_product = result

    def pull_purchased_product_data(self):
        if self.local_data:
            result = pd.read_csv(os.path.join(user_folder, 'purchased_product.csv'))
        else:
            query = f'''SELECT DATE(date) AS date, advertisedSku AS sku, advertisedAsin as asin, purchasedAsin,
                        SUM(unitsSoldOtherSku14d) AS otherSkuUnits,
                        SUM(salesOtherSku14d) as otherSkuSales,
                        FROM `reports.PurchasedProduct`
                        WHERE DATE(date) BETWEEN DATE("{self.start}") AND DATE("{self.end}")
                        AND UPPER(country_code) = "{self.market}"
                        GROUP BY date, advertisedSku, advertisedAsin, purchasedAsin ORDER BY date, advertisedSku, advertisedAsin, purchasedAsin
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'purchased_product.csv'), index=False)
        self.purchased_product = result

    def pull_attribution_data(self): # NOT product specific
        if self.local_data:
            result = pd.read_csv(os.path.join(user_folder, 'attribution.csv'))
        else:
            query = f'''SELECT DATE(date) AS date,
                        SUM(unitsSold14d) AS specificUnitsSold, SUM(totalUnitsSold14d) AS totalUnitsSold,
                        SUM(attributedSales14d) as specificSales, SUM(totalAttributedSales14d) as totalSales,
                        FROM `reports.attribution`
                        WHERE DATE(date) BETWEEN DATE("{self.start}") AND DATE("{self.end}")
                        AND UPPER(countryCode) = "{self.market}"
                        GROUP BY date ORDER BY date
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'attribution.csv'), index=False)
        self.attribution = result

    def pull_dsp_data(self): # NOT product specific
        if self.local_data:
            result = pd.read_csv(os.path.join(user_folder, 'dsp.csv'))
        else:
            query = f'''SELECT DATE(date) AS date,
                        SUM(total_cost) AS total_cost, SUM(Impressions) AS impressions,
                        SUM(click_throughs) AS click_throughs,
                        SUM(units_sold) AS specificUnitsSold, SUM(total_units_sold) AS totalUnitsSold,
                        SUM(sales_usd) as specificSales, SUM(total_sales_usd) as totalSales,
                        SUM(new_to_brand_units_sold) AS specificNewUnitsSold, SUM(total_new_to_brand_units_sold) AS totalNewUnitsSold,
                        FROM `reports.dsp_report`
                        WHERE DATE(date) BETWEEN DATE("{self.start}") AND DATE("{self.end}")
                        GROUP BY date ORDER BY date
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'dsp.csv'), index=False)
        self.dsp = result

    def pull_sba_data(self): #TODO # NOT product specific
        self.sba = None

    def pull_sbv_data(self): #TODO # NOT product specific
        self.sbv = None

    def pull_sd_data(self): #TODO # NOT product specific
        self.sd = None

    def pull_fba_shipments_data(self):
        if self.local_data:
            result = pd.read_csv(os.path.join(user_folder, 'fba_shipments.csv'))
        else:
            query = f"""
                    SELECT
                    DATE(purchase_date, "America/Los_Angeles") as pacific_date, amazon_order_id,
                    sku, shipment_item_id, quantity_shipped as units_sold, currency, item_price as sales
                    FROM `reports.shipments`
                    WHERE (DATE(DATETIME(purchase_date, "America/Los_Angeles")) BETWEEN DATE("{self.start}") AND DATE("{self.end}"))
                    AND (LOWER(sales_channel) = "{self.channel}")
                    ORDER BY purchase_date, amazon_order_id, sku
                    """
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'fba_shipments.csv'), index=False)
        self.fba_shipments = result

    def pull_promotions(self):
        "generates promotions data from the 'shipment_item_id' list obtained from fba_shipments report"
        if self.local_data:
            result = pd.read_csv(os.path.join(user_folder, 'promotions.csv'))
        else:
            if not self.fba_shipments:
                self.pull_fba_shipments_data()
            shipment_item_ids = self.fba_shipments[['shipment_item_id','sku']].drop_duplicates()
            amazon_purchase_dates = self.fba_shipments[['amazon_order_id','pacific_date']].drop_duplicates()
            order_sales_data = self.fba_shipments[['shipment_item_id','units_sold','sales']].groupby('shipment_item_id').sum().reset_index()
            pandas_gbq.to_gbq(shipment_item_ids, destination_table='auxillary_development.temp_shipment_ids', if_exists='replace')

            query = '''SELECT amazon_order_id, shipment_item_id, description, item_promotion_discount
                        FROM `reports.promotions`
                        WHERE shipment_item_id IN
                            (SELECT shipment_item_id 
                            FROM `auxillary_development.temp_shipment_ids`) 
                        '''
            ids_result:pd.DataFrame = self.client.query(query).to_dataframe()
            result = pd.merge(ids_result, shipment_item_ids, how='left', on='shipment_item_id')
            result = pd.merge(result, amazon_purchase_dates, how='left', on='amazon_order_id')
            result = pd.merge(result, order_sales_data, how='left', on='shipment_item_id')
            result.loc[result['shipment_item_id'].duplicated(), ['units_sold','sales']] = 0
            # result.loc[result['shipment_item_id'].duplicated(), ] = 0
            result = result.sort_values(
                by=['pacific_date','amazon_order_id','shipment_item_id', 'item_promotion_discount','sales'],
                    ascending=[True, True, True, False, False]
                    )
            self.client.query('DROP TABLE `auxillary_development.temp_shipment_ids`')
            if self.save:
                result.to_csv(os.path.join(user_folder, 'promotions.csv'), index=False)
        self.promotions = result

    def pull_fees_dimensions(self):
        if self.local_data:
            fees = pd.read_csv(os.path.join(user_folder, 'fees.csv'))
        else:
            fees = size_match.main(out=False)
            if self.save:
                fees.to_csv(os.path.join(user_folder, 'fees.csv'), index=False)
        self.fees = fees

    def pull_warehouse(self):
        "pulls data from sellercloud, aggregating inventory stock at warehouse"
        if self.local_data:
            warehouse = pd.read_csv(os.path.join(user_folder, 'warehouse.csv'))
        else:
            query = f"""
                    SELECT date, ProductID as sku, QtyAvailable, QtyPhysical, BinType, Sellable, BinName
                    FROM `mellanni-project-da.sellercloud.inventory_bins`
                    WHERE DATE(date)=LEAST(
                    (SELECT MAX(date) FROM `mellanni-project-da.sellercloud.inventory_bins`), DATE("{self.end}")
                    )
                    """
            result:pd.DataFrame = self.client.query(query).to_dataframe()

            # split warehouse inventory by sellable and receiving
            sellable = result.query('Sellable == True & BinType != "Picking" & ~BinName.str.startswith("DS")')
            sellable = sellable.pivot_table(
                values = ['QtyAvailable', 'QtyPhysical', 'date'],
                index = 'sku',
                aggfunc = {'QtyAvailable':'sum', 'QtyPhysical':'sum', 'date':'max'}
                ).reset_index()
            sellable = sellable.rename(columns = {'QtyAvailable':'total_wh'})
            
            receiving = result.query('Sellable == False & BinType == "Receiving"')
            receiving = receiving.pivot_table(
                values = ['QtyAvailable', 'QtyPhysical', 'date'],
                index = 'sku',
                aggfunc = {'QtyAvailable':'sum', 'QtyPhysical':'sum', 'date':'max'}
                ).reset_index()
            receiving['total_receiving'] = receiving[['QtyAvailable', 'QtyPhysical']].sum(axis = 1)
            warehouse = pd.merge(sellable, receiving[['date','sku','total_receiving']], how = 'outer', on = ['date','sku'])
            if self.save:
                warehouse.to_csv(os.path.join(user_folder, 'warehouse.csv'), index=False)
        self.warehouse = warehouse

    def pull_changelog(self): #TODO
        self.changes = None

    def pull_incoming(self): #TODO
        self.incoming = None
    
    def pull_pricing(self): #TODO
        self.pricing = None

    async def _pull_all_data(self):
        functions = [
            self.pull_br_asin_data, self.pull_br_data, self.pull_order_data, self.pull_inventory_data,
            self.pull_inventory_history, self.pull_advertised_product_data,
            self.pull_purchased_product_data, self.pull_attribution_data, self.pull_dsp_data,
            self.pull_sba_data, self.pull_sbv_data, self.pull_sd_data, self.pull_promotions,
            self.pull_fees_dimensions, self.pull_warehouse, self.pull_changelog, self.pull_incoming
        ]

        async def run_function(func):
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, func)

        tasks = [run_function(func) for func in functions]
        await asyncio.gather(*tasks)
    
    def query(self):
        asyncio.run(self._pull_all_data())
        self.pull_dictionary()
        self.pull_pricing()

