# try:
#     import fireducks.pandas as pd
# except ImportError:
import pandas as pd
import pandas_gbq
import os
from typing import Literal, List
from utils import mellanni_modules as mm

from scripts import size_match

from connectors import gcloud as gc
from connectors import gdrive as gd
from utils.mellanni_modules import week_number
from common import events, excluded_collections, user_folder
from ctk_gui.ctk_windows import PopupError
user_folder = os.path.join(user_folder,'dataset')
if not os.path.exists(user_folder):
    os.makedirs(user_folder)
import asyncio

START = "2025-01-01"
END = "2025-12-31"
MARKET = "US"
default_market_list = ["US", "CA", "GB", "UK", "MX", "FR", "DE", "IT", "ES"]
CHANNEL = None
LOCAL = True
SAVE = False
channels_mapping = {
    "US":"amazon.com",
    "CA":"amazon.ca",
    "GB":"amazon.co.uk",
    "UK":"amazon.co.uk",
    "MX":"amazon.com.mx",
    "FR":"amazon.fr",
    "DE":"amazon.de",
    "IT":"amazon.it",
    "ES":"amazon.es",
    "EU":"amazon.eu"
}

class Dataset:
    def __init__(self, start: str = START, end: str = END,
                 market: Literal["US", "CA", "GB", "UK" "MX", "FR", "DE", "IT", "ES", "*"] | List[str] = MARKET,
                 local_data: bool = LOCAL, save: bool = SAVE):
        self.client = gc.gcloud_connect()
        self.start = start
        self.end = end
        if isinstance(market, str):
            if market == '*':
                self.channel = '","'.join([value for value in channels_mapping.values()])
                self.market = '","'.join(default_market_list)
                self.market_list = default_market_list
            else:
                self.channel = channels_mapping[market.upper()]
                self.market = market.upper() if market.upper() not in ("GB", "UK") else '","'.join(("GB","UK"))
                self.market_list = [market]
        elif isinstance(market, list):
            if any(("GB" in market, "UK" in market)):
                market.extend(['GB','UK','EU'])
                market = list(set(market))
            self.channel = '","'.join([channels_mapping[key] for key in market])
            self.market = '","'.join(market)
            self.market_list = market
        self.local_data = local_data
        self.save = save
        self.fba_shipments = None
        self.orders = None
        self.functions = [
            self.pull_br_asin_data, self.pull_br_data, self.pull_order_data, self.pull_inventory_data,
            self.pull_inventory_history, self.pull_advertised_product_data,
            self.pull_purchased_product_data, self.pull_attribution_data, self.pull_dsp_data,
            self.pull_sba_data, self.pull_sbv_data, self.pull_sd_data, self.pull_promotions, self.pull_returns,
            self.pull_fees_dimensions, self.pull_warehouse, self.pull_changelog, self.pull_incoming,
            self.pull_cogs
        ]

    def __read_local__(self, file, *args, **kwargs):
        if not os.path.exists(file):
            PopupError(f"{file} not found!")
            raise BaseException('Local file not found!')
        try:
            result = pd.read_csv(file, **kwargs)
        except Exception as e:
            PopupError(f"{e} error")
            raise BaseException(f"Could not read from {file}")
        return result


    def pull_br_asin_data(self):
        "pulls sessions data (detailed) per asin for all products regardless of sales"
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'br_asin.csv'))
        else:
            query = f'''SELECT DATE(date) as date, childAsin as asin,
                        unitsOrdered, unitsOrderedB2B,
                        orderedProductSales, orderedProductSalesB2B,
                        browserSessions, browserSessionsB2B,
                        mobileAppSessions, mobileAppSessionsB2B,
                        sessions, sessionsB2B,
                        browserPageViews, browserPageViewsB2B,
                        mobileAppPageViews, mobileAppPageViewsB2B,
                        pageViews, pageViewsB2B, buyBoxPercentage as buyBox, buyBoxPercentageB2B as buyBoxB2B, country_code
                        FROM `reports.business_report_asin`
                        WHERE DATE(date) >= DATE("{self.start}") AND  DATE(date) <= DATE("{self.end}")
                        AND country_code IN ("{self.market}")
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'br_asin.csv'), index=False)
        self.br_asin = result

    def pull_br_data(self):
        "pulls sales data per sku. only those skus that had at least 1 sale are pulled"
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'br.csv'))
        else:
            query = f'''SELECT DATE(date) as date, sku, childAsin as asin,
                        unitsOrdered, unitsOrderedB2B,
                        orderedProductSales, orderedProductSalesB2B,
                        browserSessions, browserSessionsB2B,
                        mobileAppSessions, mobileAppSessionsB2B,
                        sessions, sessionsB2B,
                        browserPageViews, browserPageViewsB2B,
                        mobileAppPageViews, mobileAppPageViewsB2B,
                        pageViews, pageViewsB2B, buyBoxPercentage as buyBox, buyBoxPercentageB2B as buyBoxB2B, country_code
                        FROM `reports.business_report`
                        WHERE DATE(date) >= DATE("{self.start}") AND  DATE(date) <= DATE("{self.end}")
                        AND country_code IN ("{self.market}")
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'br.csv'), index=False)
        self.br = result

    def pull_order_data(self):
        "pulls data from order reports, converting datetime to pacific timezone"
        if self.local_data:
            result = pd.DataFrame()
            chunks = self.__read_local__(os.path.join(user_folder, 'orders.csv'), chunksize=100000)
            for chunk in chunks:
                result = pd.concat([result, chunk], ignore_index=True)

        else:
            query = f"""
                    SELECT
                    DATETIME(purchase_date, "America/Los_Angeles") as pacific_datetime,
                    DATE(purchase_date, "America/Los_Angeles") as pacific_date,
                    amazon_order_id,
                    sku,asin,quantity as units_sold,currency, item_price as sales, is_business_order,
                    ship_city, ship_state, ship_postal_code, ship_country, sales_channel
                    FROM `reports.all_orders`
                    WHERE (DATE(DATETIME(purchase_date, "America/Los_Angeles")) BETWEEN DATE("{self.start}") AND DATE("{self.end}"))
                    AND (LOWER(sales_channel) IN ("{self.channel}"))
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
            result = self.__read_local__(os.path.join(user_folder, 'inventory.csv'))
        else:
            result = pd.DataFrame()
            for marketplace in self.market_list:
                print(marketplace)
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
                            Recommended_ship_in_quantity, Recommended_ship_in_date,Inventory_Supply_at_FBA, Reserved_FC_Transfer, Reserved_FC_Processing,
                            Reserved_Customer_Order, total_days_of_supply_with_open_shipments, marketplace
                            FROM `reports.fba_inventory_planning`
                            WHERE DATE(snapshot_date) = LEAST(
                                (SELECT MAX(DATE(snapshot_date)) FROM `reports.fba_inventory_planning` WHERE marketplace = UPPER("{marketplace}")),DATE("{self.end}")
                            )
                            AND marketplace = UPPER("{marketplace}")
                            AND LOWER(condition) != "used"
                            '''
                temp_result:pd.DataFrame = self.client.query(query).to_dataframe()
                if len(temp_result)>0:
                    result = pd.concat([result, temp_result])
            if self.save:
                result.to_csv(os.path.join(user_folder, 'inventory.csv'), index=False)
        self.inventory = result

    def pull_inventory_history(self):
        "pulls inventory history (available) from the new 'fba inventory' report for the given period to identify isr"
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'inventory_history.csv'))
        else:
            query = f'''SELECT DATE(snapshot_date) AS date, sku, asin, available, Inventory_Supply_at_FBA, marketplace
                        FROM `reports.fba_inventory_planning`
                        WHERE DATE(snapshot_date) BETWEEN DATE("{self.start}") AND DATE("{self.end}")
                        AND marketplace IN ("{self.market}")
                        AND LOWER(condition) != "used"
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'inventory_history.csv'), index=False)
        self.inventory_history = result

    def pull_dictionary(self): # TODO add conditional to download different self.markets' dictionaries
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'dictionary.csv'))
        else:
            result = pd.DataFrame()
            dict_ids = {
                'US':('1zIHmbWcRRVyCTtuB9Atzam7IhAs8Ymx4','Dictionary.xlsx'),
                'UK':('1vt8UB2FeQp0RJimnCATI8OQt5N-bysx-', 'Dictionary_UK.xlsx'),
                'EU':('1uye8_FNxI11ZUOKnUYUfko1vqwpJVnMj','Dictionary_EU.xlsx'),
                'CA':('1ZijSZTqY1_5F307uMkdcneqTKIoNSsds','Dictionary_CA.xlsx')
                }
            for market, (folder_id, file_name) in dict_ids.items():
                dictionary_id = gd.find_file_id(folder_id=folder_id, filename=file_name, drive_id='0AMdx9NlXacARUk9PVA')
                temp:pd.DataFrame = pd.read_excel(gd.download_file(file_id=dictionary_id))
                temp['marketplace'] = market
                result = pd.concat([result, temp])
            
            result = result.dropna(subset='Collection')
            if self.save:
                result.to_csv(os.path.join(user_folder, 'dictionary.csv'), index=False)
        self.dictionary = result
        self.dictionary.columns = [x.strip().lower() for x in self.dictionary.columns]

    def pull_advertised_product_data(self):
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'advertised_product.csv'))
        else:
            query = f'''SELECT DATE(date) AS date, advertisedSku AS sku, advertisedAsin as asin,
                        SUM(clicks) AS clicks, SUM(impressions) as impressions, SUM(spend) AS spend,
                        SUM(unitsSoldSameSku14d) AS sameSkuUnits,
                        SUM(attributedSalesSameSku14d) as sameSkuSales, country_code
                        FROM `reports.AdvertisedProduct`
                        WHERE DATE(date) BETWEEN DATE("{self.start}") AND DATE("{self.end}")
                        AND UPPER(country_code) IN ("{self.market}")
                        GROUP BY date, country_code, advertisedSku, advertisedAsin ORDER BY date, advertisedSku, advertisedAsin
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'advertised_product.csv'), index=False)
        self.advertised_product = result

    def pull_purchased_product_data(self):
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'purchased_product.csv'))
        else:
            query = f'''SELECT DATE(date) AS date, advertisedSku AS sku, advertisedAsin as asin, purchasedAsin,
                        SUM(unitsSoldOtherSku14d) AS otherSkuUnits,
                        SUM(salesOtherSku14d) as otherSkuSales, country_code
                        FROM `reports.PurchasedProduct`
                        WHERE DATE(date) BETWEEN DATE("{self.start}") AND DATE("{self.end}")
                        AND UPPER(country_code) IN ("{self.market}")
                        GROUP BY date, country_code, advertisedSku, advertisedAsin, purchasedAsin ORDER BY date, advertisedSku, advertisedAsin, purchasedAsin
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'purchased_product.csv'), index=False)
        self.purchased_product = result

    def pull_attribution_data(self): # NOT product specific
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'attribution.csv'))
        else:
            query = f'''SELECT DATE(date) AS date,
                        SUM(unitsSold14d) AS specificUnitsSold, SUM(totalUnitsSold14d) AS totalUnitsSold,
                        SUM(attributedSales14d) as specificSales, SUM(totalAttributedSales14d) as totalSales, countryCode
                        FROM `reports.attribution`
                        WHERE DATE(date) BETWEEN DATE("{self.start}") AND DATE("{self.end}")
                        AND UPPER(countryCode) IN ("{self.market}")
                        GROUP BY date, countryCode ORDER BY date, countryCode
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'attribution.csv'), index=False)
        self.attribution = result

    def pull_dsp_data(self): # NOT product specific
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'dsp.csv'))
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

    def pull_sba_data(self): # NOT product specific
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'sba.csv'))
        else:
            query = f'''SELECT DATE(date) AS date,
                        SUM(cost) AS cost, SUM(clicks) AS clicks,
                        SUM(impressions) AS impressions, SUM(dpv14d) AS dpv14d,
                        SUM(unitsSold14d) AS unitsSold14d, SUM(attributedSales14d) AS attributedSales14d, country_code
                        FROM `reports.sponsored_brands_all`
                        WHERE DATE(date) BETWEEN DATE("{self.start}") AND DATE("{self.end}")
                        AND UPPER(country_code) IN ("{self.market}")
                        GROUP BY date, country_code ORDER BY date, country_code
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'sba.csv'), index=False)
        self.sba = result

    def pull_sbv_data(self): # NOT product specific
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'sbv.csv'))
        else:
            query = f'''SELECT DATE(date) AS date,
                        SUM(cost) AS cost, SUM(clicks) AS clicks,
                        SUM(impressions) AS impressions, SUM(dpv14d) AS dpv14d,
                        SUM(attributedUnitsOrderedNewToBrand14d) AS attributedUnitsOrderedNewToBrand14d,
                        SUM(attributedSales14d) AS attributedSales14d, country_code
                        FROM `reports.sponsored_brands_video`
                        WHERE DATE(date) BETWEEN DATE("{self.start}") AND DATE("{self.end}")
                        AND UPPER(country_code) IN ("{self.market}")
                        GROUP BY date, country_code ORDER BY date, country_code
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'sbv.csv'), index=False)
        self.sbv = result

    def pull_sd_data(self): # NOT product specific
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'sd.csv'))
        else:
            query = f'''SELECT DATE(date) AS date,
                        SUM(cost) AS cost, SUM(clicks) AS clicks,
                        SUM(impressions) AS impressions, SUM(dpv14d) AS dpv14d,
                        SUM(unitsSold14d) AS unitsSold14d, SUM(attributedSales14d) AS attributedSales14d, country_code
                        FROM `reports.sponsored_brands_all`
                        WHERE DATE(date) BETWEEN DATE("{self.start}") AND DATE("{self.end}")
                        AND UPPER(country_code) IN ("{self.market}")
                        GROUP BY date, country_code ORDER BY date, country_code
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'sd.csv'), index=False)
        self.sd = result

    def pull_fba_shipments_data(self):
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'fba_shipments.csv'))
        else:
            query = f"""
                    SELECT
                    DATE(purchase_date, "America/Los_Angeles") as pacific_date, amazon_order_id,
                    sku, shipment_item_id, quantity_shipped as units_sold, currency, item_price as sales, sales_channel
                    FROM `reports.shipments`
                    WHERE (DATE(DATETIME(purchase_date, "America/Los_Angeles")) BETWEEN DATE("{self.start}") AND DATE("{self.end}"))
                    AND (LOWER(sales_channel) IN ("{self.channel}"))
                    ORDER BY purchase_date, amazon_order_id, sku
                    """
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            if self.save:
                result.to_csv(os.path.join(user_folder, 'fba_shipments.csv'), index=False)
        self.fba_shipments = result

    def pull_promotions(self):
        "generates promotions data from the 'shipment_item_id' list obtained from fba_shipments report"
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'promotions.csv'))
        else:
            if not self.fba_shipments:
                self.pull_fba_shipments_data()
            shipment_item_ids = self.fba_shipments[['shipment_item_id','sku','sales_channel']].drop_duplicates()
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

    def pull_returns(self):
        "generates returns data from the 'amazon_order_is' list obtained from orders report"
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'returns.csv'))
        else:
            if not isinstance(self.orders, pd.DataFrame):
                self.pull_order_data()
            amazon_order_ids = pd.DataFrame(self.orders['amazon_order_id'].unique().tolist(), columns=['amazon_order_id'])
            pandas_gbq.to_gbq(amazon_order_ids, destination_table='auxillary_development.temp_order_ids', if_exists='replace')

            query = '''SELECT DATETIME(return_date, "America/Los_Angeles") as return_date,
                        order_id, sku, asin, quantity, detailed_disposition, reason, status, customer_comments, country_code
                        FROM `reports.fba_returns`
                        WHERE order_id IN
                            (SELECT amazon_order_id 
                            FROM `auxillary_development.temp_order_ids`)
                        '''
            result:pd.DataFrame = self.client.query(query).to_dataframe()
            self.client.query('DROP TABLE `auxillary_development.temp_order_ids`')
            if self.save:
                result.to_csv(os.path.join(user_folder, 'returns.csv'), index=False)
        self.returns = result

    def pull_fees_dimensions(self):
        if self.local_data:
            fees = self.__read_local__(os.path.join(user_folder, 'fees.csv'))
        else:
            fees = size_match.main(out=False)
            if self.save:
                fees.to_csv(os.path.join(user_folder, 'fees.csv'), index=False)
        self.fees = fees

    def pull_warehouse(self):
        "pulls data from sellercloud, aggregating inventory stock at warehouse"
        if self.local_data:
            warehouse = self.__read_local__(os.path.join(user_folder, 'warehouse.csv'))
        else:
            query = f"""
                    SELECT date, ProductID as sku, QtyAvailable, QtyPhysical, BinType, Sellable, BinName
                    FROM `mellanni-project-da.sellercloud.inventory_bins`
                    WHERE DATE(date)=(
                        SELECT DATE(MAX(date)) FROM `mellanni-project-da.sellercloud.inventory_bins`
                        WHERE DATE(date) <= DATE("{self.end}")
                    )
                    """
            result:pd.DataFrame = self.client.query(query).to_dataframe()

            # split warehouse inventory by sellable and receiving
            sellable = result.query('Sellable == True & BinType != "Picking" & ~BinName.str.startswith("DS")')
            if len(sellable)>0:
                sellable = sellable.pivot_table(
                    values = ['QtyAvailable', 'QtyPhysical', 'date'],
                    index = 'sku',
                    aggfunc = {'QtyAvailable':'sum', 'QtyPhysical':'sum', 'date':'max'}
                    ).reset_index()
            sellable = sellable.rename(columns = {'QtyAvailable':'total_wh'})
            
            receiving = result.query('Sellable == False & BinType == "Receiving"')
            if len(receiving) > 0:
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
        # self.warehouse = result

    def pull_changelog(self): # TODO add conditional to download sku_changelogs for different markets
        changelog_markets = [x for x in self.market_list if x not in ("GB","MX")]
        tables = {x:f'sku_changelog_{x.lower()}' if x != "US" else 'sku_changelog' for x in changelog_markets}
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'changelog.csv'))
        else:
            result = pd.DataFrame()
            for country_code, table in tables.items():
                query = f'''SELECT DATE(date) AS date, sku, change_type, notes,
                            FROM `auxillary_development.{table}`
                            WHERE DATE(date) BETWEEN DATE("{self.start}") AND DATE("{self.end}")
                            '''
                temp_result:pd.DataFrame = self.client.query(query).to_dataframe()
                temp_result['country_code'] = country_code
                result = pd.concat([result, temp_result])
            if self.save:
                result.to_csv(os.path.join(user_folder, 'changelog.csv'), index=False)
        self.changelog = result

    def pull_incoming(self):
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'incoming.csv'))
        else:
            query = '''SELECT ExpectedDeliveryDate, Items FROM `mellanni-project-da.sellercloud.purchase_orders`
                        WHERE DATE(ExpectedDeliveryDate) >= DATE(CURRENT_DATE())
                        '''
            nested:pd.DataFrame = self.client.query(query).to_dataframe()
            unnested = pd.DataFrame()
            for i, df in nested.iterrows():
                for row in df.Items:
                    temp = pd.DataFrame(row, index=[i])
                    temp['eta'] = df.ExpectedDeliveryDate
                    unnested = pd.concat([unnested, temp])
            unnested['eta'] = pd.to_datetime(unnested['eta'])
            unnested['year'] = unnested['eta'].dt.year
            unnested['week'] = unnested['eta'].apply(week_number)
            unnested['year-week'] = unnested['year'].astype(str) + "-" + unnested['week'].astype(str)
            unnested['eta'] = unnested['eta'].dt.date

            result = unnested.pivot_table(
                values = ['QtyOrdered','eta'],
                index = ['year-week','SKU'],
                aggfunc = {'QtyOrdered':'sum','eta':'max'}
                ).reset_index()
            result = result.rename(columns={'SKU':'sku'}).sort_values('eta', ascending=False)
                        
            if self.save:
                result.to_csv(os.path.join(user_folder, 'incoming.csv'), index=False)
        self.incoming = result
            
    def pull_pricing(self):
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'pricing.csv'))
        else:
            result = gd.download_gspread(spreadsheet_id='1iB1CmY_XdOVA4FvLMPeiEGEcxiVEH3Bgp4FJs1iNmQs', sheet_id=0)
            result = result[['SKU','ASIN','Full price','Sale price','Discount','Date of last event (price change)','Status']]
            if self.save:
                result.to_csv(os.path.join(user_folder, 'pricing.csv'), index=False)
        self.pricing = result
        self.pricing.columns = [x.strip().lower() for x in self.pricing.columns]

    def pull_cogs(self):
        "pulls data from product cost report"
        if self.local_data:
            result = self.__read_local__(os.path.join(user_folder, 'cogs.csv'))
        else:
            channels = list(set([channels_mapping[x] for x in self.market_list]))
            result = pd.DataFrame()
            for channel in channels:
                query = f"""
                        SELECT sku, pc_value_usd as product_cost, pc_value_local as product_cost_local, start_date as date, channel
                        FROM `ds_for_bi.product_cost_hist`
                        WHERE DATE(start_date) = (
                            SELECT MAX(DATE(start_date)) FROM `ds_for_bi.product_cost_hist` WHERE DATE(start_date)<=DATE("{self.end}") AND (LOWER(channel) = "{channel}")
                        )
                        AND (LOWER(channel) = "{channel}")
                        """
                temp_result:pd.DataFrame = self.client.query(query).to_dataframe()
                if len(temp_result)>0:
                    result = pd.concat([result, temp_result])
            if self.save:
                result.to_csv(os.path.join(user_folder, 'cogs.csv'), index=False)
        self.cogs = result

    async def _pull_all_data(self):

        async def run_function(func):
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, func)

        tasks = [run_function(func) for func in self.functions]
        await asyncio.gather(*tasks)
    
    def query(self):
        asyncio.run(self._pull_all_data())
        self.pull_dictionary()
        self.pull_pricing()

    def query_sync(self):
        for func in self.functions:
            func()
        self.pull_dictionary()
        self.pull_pricing()
