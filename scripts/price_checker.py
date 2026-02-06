import threading
from concurrent.futures import ThreadPoolExecutor

import customtkinter as ctk
import numpy as np
import pandas as pd
from common import excluded_collections, user_folder
from connectors import gdrive as dm
from connectors.gcloud import gcloud_connect
from utils import mellanni_modules as mm

BQ_BUSINESS_REPORT: str = "reports.business_report"
# BQ_DICTIONARY: str = "auxillary_development.dictionary"
BQ_ORDERS: str = "reports.all_orders"
FBA_INVENTORY: str = "reports.fba_inventory_planning"
SALE_FILE: str = "1iB1CmY_XdOVA4FvLMPeiEGEcxiVEH3Bgp4FJs1iNmQs"
# PRICELIST_ID: str = "1VGZ5VGsQiYgX9X6PxrRREj265gMzVQu9UQMKnT_014o"
EVENT_FILE: str = "1XcrMgklKRvElCb8vZI5r6j0P9ha4wmlPZdpU9MWq7QA"
# DICTIONARY_FILENAME: str = "Dictionary.xlsx"
DICTIONARY_FILE_ID: str = "1Y4XhSBCXqmEVHHOnugEpzZZ3NQ5ZRGOlp-AsTE0KmRE"
marketplace: str = "US"


class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.geometry("400x350")
        self.title("Pricelist checker")

        self.client = gcloud_connect()
        self.executor = ThreadPoolExecutor()

        self.result_files = {}

        self.print_area = ctk.CTkTextbox(self, width=380)
        self.print_area.grid(row=0, column=0, columnspan=2, pady=10, padx=10)

        self.progress = ctk.CTkProgressBar(self, mode="indeterminate")
        self.progress.grid(row=1, column=0, columnspan=2, pady=10, padx=10, sticky="ew")

        self.inv_days = ctk.CTkEntry(self, placeholder_text="3", width=40)
        self.inv_days.grid(row=2, column=0, pady=10, padx=10)
        self.inv_days.insert(0, "3")

        self.inv_days_label = ctk.CTkLabel(
            self, text="How many days of inventory to account?"
        )
        self.inv_days_label.grid(row=2, column=1, pady=10, padx=10)

        self.button = ctk.CTkButton(self, text="GO", command=self.process_prices)
        self.button.grid(row=3, column=0, pady=10, padx=10)

        self.custom_file_checkbox = ctk.CTkCheckBox(
            self, text="Use .csv inventory file instead"
        )
        self.custom_file_checkbox.grid(row=3, column=1, pady=10, padx=10)

    def download_sale_file(self):
        # get information from pricing (sales) file
        sale_file = dm.download_gspread(spreadsheet_id=SALE_FILE)
        sale_file = sale_file[["SKU", "Full price", "Sale price", "Status"]]
        self.result_files["sale_file"] = sale_file
        self.print_area.insert(ctk.END, text="Downloaded sale file\n")
        return

    def download_fba_inventory(self):
        if self.custom_file_checkbox.get():
            fba_inv_filename = ctk.filedialog.askopenfilename(
                title="Select FBA Inventory.csv file", initialdir=user_folder
            )
            fba_inv = pd.read_csv(
                fba_inv_filename,
                usecols=[
                    "snapshot-date",
                    "sku",
                    "asin",
                    "available",
                    "your-price",
                    "sales-price",
                ],
            )
            fba_inv = fba_inv.rename(
                columns={
                    "snapshot-date": "snapshot_date",
                    "sku": "SKU",
                    "your-price": "your_price",
                    "sales-price": "sales_price",
                }
            )
        else:
            try:
                num_days = int(self.inv_days.get())
            except ValueError:
                self.print_area.insert(
                    ctk.END, text="Inventory days MUST be an INTEGER!"
                )
                raise BaseException(
                    f"Inventory days MUST be an INTEGER!, you entered `{self.inv_days.get()}`"
                )
            fba_query = f"""
            WITH RecentSnapshots AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY sku ORDER BY snapshot_date DESC) AS rn
            FROM 
                `mellanni-project-da.{FBA_INVENTORY}`
            WHERE 
                marketplace = "{marketplace}"
                AND LOWER(condition) = "new"
                AND snapshot_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {num_days} DAY)
            )            
            SELECT DATE(snapshot_date) as snapshot_date, sku as SKU, asin, available, your_price, sales_price
            FROM RecentSnapshots
            WHERE rn=1
            """
            fba_inv = self.client.query(fba_query).to_dataframe()
        self.result_files["fba_inventory"] = fba_inv
        self.print_area.insert(ctk.END, text="fba file downloaded\n")
        return

    def download_dictionary(self):
        dictionary_spreadsheet = dm.download_gspread(
            spreadsheet_id=DICTIONARY_FILE_ID, sheet_id="449289593"
        )

        dictionary = dictionary_spreadsheet[
            [
                "SKU",
                "ASIN",
                "Collection",
                "Sub-collection",
                "Size Map",
                "Color",
                "Standard price",
                "MSRP",
            ]
        ]
        dictionary = dictionary[~dictionary["Collection"].isin(excluded_collections)]
        del dictionary["Collection"]
        dictionary = dictionary.rename(
            columns={"Sub-collection": "collection", "Size Map": "size"}
        )
        wrong_sub_cols = [
            "1800 Bed Sheet Set - Striped",
            "1800 Bed Sheet Set - Printed",
            "1800 Bed Sheet Set - Solid - White",
            "1800 Bed Sheet Set - Solid",
            "1800 Bed Sheet Set - Solid - Light Gray",
            "1800 Bed Sheet Set - Solid - Gray",
            "1800 Bed Sheet Set - Solid - RV Sheets",
        ]
        dictionary.loc[dictionary["collection"].isin(wrong_sub_cols), "collection"] = (
            "1800 Bed Sheets"
        )
        self.result_files["dictionary"] = dictionary
        self.print_area.insert(ctk.END, text="dictionary downloaded\n")
        return

    def merge_files(self, dictionary, fba_inventory, sale_file, event_file):
        self.print_area.insert(ctk.END, text="Merging files\n")

        price_check = pd.merge(
            dictionary, fba_inventory, how="outer", on="SKU", validate="1:1"
        )
        price_check = pd.merge(
            price_check, sale_file, how="outer", on="SKU", validate="1:1"
        )
        return price_check

    def process_file(self, price_check):
        self.print_area.insert(ctk.END, text="processing file\n")
        price_check_refined = price_check.copy()
        price_check_refined.loc[
            price_check_refined["Status"].isin(["Selling", "TEST"]),
            "Target current price",
        ] = price_check_refined["Sale price"]
        price_check_refined.loc[
            ~price_check_refined["Status"].isin(["Selling", "TEST"]),
            "Target current price",
        ] = price_check_refined["Full price"]

        str_columns = [
            "your_price",
            "sales_price",
            "Full price",
            "Sale price",
            "Target current price",
        ]
        for str_column in str_columns:
            self.str_to_float(price_check_refined, str_column)
        price_check_refined.loc[
            price_check_refined["Status"].isin(["Selling", "TEST"]), "Price_diff"
        ] = (
            price_check_refined["Target current price"]
            - price_check_refined["your_price"]
        )
        price_check_refined.loc[
            ~price_check_refined["Status"].isin(["Selling", "TEST"]), "Price_diff"
        ] = (
            price_check_refined["Target current price"]
            - price_check_refined["your_price"]
        )
        return price_check_refined

    def str_to_float(self, df, column_name):
        try:
            df[column_name] = df[column_name].str.replace("$", "").replace("", np.nan)
        except AttributeError:
            pass
        except Exception as e:
            print(e, column_name)
        df[column_name] = df[column_name].astype(float)

    def export_to_excel(self, price_check_refined):
        mm.export_to_excel(
            dfs=[price_check_refined],
            sheet_names=["price check"],
            filename="Pricelist checker.xlsx",
            out_folder=user_folder,
        )
        mm.open_file_folder(user_folder)

    def process_prices(self):
        self.print_area.delete(0.0, ctk.END)
        self.executor.submit(self.main)

    def main(self):
        self.progress.start()
        threads = [
            threading.Thread(target=self.download_sale_file, args=()),
            threading.Thread(target=self.download_fba_inventory, args=()),
            threading.Thread(target=self.download_dictionary, args=()),
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        sale_file = self.result_files.get("sale_file")
        event_file = self.result_files.get("event_file")
        fba_inventory = self.result_files.get("fba_inventory")
        dictionary = self.result_files.get("dictionary")

        price_check = self.merge_files(dictionary, fba_inventory, sale_file, event_file)
        price_check_refined = self.process_file(price_check)
        price_check_refined["ASIN"] = price_check_refined["ASIN"].combine_first(
            price_check_refined["asin"]
        )
        del price_check_refined["asin"]
        _ = self.export_to_excel(price_check_refined)
        self.progress.stop()


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
