from connectors import gdrive as gd
from connectors import gcloud as gc
import pandas as pd
from utils import mellanni_modules as mm
from common import user_folder
from tkinter.messagebox import showinfo, showerror

DICTIONARY_ID = "1Y4XhSBCXqmEVHHOnugEpzZZ3NQ5ZRGOlp-AsTE0KmRE"


def get_dictionary():
    dictionary = gd.download_gspread(spreadsheet_id=DICTIONARY_ID, sheet_id=359795095)
    index_cols = ["Bundle SKU", "Bundle ASIN"]
    dictionary = dictionary.drop_duplicates(index_cols)
    sku_cols = [x for x in dictionary.columns if "Included SKU" in x]
    asin_cols = [x for x in dictionary.columns if "Included ASIN" in x]
    dictionary_stacked = pd.lreshape(
        data=dictionary.loc[:, index_cols + sku_cols + asin_cols],
        groups={"SKU": sku_cols, "ASIN": asin_cols},
    ).reset_index(drop=True)
    dictionary_stacked = dictionary_stacked.dropna(subset="SKU")
    dictionary_stacked = dictionary_stacked[dictionary_stacked["SKU"] != ""]
    return dictionary_stacked


def get_amazon_inventory(marketplace="US", num_days=3):
    inventory = None
    query = f"""
        WITH RecentSnapshots AS (
            SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku ORDER BY snapshot_date DESC) AS rn
        FROM 
            `mellanni-project-da.reports.fba_inventory_planning`
        WHERE 
            marketplace = "{marketplace}"
            AND LOWER(condition) = "new"
            AND snapshot_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {num_days} DAY)
            )            
        SELECT DATE(snapshot_date) as snapshot_date, sku as SKU, available
            FROM RecentSnapshots
        WHERE rn=1
        """
    with gc.gcloud_connect() as client:
        inventory = client.query(query).to_dataframe()

    return inventory


def combine_files(dictionary_stacked, inventory):
    combined = pd.merge(dictionary_stacked, inventory, how="left", on="SKU")
    combined["available"] = combined["available"].fillna(0)
    combined["SKU inventory"] = (
        combined["SKU"].astype(str) + ": " + combined["available"].astype(str)
    )
    summary = combined.pivot_table(
        index=["Bundle SKU", "Bundle ASIN"],
        aggfunc={"available": "min", "SKU inventory": lambda x: ", ".join(x.unique())},
    ).reset_index()
    return summary


def main():
    try:
        dictionary_stacked = get_dictionary()
        inventory = get_amazon_inventory()
        combined = combine_files(dictionary_stacked, inventory)
        mm.export_to_excel(
            dfs=[combined],
            sheet_names=["Bundle inventory"],
            filename="Bundle inventory.xlsx",
            out_folder=user_folder,
        )
        showinfo(
            title="Success", message=f"File has been saved to your {user_folder} folder"
        )
        mm.open_file_folder(user_folder)
    except Exception as e:
        showerror(title="Error", message=f"Error: {e}")
