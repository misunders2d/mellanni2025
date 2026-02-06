import easygui
import pandas as pd
from common import user_folder
from connectors import gcloud as gc
from connectors import gdrive as gd
from utils import mellanni_modules as mm

DICTIONARY_ID = "1Y4XhSBCXqmEVHHOnugEpzZZ3NQ5ZRGOlp-AsTE0KmRE"


def get_bundle_sales_report():
    bundle_sales_obj = gd.download_file(file_id="1nbHkhkWwQaa_I_CnJuxYzTmAbWZzRB2q")
    bundle_sales = pd.read_excel(
        bundle_sales_obj, sheet_name="Total", usecols=["BUNDLE_ASIN", "BUNDLES_SOLD"]
    )
    bundle_sales = bundle_sales.rename(columns={"BUNDLE_ASIN": "Bundle ASIN"})
    return bundle_sales


def get_dictionary():
    dictionary = gd.download_gspread(spreadsheet_id=DICTIONARY_ID, sheet_id=359795095)
    index_cols = ["Bundle SKU", "Bundle ASIN"]
    dictionary = dictionary.drop_duplicates(index_cols)

    sku_cols = [col for col in dictionary.columns if "Included SKU" in col]
    asin_cols = [x for x in dictionary.columns if "Included ASIN" in x]

    dictionary = dictionary.loc[:, index_cols + sku_cols + asin_cols]

    dictionary_stacked = pd.lreshape(
        data=dictionary,
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


def combine_files(dictionary_stacked, inventory, bundle_sales):
    combined = pd.merge(
        dictionary_stacked, inventory, how="left", on="SKU", validate="m:1"
    )
    combined["available"] = combined["available"].fillna(0)
    combined["snapshot_date"] = combined["snapshot_date"].astype(str).fillna(0)

    combined["SKU inventory"] = (
        combined["SKU"].astype(str) + ": " + combined["available"].astype(str)
    )
    summary = combined.pivot_table(
        index=["Bundle SKU", "Bundle ASIN"],
        aggfunc={
            "available": "min",
            "SKU inventory": lambda x: ", ".join(x.unique()),
            "snapshot_date": lambda x: ", ".join(sorted(x.unique())),
        },
    ).reset_index()
    summary_total = pd.merge(summary, bundle_sales, how="left", on="Bundle ASIN")
    return summary_total


def main():
    try:
        bundle_sales = get_bundle_sales_report()
        dictionary_stacked = get_dictionary()
        inventory = get_amazon_inventory()
        combined = combine_files(dictionary_stacked, inventory, bundle_sales)
        combined = combined.sort_values(
            ["BUNDLES_SOLD", "available"], ascending=[False, True]
        )
        mm.export_to_excel(
            dfs=[combined],
            sheet_names=["Bundle inventory"],
            filename="Bundle inventory.xlsx",
            out_folder=user_folder,
        )
        easygui.msgbox(
            title="Success",
            msg=f"File has been saved to your {user_folder} folder",
        )
        mm.open_file_folder(user_folder)
    except Exception as e:
        easygui.exceptionbox(title="Error", msg=f"Error: {e}")


if __name__ == "__main__":
    main()
