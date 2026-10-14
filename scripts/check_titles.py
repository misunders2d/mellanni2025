from utils import mellanni_modules as mm
import os
import pandas as pd
import customtkinter as ctk
from concurrent.futures import ThreadPoolExecutor
import re

from common import user_folder
from connectors import gcloud as gc

output = user_folder


class TitleChecker(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.geometry("400x300")
        self.title("Title checker")
        self.executor = ThreadPoolExecutor()
        self.client = gc.gcloud_connect()
        self.file_dir = None
        self.market = ctk.StringVar(self, value="US")
        self.market_radio_us = ctk.CTkRadioButton(
            self, text="US", value="US", variable=self.market
        )
        self.market_radio_us.pack(pady=10)

        self.market_radio_ca = ctk.CTkRadioButton(
            self, text="CA", value="CA", variable=self.market
        )
        self.market_radio_ca.pack(pady=10)

        self.folder_label = ctk.CTkLabel(self, text="No folder selected")
        self.folder_label.pack(pady=10)

        self.folder_button = ctk.CTkButton(
            self, text="1. Select folder with flat files", command=self.select_folder
        )
        self.folder_button.pack(pady=10)

        self.ok_button = ctk.CTkButton(self, text="2. GO", command=self.start)
        self.ok_button.pack(pady=10)

        self.progresbar = ctk.CTkProgressBar(self, mode="indeterminate")
        self.progresbar.pack(pady=10)

        self.warning_label = ctk.CTkLabel(self, text="")
        self.warning_label.pack(pady=10)

    def start(self):
        self.executor.submit(self.process_files)
        self.progresbar.start()

    def process_files(self):
        try:
            inv_file = self.read_inventory()
            full_list = self.read_flat_files()
            dictionary = self.read_dictionary()
            if not full_list is None:
                mismatch = self.compare_titles(full_list, inv_file)
                if len(mismatch) > 0:
                    mismatch = pd.merge(mismatch, dictionary, how="left", on="sku")
                    mm.export_to_excel(
                        [mismatch],
                        ["mismatched_titles"],
                        "title_check.xlsx",
                        out_folder=user_folder,
                    )
                    mm.open_file_folder(user_folder)
                else:
                    self.warning_label.configure(text="No mismatch found")
        except Exception as e:
            print(e)
            self.warning_label.configure(text=f"Check failed: {e}")
        self.progresbar.stop()

    @staticmethod
    def compare_titles(full_list, inv_file):
        result = pd.merge(full_list, inv_file, how="left", on="sku")
        result["sku"] = result["sku"].astype(str)
        result = result[~result["sku"].str.lower().str.contains("parent")].copy()
        result["reasons"] = ""
        for flat_column, listing_column, field in [
            ("Flat File title", "Listing title", "Title"),
            ("Flat File item highlights", "Listing item highlights", "Item highlights"),
        ]:
            flat = result[flat_column].fillna("")
            listing = result[listing_column].fillna("")
            available = result[listing_column].notna()
            conditions_reasons = [
                ((flat != listing) & available, f"{field} mismatch"),
                (
                    flat.str.contains("  ", na=False),
                    f"double space in flat file {field.lower()}",
                ),
                (
                    listing.str.contains("  ", na=False),
                    f"double space on Amazon {field.lower()}",
                ),
                (
                    flat.str.contains(r"(?<!\s)\(", regex=True, na=False),
                    f"no space before '(' in flat file {field.lower()}",
                ),
                (
                    listing.str.contains(r"(?<!\s)\(", regex=True, na=False),
                    f"no space before '(' on Amazon {field.lower()}",
                ),
            ]
            for condition, reason in conditions_reasons:
                result.loc[condition, "reasons"] += reason + "; "
        unavailable = (
            result["Listing title"].notna()
            & result["Listing item highlights"].isna()
            & result["Flat File item highlights"].fillna("").ne("")
        )
        result.loc[unavailable, "reasons"] += (
            "Item highlights unavailable in inventory report; "
        )
        return result[result["reasons"] != ""]

    def select_folder(self):
        self.file_dir = ctk.filedialog.askdirectory(initialdir=user_folder)
        self.folder_label.configure(text=self.file_dir)

    def read_flat_files(self):
        if not self.file_dir:
            return
        files = [
            x
            for x in os.listdir(self.file_dir)
            if ".xlsm" in os.path.basename(x) and "~" not in x
        ]
        if len(files) == 0:
            print("No files found, exiting")
        self.warning_label.configure(text=f"Reading {len(files)} flat files")
        full_list = pd.DataFrame()
        for file in files:
            with pd.ExcelFile(os.path.join(self.file_dir, file)) as workbook:
                header = pd.read_excel(
                    workbook, sheet_name="Template", header=None, nrows=5
                )
                columns = {
                    str(column): str(column).split("[")[0].split("#")[0]
                    for column in header.iloc[4].dropna()
                }
                names = {
                    "contribution_sku": "sku",
                    "item_name": "Flat File title",
                    "title_differentiation": "Flat File item highlights",
                }
                rename = {
                    column: names[attribute]
                    for column, attribute in columns.items()
                    if attribute in names
                }
                if not {"sku", "Flat File title"}.issubset(rename.values()):
                    raise ValueError(f"Missing SKU or item_name column in {file}")
                temp_file = pd.read_excel(
                    workbook,
                    sheet_name="Template",
                    skiprows=4,
                    usecols=list(rename),
                    dtype=str,
                ).rename(columns=rename)
                # Amazon's dataRow excludes examples and preference-profile rows.
                data_row = re.search(
                    r"(?:^|&)dataRow=(\d+)(?:&|$)", str(header.iat[0, 0])
                )
                first_data_row = int(data_row.group(1)) if data_row else 7
                temp_file = temp_file.iloc[first_data_row - 6 :].copy()
                temp_file = temp_file[temp_file["sku"].notna()]
                if "Flat File item highlights" not in temp_file:
                    temp_file["Flat File item highlights"] = ""
                full_list = pd.concat([full_list, temp_file], ignore_index=True)
        duplicates = full_list["sku"].duplicated().sum()
        if duplicates > 0:
            self.warning_label.configure(
                text=f"There are {duplicates} duplicates in the folder, please check."
            )
        return full_list

    def read_inventory(self):
        self.warning_label.configure(text="Reading inventory file")
        market = self.market.get()
        query = f"""SELECT sku, product_name
                    FROM `reports.fba_inventory_planning`
                    WHERE marketplace = "{market}"
                    AND snapshot_date = (
                        SELECT MAX(snapshot_date) FROM `reports.fba_inventory_planning`
                        WHERE marketplace = "{market}"
                    )"""
        inv_file: pd.DataFrame = self.client.query(query).to_dataframe()
        parts = inv_file["product_name"].astype("string").str.partition("|")
        inv_file["Listing title"] = parts[0].str.rstrip()
        # A legacy title without a separator does not prove highlights are absent.
        inv_file["Listing item highlights"] = parts[2].str.lstrip().where(parts[1] == "|")
        inv_file = inv_file.drop(columns="product_name")
        return inv_file

    def read_dictionary(self):
        self.warning_label.configure(text="Reading dictionary")
        query = "SELECT sku, collection FROM `auxillary_development.dictionary`"
        client = gc.gcloud_connect()
        dictionary = client.query(query).to_dataframe()
        return dictionary


def main():
    app = TitleChecker()
    app.mainloop()


if __name__ == "__main__":
    main()
