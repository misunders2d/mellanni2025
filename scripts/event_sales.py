import customtkinter as ctk
import pandas as pd
from common import user_folder
from ctk_gui.ctk_windows import PopupError, PopupWarning


def main():
    try:
        PopupWarning(
            "First, download the `All orders` report from Seller Central.\n\n https://sellercentral.amazon.com/reportcentral/FlatFileAllOrdersReport/1"
        )
        path = ctk.filedialog.askopenfilename(
            title="All orders file", initialdir=user_folder
        )

        file = pd.read_csv(path, sep="\t")
        file = file[file["sales-channel"] == "Amazon.com"]
        file["purchase-date"] = pd.to_datetime(file["purchase-date"].copy())

        file["pacific-datetime"] = file["purchase-date"].dt.tz_convert("US/Pacific")
        file["pacific-date"] = file["pacific-datetime"].dt.date

        result = file.pivot_table(
            values="quantity", index="asin", columns="pacific-date", aggfunc="sum"
        ).reset_index()

        result.to_clipboard(index=False)  # sku sales
        PopupWarning(
            "Event sales are copied to clipboard. You can now paste them to any Excel spreadsheet."
        )
        
    except Exception as e:
        PopupError(str(e))


if __name__ == "__main__":
    main()
