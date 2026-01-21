from connectors import gdrive as gd
from common import user_folder
from utils import mellanni_modules as mm
import pandas as pd
import datetime
import re


DRIVE_ID = "0AMdx9NlXacARUk9PVA"
LOST_SALES_FOLDER = "1pgnXoZO48hog7ciKNEHu1U7r2I7kGqf4"


def get_dates(today:datetime.date):
    start_date = today - pd.Timedelta(days = today.weekday()+8)
    end_date = today - pd.Timedelta(days = today.weekday()+2)
    return pd.date_range(start=start_date, end = end_date)

def main(dates):
    files = gd.list_files_in_folder(folder_id=LOST_SALES_FOLDER, drive_id=DRIVE_ID)
    excel_files = [x for x in files if x.endswith(".xlsx")]
    files_to_use = {excel_file: files[excel_file] for excel_file in excel_files}


    weekly_files = []
    for file in files_to_use:
        file_date = re.search(r"(\d{4}-\d{2}-\d{2})", file)
        if file_date:
            date = file_date.group()
            if date in dates:
                weekly_files.append(file)

    if len(weekly_files) == 0:
        raise BaseException("No files found for the specified week.")

    total_df = pd.DataFrame()
    for file in weekly_files:
        print(f"Processing file: {file}")
        file_id = files_to_use[file]
        df = pd.read_excel(gd.download_file(file_id["id"]))
        total_df = pd.concat([total_df, df])
    total_df["asin lost sales"] = total_df["lost sales min"] > 0

    result = total_df.pivot_table(
        values=["lost sales min", "lost sales max", "asin lost sales"],
        index=["date"],
        aggfunc="sum",
    ).reset_index()
    mm.export_to_excel(
        dfs=[result],
        sheet_names=["Lost Sales Summary"],
        filename="lost_sales_summary.xlsx",
        out_folder=user_folder,
    )
    mm.open_file_folder(user_folder)


if __name__ == "__main__":
    today = pd.to_datetime('today').date()
    # dates = pd.date_range("2026-01-11", "2026-01-17")
    dates = get_dates(today)
    print(f"Calculating lost sales for {dates[0].date()} - {dates[-1].date()}")

    main(dates = dates)
    print("All done")