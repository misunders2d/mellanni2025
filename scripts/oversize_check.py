from tkinter import filedialog
import pandas as pd
from common import user_folder
from utils import size_match, mellanni_modules as mm
from concurrent.futures import ThreadPoolExecutor


def read_fees_file(marketplace="US"):
    file_path = filedialog.askopenfilename(
        initialdir=user_folder,
        title="Select Amazon Fee Preview file",
        filetypes=(("csv files", ".csv"),),
    )
    encodings = ("cp1251", "utf-8")
    file = None
    for encoding in encodings:
        file = pd.read_csv(file_path, encoding=encoding)
        if file is not None:
            break
    if file is None:
        raise BaseException("Could not read file")
    sku_col = [x for x in file.columns if "sku" in x][0]
    file = file.rename(columns={sku_col: "sku"})
    columns = file.columns
    if any(
        [x not in columns for x in ("longest-side", "median-side", "shortest-side")]
    ):
        raise BaseException("Wrong file submitted")
    if marketplace:
        file = file[file["amazon-store"] == marketplace]
    return file


def combine_files(dimensions, amz_fees):
    result = pd.merge(
        dimensions, amz_fees, how="right", on=["sku", "asin"], validate="1:1"
    )
    result["problem"] = (
        result["product-size-tier"].str.lower().str.contains("bulky")
    ) & (~result["size_tier"].str.lower().str.contains("bulky", na=False))
    return result


def main():

    with ThreadPoolExecutor() as executor:
        amazon_fees_future = executor.submit(read_fees_file)
        dimensions_future = executor.submit(size_match.main, False)

    dimensions = dimensions_future.result()
    amz_fees = amazon_fees_future.result()
    result = combine_files(dimensions, amz_fees)
    mm.export_to_excel([result], ["oversize"], "oversize.xlsx", user_folder)
    mm.open_file_folder(user_folder)


if __name__ == "__main__":
    main()
