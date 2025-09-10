from customtkinter import filedialog
from common import user_folder
from utils import mellanni_modules as mm
import os
import pandas as pd


def main():
    folder = filedialog.askdirectory(
        title="Select folder with images", initialdir=user_folder
    )
    if not folder:
        return
    files = os.listdir(folder)

    sorted_files = sorted(
        files,
        key=lambda x: (
            os.path.splitext(x)[0].split("_")[1],
            os.path.splitext(x)[0].split("_")[2],
            os.path.splitext(x)[0].split("_")[3],
        ),
    )

    products = {}
    colors = {}
    sizes = {}
    sorted_df = pd.DataFrame()

    for file in sorted_files:
        product, color, size, position, _, market = os.path.splitext(file)[0].split("_")
        temp_row = pd.DataFrame(
            [
                [
                    product,
                    color,
                    size,
                    market,
                    _,
                    int(position),
                    os.path.join(folder, file),
                ]
            ],
            columns=["product", "color", "size", "market", "props", "position", "path"],
        )
        sorted_df = pd.concat([sorted_df, temp_row])
        if product not in products:
            products[product] = 1
        else:
            products[product] += 1
        if color not in colors:
            colors[color] = 1
        else:
            colors[color] += 1
        if size not in sizes:
            sizes[size] = 1
        else:
            sizes[size] += 1

    df_products = (
        pd.DataFrame.from_dict(products, orient="index", columns=["count"])
        .reset_index()
        .rename(columns={"index": "product"})
    )
    df_colors = (
        pd.DataFrame.from_dict(colors, orient="index", columns=["count"])
        .reset_index()
        .rename(columns={"index": "color"})
    )
    df_sizes = (
        pd.DataFrame.from_dict(sizes, orient="index", columns=["count"])
        .reset_index()
        .rename(columns={"index": "size"})
    )

    total = pd.concat([df_products, df_colors, df_sizes])
    total = total[["product", "size", "color", "count"]]
    with pd.ExcelWriter(
        os.path.join(user_folder, "images.xlsx"), engine="xlsxwriter"
    ) as writer:
        total.to_excel(writer, sheet_name="check", index=False)
        sorted_df.to_excel(writer, sheet_name="sorted", index=False)

    mm.open_file_folder(user_folder)


if __name__ == "__main__":
    main()
