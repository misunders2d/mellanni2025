from utils import size_match
import pandas as pd
from common import user_folder
from utils import mellanni_modules as mm


def main():
    matrix_df = size_match.pull_matrix_file()
    dimensions = size_match.main(out=False)
    if (
        matrix_df is not None
        and dimensions is not None
        and isinstance(matrix_df, pd.DataFrame)
    ):
        matrix_df = matrix_df.drop_duplicates("sku")
        total = pd.merge(dimensions, matrix_df, how="left", on="sku", validate="1:1")
        total = total[
            [
                "sku",
                "asin",
                "collection",
                "size",
                "color",
                "l",
                "w",
                "h",
                "individual weight lbs",
                "sets in a box",
                "box length",
                "box width",
                "box depth",
                "box weight lbs",
                "target_l",
                "target_w",
                "target_h",
                "target_weight",
                "target_qty_per_box",
                "target_box_l",
                "target_box_w",
                "target_box_h",
                "target_box_weight",
            ]
        ]

        column_mapping = {
            "l": "target_l",
            "w": "target_w",
            "h": "target_h",
            "individual weight lbs": "target_weight",
            "sets in a box": "target_qty_per_box",
            "box length": "target_box_l",
            "box width": "target_box_w",
            "box depth": "target_box_h",
            "box weight lbs": "target_box_weight",
        }

        for key in column_mapping:
            total[key] = total[key].round(2)

        matches = pd.DataFrame(
            [
                pd.to_numeric(total[key], errors="coerce").round(2)
                != pd.to_numeric(total[value], errors="coerce").round(2)
                for key, value in column_mapping.items()
            ]
        ).T
        total["difference"] = matches.any(axis=1)
        mm.export_to_excel([total], ["dimensions"], "dimensions.xlsx", user_folder)
        mm.open_file_folder(user_folder)
