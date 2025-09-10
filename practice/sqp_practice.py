import os
import pandas as pd

folder = "/home/misunderstood/temp/practice"  # change path
files = os.listdir(folder)


def read_file(file_path):

    header = pd.read_csv(os.path.join(folder, file_path), nrows=1).columns.tolist()
    df = pd.read_csv(os.path.join(folder, file_path), header=1)

    sqp = {"header": header, "data": df}
    return sqp


sqps = []
for file in files:
    sqp_temp = read_file(file)
    sqps.append(sqp_temp)


total_df = pd.DataFrame()

for sqp_obj in sqps:
    df = sqp_obj["data"]
    total_df = pd.concat([total_df, df])

immutable_cols = ["Search Query Volume", "Impressions: Total Count"]
sum_cols = ["Impressions: ASIN Count"]

total_data = total_df.groupby("Search Query")[immutable_cols].agg("min").reset_index()
asin_data = total_df.groupby("Search Query")[sum_cols].agg("sum").reset_index()
result = pd.merge(
    total_data, asin_data, how="outer", on="Search Query", validate="one_to_one"
)
result.to_excel(
    "/home/misunderstood/temp/result.xlsx", index=False
)  # change output path
