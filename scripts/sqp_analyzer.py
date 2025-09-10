import pandas as pd
import pandas_gbq
from fuzzywuzzy import fuzz
import os, re, time
from common import user_folder
from utils import mellanni_modules as mm
from typing import Literal
from connectors import gcloud as gc

folder = "/home/misunderstood/Documents/sqp2"
file_list = [os.path.join(folder, x) for x in os.listdir(folder) if x.endswith(".csv")]

renaming = {
    "search_query": "Search Query",
    "search_query_volume": "Search Query Volume",
    "impressions:_total_count": "Impressions: Total Count",
    "impressions:_brand_count": "Impressions: Brand Count",
    "impressions:_brand_share_%": "Impressions: Brand Share %",
    "clicks:_total_count": "Clicks: Total Count",
    "clicks:_click_rate_%": "Clicks: Click Rate %",
    "clicks:_brand_count": "Clicks: Brand Count",
    "clicks:_brand_share_%": "Clicks: Brand Share %",
    "clicks:_price_median": "Clicks: Price (Median)",
    "clicks:_brand_price_median": "Clicks: Brand Price (Median)",
    "clicks:_same_day_shipping_speed": "Clicks: Same Day Shipping Speed",
    "clicks:_1d_shipping_speed": "Clicks: 1D Shipping Speed",
    "clicks:_2d_shipping_speed": "Clicks: 2D Shipping Speed",
    "cart_adds:_total_count": "Cart Adds: Total Count",
    "cart_adds:_cart_add_rate_%": "Cart Adds: Cart Add Rate %",
    "cart_adds:_brand_count": "Cart Adds: Brand Count",
    "cart_adds:_brand_share_%": "Cart Adds: Brand Share %",
    "cart_adds:_price_median": "Cart Adds: Price (Median)",
    "cart_adds:_brand_price_median": "Cart Adds: Brand Price (Median)",
    "cart_adds:_same_day_shipping_speed": "Cart Adds: Same Day Shipping Speed",
    "cart_adds:_1d_shipping_speed": "Cart Adds: 1D Shipping Speed",
    "cart_adds:_2d_shipping_speed": "Cart Adds: 2D Shipping Speed",
    "purchases:_total_count": "Purchases: Total Count",
    "purchases:_purchase_rate_%": "Purchases: Purchase Rate %",
    "purchases:_brand_count": "Purchases: Brand Count",
    "purchases:_brand_share_%": "Purchases: Brand Share %",
    "purchases:_price_median": "Purchases: Price (Median)",
    "purchases:_brand_price_median": "Purchases: Brand Price (Median)",
    "purchases:_same_day_shipping_speed": "Purchases: Same Day Shipping Speed",
    "purchases:_1d_shipping_speed": "Purchases: 1D Shipping Speed",
    "purchases:_2d_shipping_speed": "Purchases: 2D Shipping Speed",
    "reporting_date": "Reporting Date",
    "year": "year",
    "week": "week",
}

combined_result_asin = {
    "Weekly": {str(year): {} for year in range(2020, 2030)},
    "Monthly": {str(year): {} for year in range(2020, 2030)},
    "Quarterly": {str(year): {} for year in range(2020, 2030)},
}
combined_result_brand = {
    "Weekly": {str(year): {} for year in range(2020, 2030)},
    "Monthly": {str(year): {} for year in range(2020, 2030)},
    "Quarterly": {str(year): {} for year in range(2020, 2030)},
}


def is_similar(query, target, threshold):
    """Check if the query is similar to the target string using fuzzy matching."""
    words = query.lower().split()
    for word in words:
        if any([fuzz.ratio(word, x.lower()) >= threshold for x in target.split()]):
            return True
    return False


def read_bq(
    start_week: str = "2025-01-01", end_week: str = "2025-12-31"
) -> pd.DataFrame:
    """Read data from BigQuery and return a DataFrame."""
    with gc.gcloud_connect() as client:
        query = f'''SELECT * FROM `auxillary_development.sqp_brand_weekly` WHERE reporting_date BETWEEN "{start_week}" AND "{end_week}"'''
        result = client.query(query).to_dataframe()
        result = result.rename(columns=renaming)
    return result


def push_to_bq(file_list):
    """Push downloaded SQP Brand weekly report to BigQuery."""
    total_df = pd.DataFrame()
    for file in file_list:
        check_file(file, scope="bq")
        header = pd.read_csv(file, nrows=1)
        columns = header.columns.tolist()
        sqp = process_header_columns(columns)
        temp = pd.read_csv(file, skiprows=1)
        year = int(sqp["year"])
        week = int(sqp["week"].split(" ")[1])
        temp[["year", "week"]] = [year, week]
        total_df = pd.concat([total_df, temp])
    del total_df["Search Query Score"]
    gc.normalize_columns(total_df)
    total_df = total_df.sort_values(
        ["reporting_date", "search_query_volume"], ascending=[True, False]
    )

    with gc.gcloud_connect() as client:
        dates = total_df["reporting_date"].unique().tolist()
        dates_str = '","'.join(dates)
        query = f"""DELETE FROM `auxillary_development.sqp_brand_weekly` WHERE reporting_date IN ("{dates_str}")"""
        result = client.query(query)
        while result.running():
            print("Wait, deleting rows")
            time.sleep(2)
            result.reload()
        print(
            f"Deleted {result.num_dml_affected_rows} rows from the table, pushing new data"
        )
        pandas_gbq.to_gbq(
            total_df, "auxillary_development.sqp_brand_weekly", if_exists="append"
        )
    return


def check_file(filename: str, scope: Literal["general", "bq"] = "general") -> None:
    """Check if the file is a valid SQP file."""
    if not os.path.splitext(filename)[1] == ".csv":
        raise BaseException(f"Files must be csv:\n{filename}")
    test = pd.read_csv(filename)
    condition = (
        ("ASIN" in test.columns.tolist()[0] or "Brand" in test.columns.tolist()[0])
        if scope == "general"
        else ("Brand" in test.columns.tolist()[0])
    )
    if not condition:
        raise BaseException(f"Wrong / non-SQP file provided:\n{filename}")


def process_header_columns(columns: list) -> dict:
    """Process the header columns to extract relevant information."""
    asin = re.findall('"(.*?)"', columns[0])[0]
    timeframe = re.findall('"(.*?)"', columns[1])[0]
    scope = "asin" if "ASIN" in columns[0] else "brand"
    if timeframe == "Weekly":
        year = re.findall("(2[0-90]{3})", columns[2])[-1]
        week = re.findall('"(.*?)"', columns[2])[0]
        return {scope: [asin], "timeframe": timeframe, "year": year, "week": week}
    elif timeframe == "Monthly":
        year = re.findall('"(.*?)"', columns[2])[0]
        month = re.findall('"(.*?)"', columns[3])[0]
        return {scope: [asin], "timeframe": timeframe, "year": year, "month": month}
    elif timeframe == "Quarterly":
        year = re.findall('"(.*?)"', columns[2])[0]
        quarter = re.findall('"(.*?)"', columns[3])[0]
        return {scope: [asin], "timeframe": timeframe, "year": year, "quarter": quarter}


def sort_files(file_list: list) -> None:
    """ "Sort the files into the combined result dictionaries."""
    duplicates = []
    for file in file_list:
        check_file(file)
        header = pd.read_csv(file, nrows=1)
        columns = header.columns.tolist()
        sqp = process_header_columns(columns)
        if not sqp in duplicates:  # check for duplicated files just in case
            duplicates.append(dict(sqp))
        else:
            raise BaseException(f"Duplicate file found:\n{file}\n{sqp}")

        if "asin" in sqp:
            target_dict = combined_result_asin
            scope = "asin"
        elif "brand" in sqp:
            target_dict = combined_result_brand
            scope = "brand"

        sqp["filepath"] = [file]
        if sqp["timeframe"] == "Weekly":
            year = sqp["year"]
            week = sqp["week"]
            if week in target_dict["Weekly"][year]:
                target_dict["Weekly"][year][week][scope].extend(sqp[scope])
                target_dict["Weekly"][year][week]["filepath"].extend(sqp["filepath"])
            else:
                target_dict["Weekly"][year][week] = sqp
        elif sqp["timeframe"] == "Monthly":
            year = sqp["year"]
            month = sqp["month"]
            if month in target_dict["Monthly"][year]:
                target_dict["Monthly"][year][month][scope].extend(sqp[scope])
                target_dict["Monthly"][year][month]["filepath"].extend(sqp["filepath"])
            else:
                target_dict["Monthly"][year][month] = sqp
        elif sqp["timeframe"] == "Quarterly":
            year = sqp["year"]
            quarter = sqp["quarter"]
            if quarter in target_dict["Quarterly"][year]:
                target_dict["Quarterly"][year][quarter][scope].extend(sqp[scope])
                target_dict["Quarterly"][year][quarter]["filepath"].extend(
                    sqp["filepath"]
                )
            else:
                target_dict["Quarterly"][year][quarter] = sqp
    return


def filter_dicts(files_dict: dict) -> dict:
    """Filter the dictionaries to remove empty entries."""
    clean = {"Weekly": {}, "Monthly": {}, "Quarterly": {}}
    for timeframe in ("Weekly", "Monthly", "Quarterly"):
        temp_dict = files_dict[timeframe]
        for year in temp_dict:
            if temp_dict[year]:
                clean[timeframe][year] = temp_dict[year]
    return {key: value for key, value in clean.items() if value}


def refine_file(
    df: pd.DataFrame, scope: Literal["asin", "brand"] = "asin"
) -> pd.DataFrame:
    """Refine the DataFrame by calculating additional metrics."""
    entity = "ASIN" if scope == "asin" else "Brand"
    df["ASINs shown"] = df["Impressions: Total Count"] / df["Search Query Volume"]
    df["ASINs glance rate"] = (
        df[f"Impressions: {entity} Count"] / df["Search Query Volume"]
    )
    df["KW ctr"] = df["Clicks: Total Count"] / df["Impressions: Total Count"]
    df["ASIN ctr"] = df[f"Clicks: {entity} Count"] / df[f"Impressions: {entity} Count"]
    df["KW ATC %"] = df["Cart Adds: Total Count"] / df["Clicks: Total Count"]
    df["ASINs ATC %"] = df[f"Cart Adds: {entity} Count"] / df[f"Clicks: {entity} Count"]

    df["KW ATC conversion"] = (
        df["Purchases: Total Count"] / df["Cart Adds: Total Count"]
    )
    df["ASINs ATC conversion"] = (
        df[f"Purchases: {entity} Count"] / df[f"Cart Adds: {entity} Count"]
    )

    df["KW conversion"] = df["Purchases: Total Count"] / df["Clicks: Total Count"]
    df["ASINs conversion"] = (
        df[f"Purchases: {entity} Count"] / df[f"Clicks: {entity} Count"]
    )
    return df


def combine_files(
    dfs: list,
    scope: Literal["asin", "brand"] = "asin",
    column: Literal["Search Query", "Reporting Date"] = "Search Query",
) -> pd.DataFrame:
    """Combine multiple DataFrames into one and calculate additional metrics."""
    entity = "ASIN" if scope == "asin" else "Brand"
    agg_func = "min" if scope == "asin" else "sum"
    sum_cols_asin = [
        f"Impressions: {entity} Count",
        f"Clicks: {entity} Count",
        f"Cart Adds: {entity} Count",
        f"Purchases: {entity} Count",
        "median_click_product",
        "median_atc_product",
        "median_purchase_product",
    ]
    immutable_cols = [
        "Search Query Volume",
        "Impressions: Total Count",
        "Clicks: Total Count",
        "Cart Adds: Total Count",
        "Purchases: Total Count",
        "median_click_total",
        "median_atc_total",
        "median_purchase_total",
    ]

    total = pd.concat(dfs).fillna(0)

    total["median_click_total"] = (
        total["Clicks: Price (Median)"] * total["Clicks: Total Count"]
    )
    total["median_atc_total"] = (
        total["Cart Adds: Price (Median)"] * total["Cart Adds: Total Count"]
    )
    total["median_purchase_total"] = (
        total["Purchases: Price (Median)"] * total["Purchases: Total Count"]
    )

    total["median_click_product"] = (
        total[f"Clicks: {entity} Price (Median)"] * total[f"Clicks: {entity} Count"]
    )
    total["median_atc_product"] = (
        total[f"Cart Adds: {entity} Price (Median)"]
        * total[f"Cart Adds: {entity} Count"]
    )
    total["median_purchase_product"] = (
        total[f"Purchases: {entity} Price (Median)"]
        * total[f"Purchases: {entity} Count"]
    )

    common_df = total.groupby(column)[immutable_cols].agg(agg_func).reset_index()
    asin_df = total.groupby(column)[sum_cols_asin].agg("sum").reset_index()

    common_df["Clicks: Price (Median)"] = (
        common_df["median_click_total"] / common_df["Clicks: Total Count"]
    )
    common_df["Cart Adds: Price (Median)"] = (
        common_df["median_atc_total"] / common_df["Cart Adds: Total Count"]
    )
    common_df["Purchases: Price (Median)"] = (
        common_df["median_purchase_total"] / common_df["Purchases: Total Count"]
    )

    asin_df[f"Clicks: {entity} Price (Median)"] = (
        asin_df["median_click_product"] / asin_df[f"Clicks: {entity} Count"]
    )
    asin_df[f"Cart Adds: {entity} Price (Median)"] = (
        asin_df["median_atc_product"] / asin_df[f"Cart Adds: {entity} Count"]
    )
    asin_df[f"Purchases: {entity} Price (Median)"] = (
        asin_df["median_purchase_product"] / asin_df[f"Purchases: {entity} Count"]
    )

    summary = pd.merge(common_df, asin_df, how="outer", on=column, validate="1:1")

    for col in (
        "median_click_product",
        "median_atc_product",
        "median_purchase_product",
        "median_click_total",
        "median_atc_total",
        "median_purchase_total",
    ):
        del summary[col]

    return summary


def export_sqps(sqp_dict, scope="asin"):
    """Export the SQP data to Excel files."""
    for timeframe in sqp_dict:
        with pd.ExcelWriter(
            os.path.join(user_folder, f"SQP_{scope}_{timeframe}.xlsx"),
            engine="xlsxwriter",
        ) as writer:
            for year in sqp_dict[timeframe]:
                for period in sqp_dict[timeframe][year]:
                    reporting_range = period.split("|")[0].strip()
                    sheet_name = f"{year} - {reporting_range}"
                    entities = ", ".join(sqp_dict[timeframe][year][period][scope])
                    dfs = [
                        pd.read_csv(file, skiprows=1)
                        for file in sqp_dict[timeframe][year][period]["filepath"]
                    ]
                    combined = combine_files(dfs, scope)
                    summary = refine_file(combined.copy(), scope)
                    entity_row = pd.DataFrame(
                        [f"SQP analysis for: {entities}"], columns=["Search Query"]
                    )

                    summary = pd.concat(
                        [summary, entity_row], axis=0, ignore_index=True
                    )
                    summary.to_excel(writer, sheet_name=sheet_name, index=False)
                    mm.format_header(summary, writer, sheet_name)


# sort_files(file_list)
# brand_sqps = filter_dicts(combined_result_brand)
# asin_sqps = filter_dicts(combined_result_asin)
# export_sqps(brand_sqps, 'brand')
# export_sqps(asin_sqps, 'asin')

bq = read_bq(start_week="2025-03-01", end_week="2025-03-08")
filtered = bq[bq["Search Query"].apply(lambda x: is_similar(x, "pillowcase", 70))]
df_date = combine_files([filtered], scope="brand", column="Reporting Date")
df_search = combine_files([filtered], scope="brand", column="Search Query")
df_date = refine_file(df_date, scope="brand")
df_search = refine_file(df_search, scope="brand")


with gc.gcloud_connect() as client:
    query = """SELECT DISTINCT (week,year) as weekyear, week, year, reporting_date FROM `auxillary_development.sqp_brand_weekly`"""
    result = client.query(query).to_dataframe()
    del result["weekyear"]
    result = result.sort_values("reporting_date")
