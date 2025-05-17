import pandas as pd
import os
from typing import Literal, List

folder = '/home/misunderstood/Documents/sqp' ## update folder path
file_paths = [ ### update file paths
    'US_Search_Query_Performance_ASIN_View_Simple_Month_2025_03_31 (1).csv',
    'US_Search_Query_Performance_ASIN_View_Simple_Month_2025_03_31.csv'
    ]

def refine_file(
        df: pd.DataFrame,
        scope:Literal['asin','brand']='asin'
        ) -> pd.DataFrame:
    """Refine the DataFrame by calculating additional metrics."""
    entity='ASIN' if scope=='asin' else 'Brand'
    df['ASINs shown'] = df['Impressions: Total Count'] / df['Search Query Volume']
    df['ASINs glance rate'] = df[f'Impressions: {entity} Count'] / df['Search Query Volume']
    df['KW ctr'] = df['Clicks: Total Count'] / df['Impressions: Total Count']
    df['ASIN ctr'] = df[f'Clicks: {entity} Count'] / df[f'Impressions: {entity} Count']
    df['KW ATC %'] = df['Cart Adds: Total Count'] / df['Clicks: Total Count']
    df['ASINs ATC %'] = df[f'Cart Adds: {entity} Count'] / df[f'Clicks: {entity} Count']

    df['KW ATC conversion'] = df['Purchases: Total Count'] / df['Cart Adds: Total Count']
    df['ASINs ATC conversion'] = df[f'Purchases: {entity} Count'] / df[f'Cart Adds: {entity} Count']

    df['KW conversion'] = df['Purchases: Total Count'] / df['Clicks: Total Count']
    df['ASINs conversion'] = df[f'Purchases: {entity} Count'] / df[f'Clicks: {entity} Count']
    return df

def combine_files(
        dfs: List[pd.DataFrame],
        scope:Literal['asin','brand']='asin',
        column:Literal['Search Query', 'Reporting Date'] = 'Search Query'
        ) -> pd.DataFrame:
    """Combine multiple DataFrames into one and calculate additional metrics."""
    entity='ASIN' if scope=='asin' else 'Brand'
    agg_func = 'min' if scope=='asin' else 'sum'
    sum_cols_asin = [
        f'Impressions: {entity} Count',f'Clicks: {entity} Count',
        f'Cart Adds: {entity} Count',f'Purchases: {entity} Count',
        'median_click_product','median_atc_product','median_purchase_product'
        ]
    immutable_cols = [
        'Search Query Volume','Impressions: Total Count', 'Clicks: Total Count',
        'Cart Adds: Total Count','Purchases: Total Count',
        'median_click_total', 'median_atc_total', 'median_purchase_total'
        ]
    
    total = pd.concat(dfs).fillna(0)

    total['median_click_total'] = total['Clicks: Price (Median)'] * total['Clicks: Total Count']
    total['median_atc_total'] = total['Cart Adds: Price (Median)'] * total['Cart Adds: Total Count']
    total['median_purchase_total'] = total['Purchases: Price (Median)'] * total['Purchases: Total Count']
    
    total['median_click_product'] = total[f'Clicks: {entity} Price (Median)'] * total[f'Clicks: {entity} Count']
    total['median_atc_product'] = total[f'Cart Adds: {entity} Price (Median)'] * total[f'Cart Adds: {entity} Count']
    total['median_purchase_product'] = total[f'Purchases: {entity} Price (Median)'] * total[f'Purchases: {entity} Count']
    

    common_df = total.groupby(column)[immutable_cols].agg(agg_func).reset_index()
    asin_df = total.groupby(column)[sum_cols_asin].agg('sum').reset_index()
    
    common_df['Clicks: Price (Median)'] = common_df['median_click_total'] / common_df['Clicks: Total Count']
    common_df['Cart Adds: Price (Median)'] = common_df['median_atc_total'] / common_df['Cart Adds: Total Count']
    common_df['Purchases: Price (Median)'] = common_df['median_purchase_total'] / common_df['Purchases: Total Count']

    asin_df[f'Clicks: {entity} Price (Median)'] = asin_df['median_click_product'] / asin_df[f'Clicks: {entity} Count']
    asin_df[f'Cart Adds: {entity} Price (Median)'] = asin_df['median_atc_product'] / asin_df[f'Cart Adds: {entity} Count']
    asin_df[f'Purchases: {entity} Price (Median)'] = asin_df['median_purchase_product'] / asin_df[f'Purchases: {entity} Count']
    
    summary = pd.merge(common_df, asin_df, how='outer', on=column, validate="1:1")

    for col in ('median_click_product','median_atc_product','median_purchase_product',
                'median_click_total','median_atc_total','median_purchase_total'):
        del summary[col]
    
    return summary



file_list = [pd.read_csv(os.path.join(folder, file), skiprows=1) for file in file_paths]
# file_list = []

# for file in file_paths:
#     temp_file = pd.read_csv(os.path.join(folder, file), skiprows=1)
#     file_list.append(temp_file)


total_df = pd.concat(file_list)
sum_cols_asin = [
    'Impressions: ASIN Count','Clicks: ASIN Count',
    'Cart Adds: ASIN Count','Purchases: ASIN Count',
    ]
immutable_cols = [
    'Search Query Volume','Impressions: Total Count', 'Clicks: Total Count',
    'Cart Adds: Total Count','Purchases: Total Count',
    ]

asin_df = total_df.groupby('Search Query')[sum_cols_asin].agg('sum').reset_index()
others_df = total_df.groupby('Search Query')[immutable_cols].agg('min').reset_index()

result = pd.merge(asin_df, others_df, how='outer', on = 'Search Query', validate = '1:1')
result = refine_file(result)
result.to_excel('/home/misunderstood/temp/sqp_result.xlsx', index= False)


