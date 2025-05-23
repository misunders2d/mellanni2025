import pandas as pd
import os
from typing import Literal, List
from utils import mellanni_modules as mm

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
total_df['clicksXprice'] = total_df['Clicks: ASIN Count'] * total_df['Clicks: ASIN Price (Median)']
total_df['atcXprice'] = total_df['Cart Adds: ASIN Count'] * total_df['Cart Adds: ASIN Price (Median)']
total_df['purchaseXprice'] = total_df['Purchases: ASIN Count'] * total_df['Purchases: ASIN Price (Median)']

sum_cols_asin = [
    'Impressions: ASIN Count','Clicks: ASIN Count',
    'Cart Adds: ASIN Count','Purchases: ASIN Count',
    'clicksXprice','atcXprice','purchaseXprice'

    ]
immutable_cols = [
    'Search Query Volume','Impressions: Total Count', 'Clicks: Total Count',
    'Cart Adds: Total Count','Purchases: Total Count','Clicks: Price (Median)',
    'Cart Adds: Price (Median)', 'Purchases: Price (Median)'
    ]

asin_df = total_df.groupby('Search Query')[sum_cols_asin].agg('sum').reset_index()
others_df = total_df.groupby('Search Query')[immutable_cols].agg('min').reset_index()


#alternative method
aggregation_schema = {
    'Impressions: ASIN Count':'sum',
    'Clicks: ASIN Count':'sum',
    'Cart Adds: ASIN Count':'sum',
    'Purchases: ASIN Count':'sum',
    'Search Query Volume':'min',
    'Impressions: Total Count':'min',
    'Clicks: Total Count':'min',
    'Cart Adds: Total Count':'min',
    'Purchases: Total Count':'min',
    
    'clicksXprice':'sum',
    'atcXprice':'sum',
    'purchaseXprice':'sum',

    
    }
alt_df = total_df.groupby('Search Query').agg(aggregation_schema).reset_index()


alt_df['Clicks: ASIN Price (Median)'] = alt_df['clicksXprice'] / alt_df['Clicks: ASIN Count'] 
alt_df['Cart Adds: ASIN Price (Median)'] = alt_df['atcXprice'] / alt_df['Cart Adds: ASIN Count']
alt_df['Purchases: ASIN Price (Median)'] = alt_df['purchaseXprice'] / alt_df['Purchases: ASIN Count']
# end of alternative method


result = pd.merge(asin_df, others_df, how='outer', on = 'Search Query', validate = '1:1')
result['Clicks: ASIN Price (Median)'] = result['clicksXprice'] / result['Clicks: ASIN Count'] 
result['Cart Adds: ASIN Price (Median)'] = result['atcXprice'] / result['Cart Adds: ASIN Count']
result['Purchases: ASIN Price (Median)'] = result['purchaseXprice'] / result['Purchases: ASIN Count']


del_cols = ['clicksXprice','atcXprice','purchaseXprice']
for column in del_cols:
    del result[column]

result = refine_file(result)


result = result[['Search Query','Search Query Volume',
                 'Impressions: Total Count','Impressions: ASIN Count',
                 
                 'Clicks: Total Count','Clicks: ASIN Count',
                 'Cart Adds: Total Count','Cart Adds: ASIN Count',
                 'Purchases: Total Count','Purchases: ASIN Count', 
                 'Clicks: Price (Median)','Clicks: ASIN Price (Median)',
                 'Cart Adds: Price (Median)','Cart Adds: ASIN Price (Median)',
                 'Purchases: Price (Median)', 'Purchases: ASIN Price (Median)',
                 
                 'ASINs shown','ASINs glance rate',
                 'KW ctr', 'ASIN ctr',
                 'KW ATC %','ASINs ATC %',
                 'KW ATC conversion', 'ASINs ATC conversion',
                 'KW conversion', 'ASINs conversion']]

# columns = [x for x in result.columns if 'Xprice' not in x]
# result = result[columns]





with pd.ExcelWriter('/home/misunderstood/temp/sqp_result.xlsx', engine = 'xlsxwriter') as writer:
    result.to_excel(writer, index= False, sheet_name = 'result')
    mm.format_header(result, writer, 'result')


