import sqlite3
import pandas as pd
import os

def check_folders():
    reports_dir = 'reports_data'
    subfolders = ['fba_inventory','sales']
    # print(f'Root dir (reports_data) exists? : {os.path.exists(reports_dir)}')
    if not {os.path.exists(reports_dir)}:
        return False
    for subfolder in subfolders:
        subfolder_path = os.path.join(reports_dir, subfolder)
        # print(f'Subfolder {subfolder} exists? : {os.path.exists(subfolder_path)}')
        if not os.path.exists(subfolder_path):
            # print(f'Creating subfolder: {subfolder_path}')
            # os.makedirs(subfolder_path, exist_ok=True)
            return False
    return True

if not check_folders():
    print('Please create the reports_data folder and its subfolders (fba_inventory, sales) before running the script.')
    raise BaseException('Required folders are missing.')
else:
    print('All required folders exist. Proceeding with the script...')

def read_files(subfolder):
    result = pd.DataFrame()
    file_list = os.listdir(os.path.join('reports_data', subfolder))
    for file in file_list:
        if not file.endswith('.csv'):
            print(f'Skipping non-CSV file: {file}')
            continue
        temp_file = pd.read_csv(os.path.join('reports_data', 'fba_inventory', file))
        result = pd.concat([result, temp_file], axis=0, ignore_index=True)
    return result

inventory = read_files('fba_inventory')
# sales = read_files('sales')


with sqlite3.connect('restock_canada.db') as connector:
    inventory.to_sql('fba_inventory', connector, if_exists='append', index=False)

with sqlite3.connect('restock_canada.db') as conn:
    db_data = pd.read_sql('SELECT * FROM fba_inventory', conn)

print(db_data.shape)