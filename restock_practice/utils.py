import os
import pandas as pd


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


def read_files(subfolder):
    result = pd.DataFrame()
    file_list = os.listdir(os.path.join('reports_data', subfolder))
    for file in file_list:
        if not file.endswith('.csv'):
            print(f'Skipping non-CSV file: {file}')
            continue
        temp_file = pd.read_csv(os.path.join('reports_data', subfolder, file))
        columns_old = temp_file.columns.tolist()
        columns_new = [x.strip() for x in columns_old]
        columns_new = [x.replace('–','-').replace(' ','_').lower() for x in columns_new]
        temp_file.columns = columns_new
        if subfolder == 'sales':
            date_str = file.replace('.csv','')
            date = pd.to_datetime(date_str)
            print(f'reading date: {date}')
            temp_file['date'] = date
        result = pd.concat([result, temp_file], axis=0, ignore_index=True)
    return result