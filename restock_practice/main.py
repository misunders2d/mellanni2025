import sqlite3
import pandas as pd
import os

reports_dir = 'reports_data/'
subfolders = ['fba_inventory','sales']
print(f'Root dir (reports_data) exists? : {os.path.exists(reports_dir)}')
for subfolder in subfolders:
    subfolder_path = os.path.join(reports_dir, subfolder)
    print(f'Subfolder {subfolder} exists? : {os.path.exists(subfolder_path)}')
    if not os.path.exists(subfolder_path):
        print(f'Creating subfolder: {subfolder_path}')
        os.makedirs(subfolder_path, exist_ok=True)

