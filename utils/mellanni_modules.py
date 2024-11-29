import os
from typing import List, Any
import pandas as pd
import sys, subprocess
import customtkinter as ctk


def open_file_folder(path: str) -> None:
    try:
        os.startfile(path)
    except AttributeError:
        opener = "open" if sys.platform == "darwin" else "xdg-open"
        subprocess.call([opener, path])
    except Exception as e:
        print(f'Uncaught exception occurred: {e}')
    return None

def export_to_excel(
        dfs: List[pd.DataFrame],
        sheet_names: List[str],
        filename: str = 'test.xlsx',
        out_folder: Any[str,None] = None
        ) -> None:
    if not out_folder:
        out_folder = ctk.filedialog.askdirectory(title='Select output folder', initialdir=os.path.expanduser('~'))
    full_output = os.path.join(out_folder,filename)
    try:
        with pd.ExcelWriter(full_output, engine = 'xlsxwriter') as writer:
            for df, sheet_name in list(zip(dfs,sheet_names)):
                df.to_excel(excel_writer = writer, sheet_name = sheet_name, index = False)
                format_header(df, writer, sheet_name)
    except PermissionError:
        print(f'{filename} is open, please close the file first')
        export_to_excel(dfs, sheet_names, filename, out_folder)
    except Exception as e:
        print(e)
        
    return None

def format_header(df,writer,sheet):
    workbook  = writer.book
    cell_format = workbook.add_format({'bold': True, 'text_wrap': True, 'valign': 'center', 'font_size':9})
    worksheet = writer.sheets[sheet]
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, cell_format)
    max_row, max_col = df.shape
    worksheet.autofilter(0, 0, max_row, max_col - 1)
    worksheet.freeze_panes(1,0)
    return None

def format_columns(df,writer,sheet,col_num):
    worksheet = writer.sheets[sheet]
    if not isinstance(col_num,list):
        col_num = [col_num]
    else:
        pass
    for c in col_num:
        width = max(df.iloc[:,c].astype(str).map(len).max(),len(df.iloc[:,c].name))
        worksheet.set_column(c,c,width)
    return None