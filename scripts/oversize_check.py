import pandas as pd
from datetime import datetime as dt
import PySimpleGUI as sg
import os
import pyperclip
from utils import mellanni_modules as mm
from connectors import gdrive as gd
from common import user_folder

# paths = mm.get_db_path('US')
inv_report = 'Inventory report.xlsx'
folder_id = '10nUn6KsjUzQpmmQMnncJ3JLUNhKFqoH6'
drive_id = '0AMdx9NlXacARUk9PVA'

link = 'https://sellercentral.amazon.com/reportcentral/ESTIMATED_FBA_FEES/1'
columns = ['sku','asin','longest-side','median-side','shortest-side',
               'length-and-girth','unit-of-dimension','item-package-weight',
               'product-size-tier','currency']

def check_oversize(file):
    file.columns = [x.replace('?','').replace('"','') for x in file.columns]
    file = file[columns]
    file = file[file['currency'] == 'USD']
    exclude = ['UsLargeStandardSize','UsSmallStandardSize']
    file = file[~file['product-size-tier'].isin(exclude)]
    inventory = pd.read_excel(gd.download_file(gd.find_file_id(folder_id, drive_id, filename=inv_report)))
    # inventory = pd.read_excel(os.path.join(paths[6],'Inventory report.xlsx'))
    inventory['sku'] = inventory['seller-sku']
    inventory = inventory[['sku','Quantity Available']]
    file = file.merge(inventory, how = 'left', on = 'sku')
    data = file.values.tolist()
    return data



def main():
    data = [['' for row in range(len(columns))]for col in range(2)]
    layout = [
        [sg.Text('First, generate the\nFee Preview report\nfrom Seller Central'),sg.Button('Copy link\nto Fee Preview', size = (7,5))],
        [sg.Input('Then, select the report file', key = 'REPORT'), sg.FileBrowse('Select file', initial_folder=user_folder)],
        [sg.Text('Then, click on "Process" button')],
        [sg.Button('Process'), sg.Button('Export to Excel'),sg.Button('Cancel')],
        [sg.Table(values = data, headings = columns,auto_size_columns=False,key = 'DATA')]
    ]

    window = sg.Window('Check and report oversize units', layout)
    while True:
        event, values = window.read()

        if event == sg.WIN_CLOSED or event == 'Cancel': # if user closes window or clicks cancel
            break
        elif event == 'Copy link\nto Fee Preview':
            pyperclip.copy(link)
        elif event == 'Process':
            try:
                path = values['REPORT']
                if '.txt' in os.path.basename(path):
                    try:
                        file = pd.read_csv(path, sep = '\t', encoding = 'utf-8')
                    except:
                        file = pd.read_csv(path, sep = '\t', encoding = 'cp1251')
                elif '.csv' in os.path.basename(path):
                    try:
                        file = pd.read_csv(path, encoding = 'utf-8')
                    except:
                        file = pd.read_csv(path, encoding = 'cp1251')
                data = check_oversize(file)
                window['DATA'].update(data)
            except:
                sg.Popup('Wrong file selected')
        elif event == 'Export to Excel':
            try:
                import size_match
                asins = size_match.main(out = False)
                asins = asins[['sku','collection','size','color','actuality','l','w',
                         'h','individual weight lbs','size_tier','fba_fee']]
                
                new_path = path+'.new.xlsx'
                file = file.merge(asins, how = 'left', left_on = 'sku', right_on = 'sku')
                with pd.ExcelWriter(new_path) as writer:
                    file.to_excel(writer, sheet_name = 'Sheet1', index = False)
                    mm.format_header(file, writer, 'Sheet1')
                mm.open_file_folder(new_path)
            except Exception as e:
                sg.Popup(f'Nothing to export\n{e}')
    window.close()
    return None
main()
