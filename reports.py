from connectors import gcloud as gc
import pandas as pd
import customtkinter as ctk
from common import user_folder
from utils import mellanni_modules as mm
import time, os
import pytz

client = gc.gcloud_connect()
tables = gc.get_tables('reports')

class Report(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.geometry('1280x800')
        self.title('Pull reports from BQ')
        self.labels = []
        self.check_vars = []
        self.table_list = ctk.CTkComboBox(self, values=tables, width=500, command=self.get_list_columns)
        self.table_list.pack(pady=10)
        self.control_frame = ctk.CTkFrame(self, width = 1200)
        self.control_frame.pack()
        self.button = ctk.CTkButton(self.control_frame, text='PULL', command=self.pull_bq)
        self.button.pack(side = 'left', padx = 10)
        self.select_all = ctk.CTkCheckBox(self.control_frame, text='Select all', command=self.run_select_all) #lambda: [label.select() if self.select_all.get() else label.deselect() for label in labels]
        self.select_all.pack(side = 'right', padx = 10)
        self.column_frame = ctk.CTkFrame(self, width=1200, height=600)
        self.column_frame.pack()
        self.query_field = ctk.CTkTextbox(self, width=1200, height=100)
        self.query_field.pack()

    def convert_timezones(self, df:pd.DataFrame):
        columns = df.columns.tolist()
        for column in columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]) and df[column].dt.tz is not None:
                if df[column].dt.tz != 'US/Pacific':
                    df[f'{column}_TZ'] = df[column].dt.tz
                    df[f'{column}_pacific'] = df[column].dt.tz_convert('US/Pacific').dt.tz_localize(None)

                df[column] = df[column].dt.tz_localize(None)
        return df

    def run_select_all(self, *args):
        for label in self.labels:
            if self.select_all.get():
                label.select()
            else:
                label.deselect()

    def pull_bq(self):
        query_string = self.query_field.get(0.0, ctk.END)
        df: pd.DataFrame = client.query(query_string.strip()).to_dataframe()
        df = self.convert_timezones(df)
        mm.export_to_excel([df],['bq_result'],'reports.xlsx')
        mm.open_file_folder(user_folder)

    def print_checkbox(self, *args, **kwargs):
        checkboxes_list = [x.cget('text') for x in self. labels if x.get()]
        checkbox_str = ', '.join(checkboxes_list)

        self.query_field.delete(0.0, ctk.END)

        self.query_field.insert(0.0, f'SELECT {checkbox_str} FROM reports.{self.table_list.get()} LIMIT 100')
        


    def get_list_columns(self, table_name):
        schema = client.get_table(f'reports.{table_name}').schema
        if self.labels:
            for label in self.labels:
                label.destroy()
        self.labels = []
        for row in schema:
            self.temp_checkbox = ctk.CTkCheckBox(
                self.column_frame,
                text=row.name,
                command=self.print_checkbox)
            self.labels.append(self.temp_checkbox)
        row_index = 0
        column_index = 0
        for label in self.labels:
            label.grid(row = row_index, column = column_index, sticky="W")
            row_index += 1
            if row_index == 25:
                row_index = 0
                column_index += 1

app = Report()
app.mainloop()