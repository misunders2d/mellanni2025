from connectors import gcloud as gc
import customtkinter as ctk
import time

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
        self.select_all = ctk.CTkCheckBox(self.control_frame, text='Select all', command=self.run_select_all)
        self.select_all.pack(side = 'right', padx = 10)
        self.column_frame = ctk.CTkFrame(self, width=1200, height=600)
        self.column_frame.pack()
        self.query_field = ctk.CTkTextbox(self, width=1200, height=100)
        self.query_field.pack()

    def run_select_all(self, *args):
        for label in self.labels:
            if self.select_all.get():
                label.select()
            else:
                label.deselect()


    def pull_bq(self):
        table_name = self.table_list.get()
        column_names = [label.cget("text") for label in self.labels if label.get()]
        if column_names:
            columns = ', '.join(column_names)
            query = f'''SELECT {columns} FROM `reports.{table_name}` LIMIT 100'''
            df = client.query(query).to_dataframe()
            print(df)

    def get_list_columns(self, table_name):
        schema = client.get_table(f'reports.{table_name}').schema
        if self.labels:
            for label in self.labels:
                label.destroy()
        self.labels = []
        for row in schema:
            self.labels.append(ctk.CTkCheckBox(self.column_frame, text=row.name))
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