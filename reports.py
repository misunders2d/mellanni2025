from connectors import gcloud as gc
import customtkinter as ctk
import time

client = gc.gcloud_connect()
tables = gc.get_tables('reports')

class Report(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.geometry('1080x800')
        self.title('Pull reports from BQ')
        self.labels = []
        self.check_vars = []
        self.table_list = ctk.CTkComboBox(self, values=tables, width=500, command=self.get_list_columns)
        self.table_list.pack(pady=10)
        self.button = ctk.CTkButton(self, text='PULL', command=self.pull_bq)
        self.button.pack()

    def pull_bq(self):
        table_name = self.table_list.get()
        column_names = [label.cget("text") for label in self.labels if label.get()]
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
            self.labels.append(ctk.CTkCheckBox(self, text=row.name))
        for label in self.labels:
            label.pack()

app = Report()
app.mainloop()