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
        self.table_list = ctk.CTkComboBox(self, values=tables, width=500, command=self.get_list_columns)
        self.table_list.pack(pady=10)


    def get_list_columns(self, table_name):
        schema = client.get_table(f'reports.{table_name}').schema
        self.labels = []
        for row in schema:
            self.labels.append(ctk.CTkLabel(self, text=row.name))
        for label in self.labels:
            label.pack()


app = Report()
app.mainloop()