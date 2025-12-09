import pandas as pd
import customtkinter as ctk
import os
from scripts import data
from common import user_folder
from utils import mellanni_modules as mm

target_column = "product_type"

output = os.path.join(os.path.expanduser("~"), "temp/pics")
os.makedirs(output, exist_ok=True)


class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.geometry("400x200")
        self.title("Flat file transfer")

        self.old_file = ctk.CTkEntry(self, placeholder_text="Old file", width=200)
        self.old_file.bind("<Button-1>", self.old_file_input)
        self.old_file.pack(pady=10)

        self.new_file = ctk.CTkEntry(self, placeholder_text="New file", width=200)
        self.new_file.bind("<Button-1>", self.new_file_input)
        self.new_file.pack(pady=10)

        self.change_generation = ctk.CTkCheckBox(
            self, text="Switch flat file generation"
        )
        self.change_generation.pack(pady=10)

        self.button = ctk.CTkButton(self, text="OK", command=self.combine_files)
        self.button.pack(pady=10)

    def old_file_input(self, *args):
        self.old_file_path = ctk.filedialog.askopenfilename(
            initialdir=user_folder, title="Select old file"
        )
        self.old_file.insert(0, self.old_file_path)

    def new_file_input(self, *args):
        self.new_file_path = ctk.filedialog.askopenfilename(
            initialdir=user_folder, title="Select new file"
        )
        self.new_file.insert(0, self.new_file_path)

    def rename_columns(self, old):
        old_columns = old.columns.tolist()
        check_column = list(data.sheets_mapping.keys())[0]
        if check_column in old_columns:
            mapping = data.sheets_mapping
        else:
            mapping = {v: k for k, v in data.sheets_mapping.items()}

        old = old.rename(columns=mapping)
        return old

    def combine_files(self):

        old = pd.read_excel(self.old_file_path, sheet_name="Template").fillna("")
        for i in range(10):
            if any(target_column in x for x in old.iloc[i]):
                break
        old = pd.read_excel(
            self.old_file_path, sheet_name="Template", header=i + 1
        ).fillna("")

        new = pd.read_excel(self.new_file_path, sheet_name="Template").fillna("")
        for i in range(10):
            if any(target_column in x for x in new.iloc[i]):
                break
        new = pd.read_excel(
            self.new_file_path, sheet_name="Template", header=i + 1
        ).fillna("")

        if self.change_generation.get():
            old = self.rename_columns(old)

        missed_columns = [x for x in old.columns if x not in new.columns]
        new_columns = [x for x in new.columns if x not in old.columns]
        if len(missed_columns) > 0:
            print(f"Old columns that are missing ({len(missed_columns)} columns):\n")
            print("\n".join(missed_columns))
        if len(new_columns) > 0:
            print(
                f"New columns that didn't exist in the old flat file ({len(new_columns)} columns):\n"
            )
            print("\n".join(new_columns))

        result = pd.concat([new, old])
        result.to_excel(os.path.join(user_folder, "combined.xlsx"), index=False)
        mm.open_file_folder(user_folder)


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
