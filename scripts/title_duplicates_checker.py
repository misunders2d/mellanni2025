import os
import pandas as pd
import customtkinter as ctk
from connectors import gcloud as gc
import inflect  # needs to be installed separately with `pip install inflect`

from common import user_folder
from utils.mellanni_modules import open_file_folder

all_files = []
result = pd.DataFrame()
p = inflect.engine()


def get_files(
    folder, extension="xls"
) -> None:  # create a full list of all 'xlsx/xlsm' files in internal folders
    initial_files = os.listdir(folder)
    for file in initial_files:
        full_path = os.path.join(folder, file)
        if os.path.isfile(full_path) and extension in os.path.basename(full_path):
            all_files.append(full_path)
        elif os.path.isdir(full_path):
            get_files(full_path)
    return None


def get_dictionary():
    query = "SELECT sku, collection FROM `auxillary_development.dictionary`"
    client = gc.gcloud_connect()
    dictionary = client.query(query).to_dataframe()
    return dictionary


def count_words(title):  # clean, singularize and count words in a standalone title
    num_list = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    title = (
        str(title)
        .lower()
        .replace(
            ",",
            " ",
        )
        .replace("-", " ")
        .replace(" and ", " ")
        .replace(" a ", " ")
        .replace(" la ", " ")
        .replace(" e ", " ")
        .replace(" di ", " ")
        .replace(" avec ", " ")
        .replace(" cm ", " ")
        .replace(" for ", " ")
        .replace(" in ", " ")
        .replace(" with ", " ")
        .replace("&", " ")
        .replace("(", "   ")
        .replace(")", "")
        .replace('"', "")
        .replace("–", " ")
        .replace(" de ", " ")
        .replace(" con ", " ")
        .replace(" per ", " ")
        .replace("  ", " ")
    )
    all_words = [x.strip() for x in title.split(" ") if x]
    # next, convert all plural words into singular form
    clean_words = [
        p.singular_noun(word.lower()) or word.lower() for word in all_words if word
    ]
    counts = {}
    for word in clean_words:
        if word not in counts:
            counts[word] = 1
        else:
            counts[word] += 1
    counts = {
        word: count
        for word, count in counts.items()
        if count > 2 and word not in num_list
    }
    return counts


def process_file(
    file_path: str,
):  # main processing logic for a single file based on its file path
    global result
    template_sheets = ["Template", "Modèle", "Modello", "Vorlage", "Plantilla"]
    if os.path.basename(file_path).endswith("xlsm"):
        title_col_str = "item_name"
    elif os.path.basename(file_path).endswith("xlsx"):
        title_col_str = "item_name"
    for (
        template
    ) in (
        template_sheets
    ):  # looping over different template sheet names to find the right one
        for row in range(
            0, 5
        ):  # looping over range of 0-5 to find the line with column names
            try:
                df = pd.read_excel(file_path, sheet_name=template, skiprows=row)
                if any([title_col_str in str(x) for x in df.columns]):
                    print(
                        f"Found correct template: {template} and header:{row}, breaking"
                    )
                    break
            except ValueError:
                pass
            except Exception as e:
                print(e)
    sku_col = [
        x
        for x in df.columns
        if any(["contribution_sku" in str(x), "item_sku" in str(x)])
    ][0]
    title_col = [x for x in df.columns if title_col_str in str(x)][0]
    df = df[[sku_col, title_col]]
    df["word_count"] = df[title_col].apply(count_words)
    df = df[df["word_count"] != {}]
    df["file_path"] = file_path
    df = df.rename(columns={title_col: "title", sku_col: "sku"})
    result = pd.concat([result, df])
    return result


class TitleDuplicateChecker(ctk.CTk):

    def __init__(self):
        super().__init__()
        self.geometry("400x200")
        self.title("Check duplicates in titles")
        file_path = ctk.filedialog.askopenfilename(
            title="Select a file with titles", initialdir=user_folder
        )
        file_obj = pd.ExcelFile(file_path)
        if "Template" in file_obj.sheet_names:
            for i in range(0, 8):
                temp_file = pd.read_excel(
                    file_obj, sheet_name="Template", nrows=20, skiprows=i
                )
                if any(["item_name" in x for x in temp_file.columns.tolist()]):
                    self.file = pd.read_excel(
                        file_obj, sheet_name="Template", skiprows=i
                    )
                    break
        else:
            self.file = pd.read_excel(file_path)
        columns = self.file.columns.tolist()

        self.label = ctk.CTkLabel(
            self, text="Select a column with titles that need to be checked"
        )
        self.label.pack(pady=10)
        self.title_cols = ctk.CTkComboBox(self, values=columns)
        self.title_cols.pack(pady=10)
        self.button = ctk.CTkButton(self, text="OK", command=self.process_custom_file)
        self.button.pack(pady=10)

    def process_custom_file(self):
        name_col = self.title_cols.get()
        self.file["word check"] = self.file[name_col].apply(count_words)
        self.file.to_excel(os.path.join(user_folder, "file_check.xlsx"), index=False)
        open_file_folder(user_folder)
        self.destroy()


def main():  # main function that loops over each file in folders
    global all_files
    folder = r"G:\Shared drives\30 Sales\30.1 MELLANNI\30.11 AMAZON\30.111 US\Products\1_Latest Flat Files for checking Titles"
    if not os.path.exists(folder):
        folder = ctk.filedialog.askdirectory(
            title="Select folder with flat files", initialdir=user_folder
        )

    get_files(folder)
    dictionary = get_dictionary()
    for i, file_path in enumerate(all_files):
        print(f"Processing {i+1} of {len(all_files)}")
        try:
            result = process_file(file_path)
        except Exception as e:
            print(e)
            print(file_path)
    result = pd.merge(result, dictionary, how="left", on="sku")
    result.to_excel(
        os.path.join(os.path.expanduser("~"), "temp", "duplicates.xlsx"), index=False
    )
    open_file_folder(os.path.join(os.path.expanduser("~"), "temp"))


def run_custom_file():
    app = TitleDuplicateChecker()
    app.mainloop()


if __name__ == "__main__":
    # main()
    run_custom_file()
