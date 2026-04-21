import os
from tkinter import filedialog

import pandas as pd
from common import user_folder
from utils import mellanni_modules as mm

columns_to_drop = [
    "warranty_description",
    "pesticide_marking_type1",
    "pesticide_marking_registration_status1",
]


replacement = ["quantity", "industry"]


probable_triggers = [
    "acne",
    "allerg",
    "anti",
    "bacter",
    "baseb",
    "contam",
    "designed in",
    "USA",
    "deterior",
    "disinfect",
    "dust ",
    "foul",
    "fung",
    "guarant",
    "insect",
    "irrit",
    "microb",
    "mildew",
    "mite",
    "mold",
    "money",
    "parasit",
    "pestic",
    "repel",
    "sleepnumber",
    "warrant",
    "dust,",
    "dust-",
]


def bulk_process_files(path: str | None = None):
    total_violations = 0
    while not path:
        path = filedialog.askdirectory(
            title="Folder with files to check?", initialdir=user_folder
        )

    files_list = [x for x in os.listdir(path) if any([".csv" in x, "xls" in x])]
    for file in files_list:
        total_violations += check_file(os.path.join(path, file))

    if total_violations == 0:
        print("ALL GOOD, no violations found")
    else:
        print(f"{total_violations} violations found")


def check_file(file):
    violations_found = 0
    if ".csv" in file:
        df = pd.read_csv(file)
    else:
        df = pd.read_excel(file)

    for col in columns_to_drop:
        if col in df.columns:
            del df[col]

    for col in df.columns:
        df[col] = df[col].astype(str)
        row = list(df[col].unique())
        for cell in row:
            cell = cell.lower().replace("quantity", "").replace("industry", "")
            triggers = [x for x in replacement if x in cell]
            if len(triggers) > 0:
                triggers_str = ", ".join(triggers)
                print(
                    f"[WARNING]: flagged words found: {triggers_str} in file {file}, column {col}"
                )
                violations_found += 1
    return violations_found


if __name__ == "__main__":
    # file = filedialog.askopenfilename(initialdir=user_folder)
    # check_file(file)
    bulk_process_files()
