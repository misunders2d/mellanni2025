import re
import textwrap
from pathlib import Path
from tkinter import filedialog

import pandas as pd
from common import user_folder

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
    "dust",
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
]

supported_extensions = {".csv", ".xls", ".xlsx", ".xlsm"}
ignored_words_pattern = re.compile(
    r"\b(?:" + "|".join(re.escape(word) for word in replacement) + r")\b"
)
trigger_patterns = []
for trigger in probable_triggers:
    pattern = re.escape(trigger.casefold())
    # Allergy claims also occur inside words, e.g. "hypoallergenic".
    if trigger != "allerg":
        pattern = r"(?<!\w)" + pattern
    # Country acronyms are whole words, so "usage" does not match "USA".
    if trigger.isupper():
        pattern += r"(?!\w)"
    trigger_patterns.append((trigger, re.compile(pattern)))


def normalize_column_name(column):
    name = re.sub(r"\[[^\]]*\]|#\d+", "", str(column).strip().casefold())
    return name.removesuffix(".value")


def is_ignored_column(column):
    name = normalize_column_name(column)
    if name in {
        "pesticide_marking.marking_type",
        "pesticide_marking.registration_status",
    }:
        return True
    return re.sub(r"\d+$", "", name) in {
        re.sub(r"\d+$", "", column) for column in columns_to_drop
    }


def bulk_process_files(path: str | None = None):
    total_violations = 0
    if not path:
        path = filedialog.askdirectory(
            title="Folder with files to check?", initialdir=user_folder
        )
        if not path:
            print("Check cancelled. No files scanned.")
            return

    folder = Path(path)
    if not folder.is_dir():
        print(f"Cannot check folder: {folder}")
        return

    files_list = sorted(
        file
        for file in folder.iterdir()
        if file.is_file()
        and file.suffix.lower() in supported_extensions
        and not file.name.startswith("~$")
    )
    if not files_list:
        print("No supported CSV/Excel files found. No files scanned.")
        return

    print("Flagged words check")
    print(f"Folder: {folder}")
    print(f"Files to check: {len(files_list)}")

    failed_files = 0
    for file in files_list:
        try:
            total_violations += check_file(file)
        except Exception as error:
            failed_files += 1
            print(f"\nFile: {file.name}")
            print(f"  Could not scan: {error}")

    checked_files = len(files_list) - failed_files
    print("\nSummary")
    print(f"  Files scanned: {checked_files}/{len(files_list)}")
    print(f"  Flagged cells: {total_violations}")
    print(f"  Files failed: {failed_files}")
    if failed_files:
        print("  Result: INCOMPLETE - resolve failed files and run again.")
    elif total_violations == 0:
        print("  Result: No flagged words found.")
    else:
        print("  Result: REVIEW NEEDED - check the cells listed above.")


def check_file(file):
    file = Path(file)
    violations_found = 0
    sheet_name = None
    header_row = 0
    if file.suffix.lower() == ".csv":
        df = pd.read_csv(file, dtype=str, keep_default_na=False)
    else:
        with pd.ExcelFile(file) as workbook:
            sheet_name = (
                "Template"
                if "Template" in workbook.sheet_names
                else workbook.sheet_names[0]
            )
            df = pd.read_excel(
                workbook,
                sheet_name=sheet_name,
                header=None,
                dtype=str,
                keep_default_na=False,
            )
        # Prefer contribution_sku; broader SKU names beat a lone display label.
        sku_rows = [
            (index, str(value).strip().casefold())
            for index, row in df.iterrows()
            for value in row
            if "sku" in str(value).casefold()
        ]
        header_row = next(
            (index for index, name in sku_rows if "contribution_sku" in name),
            next(
                (index for index, name in sku_rows if name != "sku"),
                sku_rows[0][0] if sku_rows else 0,
            ),
        )
        if not df.empty:
            headers = [
                str(value).strip() or f"Column {index + 1}"
                for index, value in enumerate(df.iloc[header_row])
            ]
            df = df.iloc[header_row + 1 :].copy()
            df.columns = headers

    if df.empty:
        raise ValueError("No data rows found")

    ignored_columns = [col for col in df.columns if is_ignored_column(col)]
    df = df.drop(columns=ignored_columns)

    findings = []
    flagged_rows = set()
    for col, values in df.items():
        for row_number, cell in enumerate(values, start=header_row + 2):
            cell = str(cell)
            normalized_cell = ignored_words_pattern.sub(" ", cell.casefold())
            triggers = [
                trigger
                for trigger, pattern in trigger_patterns
                if pattern.search(normalized_cell)
            ]
            if triggers:
                findings.append((row_number, col, triggers, cell))
                flagged_rows.add(row_number)
                violations_found += 1

    print(f"\nFile: {file.name}")
    if sheet_name is not None:
        fallback_info = (
            " (Template not found; using first sheet)"
            if sheet_name != "Template"
            else ""
        )
        print(f"  Sheet: {sheet_name}{fallback_info}")
        print(f"  Header: row {header_row + 1}")
    if ignored_columns:
        print(f"  Skipped metadata columns: {len(ignored_columns)}")
    if not findings:
        print("  No flagged words found.")
    else:
        cell_label = "cell" if violations_found == 1 else "cells"
        row_label = "row" if len(flagged_rows) == 1 else "rows"
        print(
            f"  Review needed: {violations_found} flagged {cell_label} "
            f"across {len(flagged_rows)} {row_label}"
        )
        for row_number, col, triggers, cell in sorted(findings, key=lambda item: item[0]):
            print(f"\n  Row {row_number} | Column: {col}")
            labels = [
                "allergy-related wording" if trigger == "allerg" else trigger
                for trigger in triggers
            ]
            print(f"    Flags: {', '.join(labels)}")
            print(
                textwrap.fill(
                    repr(cell),
                    width=100,
                    initial_indent="    Text: ",
                    subsequent_indent="          ",
                )
            )
    return violations_found


if __name__ == "__main__":
    bulk_process_files()
