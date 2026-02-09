import sys

import pandas_gbq
from connectors import gcloud as gc
from connectors import gdrive as gd

DICIONARY_ID = "1Y4XhSBCXqmEVHHOnugEpzZZ3NQ5ZRGOlp-AsTE0KmRE"
SHEET_ID = "449289593"
DESTINATION_TABLE = "mellanni-project-da.auxillary_development.dictionary"


def download_dictionary():
    try:
        dictionary = gd.download_gspread(spreadsheet_id=DICIONARY_ID, sheet_id=SHEET_ID)
        return dictionary
    except Exception as e:
        print(f"Error reading dictionary: {e}")


def upload_dictionary(dictionary):
    try:
        dictionary = gc.normalize_columns(dictionary)
        _ = pandas_gbq.to_gbq(
            dataframe=dictionary,
            destination_table=DESTINATION_TABLE,
            if_exists="replace",
            credentials=gc.get_credentials(),
        )
        return True
    except Exception as e:
        print(f"Could not upload dictionary: {e}")
        return False


def main():
    dictionary = download_dictionary()
    if dictionary is None:
        sys.exit(1)
    result = upload_dictionary(dictionary)
    if result:
        print("upload successful")


if __name__ == "__main__":
    main()
