import pandas as pd
import pytz


# check for columns with dates and timezones and remove timezones for export to Excel
def convert_timezones(df: pd.DataFrame):
    columns = df.columns.tolist()
    for column in columns:
        if (
            pd.api.types.is_datetime64_any_dtype(df[column])
            and df[column].dt.tz is not None
        ):
            df[column] = df[column].dt.tz_localize(None)
