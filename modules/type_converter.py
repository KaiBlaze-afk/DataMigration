import pandas as pd
from bson import ObjectId

def convert_objectid(value):
    if isinstance(value, ObjectId):
        return str(value)
    return value

def infer_mysql_type(series):
    series = series.dropna()

    if len(series) == 0:
        return "TEXT"

    sample = series.iloc[0]

    if isinstance(sample, bool):
        return "TINYINT(1)"

    if isinstance(sample, int):
        return "BIGINT"

    if isinstance(sample, float):
        if series.apply(lambda x: isinstance(x, float) and x.is_integer()).all():
            return "BIGINT"
        return "DECIMAL(20,6)"

    if pd.api.types.is_datetime64_any_dtype(series):
        return "DATETIME"

    if isinstance(sample, str):
        max_len = series.astype(str).str.len().max()
        if max_len <= 255:
            return f"VARCHAR({min(max_len + 50, 255)})"
        else:
            return "TEXT"

    return "TEXT"

def prepare_dataframe_for_mysql(df):
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].apply(convert_objectid)
    df = df.astype(object).where(pd.notnull(df), None)
    return df

def get_column_types(df):
    column_types = {}
    for col in df.columns:
        mysql_type = infer_mysql_type(df[col])
        column_types[col] = mysql_type
    return column_types
