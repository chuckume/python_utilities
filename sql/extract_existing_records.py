import pandas as pd

def extract_existing_records(df: pd.DataFrame, other: pd.DataFrame, unique_key:[str]):
    df2 = df.set_index(unique_key)
    other2 = other.set_index(unique_key)

    bool_arr = df2.index.isin(other2.index)

    existing_records =  df[bool_arr]
    new_records = df[~bool_arr]
    return existing_records, new_records