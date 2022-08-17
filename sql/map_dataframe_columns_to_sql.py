import pandas as pd

def map_dataframe_columns_to_sql(df: pd.DataFrame, mapping_dictionary: dict):
    df = df.rename(columns=mapping_dictionary)
    columns = list(mapping_dictionary.values())
    drop_columns = [col for col in df.columns if col not in columns]
    df = df.drop(drop_columns, axis=1)
    return df