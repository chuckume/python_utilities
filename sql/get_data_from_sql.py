import pandas as pd
from sqlalchemy.engine.base import Engine


def get_data_from_sql(sql_script:str, engine:Engine) -> pd.DataFrame:
    sql_query = pd.read_sql_query(sql_script, engine)
    df = pd.DataFrame(sql_query)
    return df