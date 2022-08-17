import pandas as pd
from sqlalchemy.engine.base import Engine
from extract_existing_records import extract_existing_records
from get_data_from_sql import get_data_from_sql


def get_data_from_sql(sql_script:str, engine:Engine) -> pd.DataFrame:
    sql_query = pd.read_sql_query(sql_script, engine)
    df = pd.DataFrame(sql_query)
    return df


def insert_skip(df: pd.DataFrame, pk_columns:[str], engine: Engine, table_name: str):
    if len(pk_columns)>0:
        # pk_columns = ['PK1', 'PK2']
        pks = ', '.join(pk_columns)
        sql_script =  f"SELECT Distinct {pks} FROM {table_name}"
        old_data = get_data_from_sql(sql_script, engine)

        _, new = extract_existing_records(df, old_data, pk_columns)

        new.to_sql(table_name, engine, if_exists='append', index=False)
    else:
        df.to_sql(table_name, engine, if_exists='append', index=False)


if __name__ == '__main__':
    from commun.config_management.get_config import get_config
    from commun.sql.config.config import ConfigSQL
    from commun.sql.credentials.credentials import CredentialsSQL
    from commun.sql.create_engine import create_sql_engine
    config_sql: ConfigSQL = get_config(ConfigSQL)
    credentials_sql: CredentialsSQL = get_config(CredentialsSQL)

    print(config_sql)
    engine = create_sql_engine(config_sql, credentials_sql)
    # print(type(engine))
    # d = {'A' : [1, 1, 3, 4], 'B' : [4, 3, 2, 1]}
    d = [[1, 1, 'A'],
         [1, 2, 'B'],
         [2, 1, 'C'],
         [3, 1, 'D']]
    data = pd.DataFrame(d, columns=['PK1', 'PK2', 'val'])

    data.to_csv('test.csv')


    #Create a table with a unique constraint on A.
    engine.execute("""DROP TABLE IF EXISTS test_upsert """)
    engine.execute("""CREATE TABLE test_upsert (
                      id INTEGER IDENTITY(1,1) ,
                      PK1 INTEGER,
                      Pk2 INTEGER,
                      Val VARCHAR(MAX),
                      PRIMARY KEY (PK1, PK2))
                      """)

    table_name = 'test_upsert'
    data.to_sql(table_name, engine, if_exists='append', index=False)


    d = [[1, 2, 'E'],
         [4, 1, 'F']]
    new_data = pd.DataFrame(d, columns=['PK1', 'PK2', 'val'])

    primary_keys = ["PK1", "PK2"]
    insert_skip(new_data, primary_keys, engine)

    d = [[1, 1, 'A'],
         [1, 2, 'B'],
         [2, 1, 'C'],
         [3, 1, 'D'],
         [4, 1, 'F']]
    expected = pd.DataFrame(d, columns=['PK1', 'PK2', 'val'])

    sql_script =  f"SELECT PK1, PK2, val  FROM {table_name}"
    result = get_data_from_sql(sql_script, engine)


    assert result.equals(expected)
    # pk_columns = ['PK1', 'PK2']
    # pks = ', '.join(pk_columns)
    # sql_script =  f"SELECT Distinct {pks} FROM {table_name}"
    # sql_query = pd.read_sql_query(sql_script, engine)
    # df = pd.DataFrame(sql_query)
    # # df = pd.DataFrame(sql_query, columns=['product_id', 'product_name', 'price'])
    # print(df)
    # existing, new = extract_existing_records(new_data, df, pk_columns)


    # new.to_sql('test_upsert', engine, if_exists='append', index=False)
