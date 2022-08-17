import pandas as pd
from sqlalchemy.engine.base import Engine

def import_csv_to_sql(df: pd.DataFrame, engine: Engine, table_name:str, db_schema:str = 'dbo', chunksize=None): 
    '''
        Tested in SQL Server
    '''


    result = df.to_sql(table_name,
              con=engine ,
              schema=db_schema,
              if_exists='append',
              index=False,
              chunksize=chunksize,
              dtype=None)

    print("IMPORT RESULT :", result)