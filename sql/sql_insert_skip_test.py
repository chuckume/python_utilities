from create_sql_engine import create_sql_engine, create_connection_string_sql_server

if __name__ == '__main__':
    server = 'server_name'
    database = 'database_name'
    username = 'username'
    password = 'password'
    
    conn_string = create_connection_string_sql_server(server, database, username, password)
    engine = create_sql_engine(conn_string)

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

