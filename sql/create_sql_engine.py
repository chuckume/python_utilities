from sqlalchemy import create_engine


def create_connection_string_sql_server(server:str, database: str, username: str, password: str):
    conn_string = f"mssql+pyodbc://{username}:{password}@{server}/TestCinchy?driver=ODBC+Driver+17+for+SQL+Server"
    return conn_string


def create_sql_engine(conn_string: str):
    engine = create_engine(conn_string)
    return engine

if __name__ == '__main__':
    server = 'server_name'
    database = 'database_name'
    username = 'username'
    password = 'password'
    
    conn_string = create_connection_string_sql_server(server, database, username, password)
    engine = create_sql_engine(conn_string)
