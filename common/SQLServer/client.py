import pyodbc


class ConnectionFactory:
    def __init__(self, server, database, username, password):
        self.connection_string = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
        )

    def create_connection(self):
        return pyodbc.connect(self.connection_string)


# sql_client.py
class SQLClient:
    def __init__(self, connection_factory):
        self.connection_factory = connection_factory

    def execute_query(self, query, params=None):
        with self.connection_factory.create_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params or [])
                return cursor.fetchall()
