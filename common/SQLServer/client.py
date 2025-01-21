import pyodbc

class ConnectionFactory:
    def __init__(self, server, database, username, password):
        # 接続文字列を初期化する
        self.connection_string = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
        )

    def create_connection(self):
        # データベースへの接続を作成して返す
        return pyodbc.connect(self.connection_string)


class SQLClient:
    def __init__(self, connection_factory):
        # ConnectionFactoryのインスタンスを受け取る
        self.connection_factory = connection_factory

    def execute_query(self, query, params=None):
        with self.connection_factory.create_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params or [])
                return cursor.fetchall()

    def _execute_procedure(self, procedure_name, params=None):
        """
        ストアドプロシージャを実行する共通部分。

        :param procedure_name: 実行するストアドプロシージャの名前
        :param params: ストアドプロシージャのパラメータ（省略可能）
        :return: カーソルオブジェクト
        """
        with self.connection_factory.create_connection() as connection:
            with connection.cursor() as cursor:
                if params:
                    # パラメータ付きでストアドプロシージャを実行
                    query = f"EXEC {procedure_name} " + ", ".join(["?" for _ in params])
                    cursor.execute(query, params)
                else:
                    # パラメータなしでストアドプロシージャを実行
                    query = f"EXEC {procedure_name}"
                    cursor.execute(query)
                return cursor

    def execute_with_result_set(self, procedure_name, params=None):
        """
        結果セットを返すストアドプロシージャを実行する

        :param procedure_name: 実行するストアドプロシージャの名前
        :param params: ストアドプロシージャのパラメータ（省略可能）
        :return: クエリ結果のリスト
        """
        cursor = self._execute_procedure(procedure_name, params)
        return cursor.fetchall()

    def execute_with_output_param(self, procedure_name, params):
        """
        出力パラメータを返すストアドプロシージャを実行する

        :param procedure_name: 実行するストアドプロシージャの名前
        :param params: ストアドプロシージャの入力・出力パラメータ（リスト）
        :return: 出力パラメータのリスト
        """
        cursor = self._execute_procedure(procedure_name, params)
        return [param.value for param in params]  # 出力パラメータの値を取得

    def execute_with_return_code(self, procedure_name, params=None):
        """
        戻り値コードを返すストアドプロシージャを実行する

        :param procedure_name: 実行するストアドプロシージャの名前
        :param params: ストアドプロシージャのパラメータ（省略可能）
        :return: 戻り値コード
        """
        cursor = self._execute_procedure(procedure_name, params)
        return cursor.return_value  # 戻り値コードを取得
