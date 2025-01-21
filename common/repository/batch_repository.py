from common.SQLServer.client import SQLClient

class BatchRepository:
    def __init__(self, sql_client: SQLClient, logger):
        self.sql_client = sql_client
        self.logger = logger

    def execute_stored_procedure_without_result(self, procedure_name, params=None):
        """
        何も返さないストアドプロシージャを実行する

        :param procedure_name: 実行するストアドプロシージャの名前
        :param params: ストアドプロシージャのパラメータ（省略可能）
        """
        self.logger.info(f"Executing stored procedure without result: {procedure_name}")
        try:
            self.sql_client._execute_procedure(procedure_name, params)
            self.logger.info("ストアドプロシージャの実行が正常に完了しました。")
        except Exception as e:
            self.logger.error(f"ストアドプロシージャの実行に失敗しました: {e}")
            raise


    def execute_stored_procedure(self, procedure_name, params=None):
        """
        結果セットを返すストアドプロシージャを実行する

        :param procedure_name: 実行するストアドプロシージャの名前
        :param params: ストアドプロシージャのパラメータ（省略可能）
        :return: クエリ結果のリスト
        """
        self.logger.info(f"Executing stored procedure: {procedure_name}")
        return self.sql_client.execute_with_result_set(procedure_name, params)

    def execute_stored_procedure_with_output_param(self, procedure_name, params):
        """
        出力パラメータを返すストアドプロシージャを実行する

        :param procedure_name: 実行するストアドプロシージャの名前
        :param params: ストアドプロシージャの入力・出力パラメータ（リスト）
        :return: 出力パラメータのリスト
        """
        self.logger.info(f"Executing stored procedure with output params: {procedure_name}")
        return self.sql_client.execute_with_output_param(procedure_name, params)

    def execute_stored_procedure_with_return_code(self, procedure_name, params=None):
        """
        戻り値コードを返すストアドプロシージャを実行する

        :param procedure_name: 実行するストアドプロシージャの名前
        :param params: ストアドプロシージャのパラメータ（省略可能）
        :return: 戻り値コード
        """
        self.logger.info(f"Executing stored procedure with return code: {procedure_name}")
        return self.sql_client.execute_with_return_code(procedure_name, params)

#TODO バッチ処理実行ロジック
    def load_data_to_temp_table(self, bcp_command):
        pass

    #     BCPコマンドを使用して一時テーブルにデータをロードする

    #     :param bcp_command: BCPコマンドの文字列
    #     """
    #     self.logger.info("BCPコマンドを実行中...")
    #     try:
    #         result = subprocess.run(bcp_command, shell=True, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #         self.logger.info(f"BCPコマンドが成功しました: {result.stdout}")
    #     except subprocess.CalledProcessError as e:
    #         self.logger.error(f"BCPコマンドの実行に失敗しました: {e.stderr}")
    #         raise

    def merge_temp_to_main(self, process_date, factory_code):
        """
        一時テーブルからメインテーブルにデータをマージする

        :param process_date: 処理対象日
        :param factory_code: 工場コード
        """
        procedure_name = "batch.data_processing_merge_calculate_data"
        self.logger.info(f"ストアドプロシージャを実行: {procedure_name}")
        try:
            self.execute_stored_procedure_without_result(procedure_name, [process_date, factory_code])
            self.logger.info("ストアドプロシージャの実行が正常に完了しました。")
        except Exception as e:
            self.logger.error(f"ストアドプロシージャの実行に失敗しました: {e}")
            raise
