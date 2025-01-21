from common.repository.batch_repository import BatchRepository

class SensorDataBatchService:
    def __init__(self, repository: BatchRepository, logger):
        self.repository = repository
        self.logger = logger

    def process_sensor_data_batch(self, bcp_command, factory_code, process_date):
        """
        センサーデータを一時テーブルにロードし、メインテーブルにマージする。

        :param bcp_command: 一時テーブルにロードするためのBCPコマンド
        :param process_date: 処理対象日
        :param factory_code: 工場コード
        :return: 処理結果
        """
        try:
            self.logger.info("センサーデータのバッチ処理を開始します。")
            
            # データを一時テーブルにロード
            self.repository.load_data_to_temp_table(bcp_command)
            
            # 一時テーブルからメインテーブルにマージ
            self.repository.merge_temp_to_main(process_date, factory_code)

            self.logger.info("センサーデータのバッチ処理が正常に完了しました。")
            return {"status": "success", "message": "センサーデータのバッチ処理が完了しました。"}
        
        except Exception as e:
            self.logger.error(f"バッチ処理中にエラーが発生しました: {e}")
            return {"status": "error", "message": str(e)}


