
#TODO 設定は一か所に寄せる
TABLE_NAME_SETTINGS = {
    "sensor_data_table": "batch.data_loader_data_load_temp",
    "calculation_result_table": "batch.data_processing_calculation_temp"
}
    
@staticmethod
def get_table_name(key: str) -> str:
    """
    静的メソッドでテーブル名を取得。
    :param key: str, テーブルタイプのキー（例: sensor_data_table）
    :return: str, テーブル名
    """
    if key in TABLE_NAME_SETTINGS:
        return TABLE_NAME_SETTINGS[key]
    raise KeyError(f"Table name for key '{key}' not found.")