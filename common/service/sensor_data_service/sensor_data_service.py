from typing import List
import pandas as pd
from common.repository.sensor_data_repository import SensorDataRepository,SensorDataDTO
from common.settings import get_table_name

class SensorDataService:
    """
    センサーデータを操作するためのサービスクラス。
    Repository を通じてセンサーデータを取得・加工する。
    """

    def __init__(self, repository: SensorDataRepository):
        """
        初期化メソッド。

        :param repository: SensorDataRepository のインスタンス
        """
        self.repository = repository

    def get_sensor_data(self, tags: List[str], date: str) -> pd.DataFrame:
        """
        指定されたタグと日付に基づいてセンサーデータを取得し、DataFrame 形式で返す。

        :param tags: List[str], 取得するタグのリスト
        :param date: str, データを取得する対象の日付（YYYY-MM-DD形式）
        :return: pd.DataFrame, センサーデータフレーム
        """
        if tags is None:
            raise ValueError("Tags list cannot be None")
        try:
            sensor_table = get_table_name("sensor_data_table")  # 設定からテーブル名を取得
        except KeyError as e:
            raise KeyError(f"Table name for sensor_data_table is not configured") from e
        return self.repository.fetch_sensor_data(sensor_table, tags, date)

    def save_sensor_data(self, df: pd.DataFrame) -> bool:
        """
        センサーデータを保存する。

        :param df: pd.DataFrame, 保存するセンサーデータ
        :param destination: str, 保存先
        :return: bool, 成功した場合 True
        """
        if df.empty:
            raise ValueError("DataFrame is empty")
        sensor_table = get_table_name("sensor_data_table")
        return self.repository.save_sensor_data(df, sensor_table)

    def save_calculation_result(self, df: pd.DataFrame) -> bool:
        """
        計算結果を保存する。

        :param df: pd.DataFrame, 保存する計算結果データ
        :return: bool, 成功した場合 True
        """
        calc_table = get_table_name("calculation_result_table")
        return self.repository.save_sensor_data(df, calc_table)

    def delete_sensor_data(self, tags: List[str], date: str) -> bool:
        """
        sensor_data テーブルから指定されたタグと日付に基づいてデータを削除する。

        :param tags: List[str], 削除対象のタグリスト
        :param date: str, 削除対象の日付（YYYY-MM-DD形式）
        :return: bool, 成功した場合 True
        """
        sensor_table = get_table_name("sensor_data_table")
        return self.repository.delete_sensor_data(sensor_table, tags, date)

    def delete_calculation_result(self, tags: List[str], date: str) -> bool:
        """
        calculation_result テーブルから指定されたタグと日付に基づいてデータを削除する。

        :param tags: List[str], 削除対象のタグリスト
        :param date: str, 削除対象の日付（YYYY-MM-DD形式）
        :return: bool, 成功した場合 True
        """
        calc_table = get_table_name("calculation_result_table")
        return self.repository.delete_sensor_data(calc_table, tags, date)

    def get_sensor_data_as_dto(self, tags: List[str], date: str) -> List[SensorDataDTO]:
        """
        指定されたタグと日付に基づいて正規化されたDTO形式でセンサーデータを取得する。

        :param tags: List[str], 取得するタグのリスト
        :param date: str, データを取得する対象の日付（YYYY-MM-DD形式）
        :return: List[SensorDataDTO], センサーデータDTOリスト
        """
        dtos = self.repository.fetch_as_dto(tags, date)
        for dto in dtos:
            if not isinstance(dto, SensorDataDTO):
                raise ValueError("Invalid data format for DTO conversion")
        return dtos
