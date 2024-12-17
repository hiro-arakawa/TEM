from dataclasses import dataclass
from typing import List
import pandas as pd
from abc import ABC, abstractmethod

from common.SQLServer.client import SQLClient


# DTO定義: 正規化された形式を反映
@dataclass
class SensorDataDTO:
    factory: str       # 工場名
    tag: str           # タグ名 (例: tag1, tag2)
    timestamp: str     # タイムスタンプ (YYYY-MM-DD HH:MM:SS)
    value: float       # 値 (データ1, データ2, データ3などに対応)


class AbstractSensorDataRepository(ABC):
    @abstractmethod
    def fetch_sensor_data(self, tags: List[str], date: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def save_sensor_data(self, df: pd.DataFrame) -> bool:
        pass


# 本番環境用のリポジトリ
class ProductionSensorDataRepository(AbstractSensorDataRepository):
    def __init__(self, sql_client: SQLClient, logger):
        self.sql_client = sql_client
        self.logger = logger


    def fetch_sensor_data(self, table_name: str, tags: List[str], date: str) -> pd.DataFrame:
        sql_query = f"""
        SELECT 
            [factory], [tag], [date],
            [local_tag], [local_id], [name], [unit], [data_division],
            {', '.join([f'[d{i}_{j}]' for i in range(4) for j in range(30)])},
            [last_update]  -- 追加
        FROM {table_name}
        WHERE [tag] IN ({', '.join(['?' for _ in tags])})
        AND [date] = ?
        """
        params = tags + [date]

        try:
            # SQLクエリの実行
            sql_response = self.sql_client.execute_query(sql_query, params)

            # レスポンスをリストのリストに変換
            sql_response = [list(row) for row in sql_response]  # 必要な変換

            # カラムリスト
            columns = ["factory", "tag", "date", "local_tag", "local_id", "name", "unit", "data_division"] + \
                    [f"d{i}_{j}" for i in range(4) for j in range(30)] + ["last_update"]  # 修正箇所

            # DataFrame作成
            return pd.DataFrame(sql_response, columns=columns)

        except Exception as e:
            raise RuntimeError(f"Error fetching sensor data from table '{table_name}': {e}")

    def save_sensor_data(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        Save sensor data to the specified table in the database using UPDATE for existing rows
        and INSERT for new rows.
        """
        try:
            # 必須カラムと空のデータフレームの早期チェック
            if not {"factory", "tag", "date", "d0_0"}.issubset(df.columns):
                self.logger.error("Data is missing required columns or is empty.")
                return False

            import datetime

            # データフレームに現在の時刻を表す列を追加
            if "last_update" in df.columns:
                self.logger.warning("Overwriting existing 'last_update' column.")
            df['last_update'] = datetime.datetime.now()

            # カラムリスト
            columns = df.columns.tolist()

            # UPDATE用のクエリ生成
            update_query = f"""
            UPDATE {table_name}
            SET {', '.join([f"{col} = ?" for col in columns if col != 'last_update'])},
                last_update = ?
            WHERE factory = ? AND tag = ? AND date = ?
            """

            # INSERT用のクエリ生成
            insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join(['?'] * len(columns))})
            """

            with self.sql_client.connection_factory.create_connection() as connection:
                try:
                    with connection.cursor() as cursor:
                        for _, row in df.iterrows():
                            # UPDATEの実行
                            update_params = [
                                row[col] for col in columns if col != 'last_update'
                            ] + [row['last_update'], row['factory'], row['tag'], row['date']]
                            cursor.execute(update_query, update_params)

                            # INSERTの実行（更新が影響を及ぼさない場合）
                            if cursor.rowcount == 0:
                                insert_params = [row[col] for col in columns]
                                cursor.execute(insert_query, insert_params)

                        connection.commit()
                except Exception as e:
                    connection.rollback()
                    self.logger.error(f"SQL execution error during save operation: {e}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                    return False

            self.logger.info(f"Data successfully saved to {table_name}.")
            return True

        except Exception as e:
            self.logger.error(f"Unexpected error during save operation: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False


    def copy_sensor_data(self, source_table: str, target_table: str, tags: List[str], date: str) -> bool:
        """
        指定したタグと日付のデータを、ソーステーブルからターゲットテーブルにコピーします。
        
        Args:
            source_table (str): データを取得する元テーブル名
            target_table (str): データを保存する先テーブル名
            tags (List[str]): コピー対象のタグのリスト
            date (str): コピー対象の日付（フォーマット例: 'YYYY-MM-DD'）
        
        Returns:
            bool: コピー成功時はTrue、それ以外はFalse
        """
        try:
            # ソーステーブルからデータを取得
            self.logger.error(f"Fetching data from table {source_table} for date {date} and tags {tags}...")
            sql_query = f"""
            SELECT 
                [factory], [tag], [date],
                [local_tag], [local_id], [name], [unit], [data_division],
                {', '.join([f'[d{i}_{j}]' for i in range(4) for j in range(30)])}
            FROM {source_table}
            WHERE [tag] IN ({', '.join(['?' for _ in tags])})
            AND [date] = ?
            """
            params = tags + [date]
            sql_response = self.sql_client.execute_query(sql_query, params)
            
            if not sql_response:
                self.logger.error("No data found for the given tags and date.")
                return False

            # レスポンスをデータフレームに変換
            columns = ["factory", "tag", "date", "local_tag", "local_id", "name", "unit", "data_division"] + \
                      [f"d{i}_{j}" for i in range(4) for j in range(30)]
            df = pd.DataFrame([list(row) for row in sql_response], columns=columns)

            # ターゲットテーブルにデータを保存
            self.logger.error(f"Saving data to table {target_table}...")
            save_result = self.save_sensor_data(df, target_table)

            if save_result:
                self.logger.error("Data copy operation completed successfully.")
                return True
            else:
                self.logger.error("Data copy operation failed during save.")
                return False

        except Exception as e:
            self.logger.error(f"Error during copy_sensor_data operation: {e}")
            return False

    def delete_sensor_data(self, table_name: str, tags: List[str], date: str) -> bool:
        """
        指定されたテーブルから、タグと日付に基づいてセンサーのデータを削除します。
        """
        try:
            tag_placeholders = ", ".join(["?"] * len(tags))
            delete_query = f"""
            DELETE FROM {table_name}
            WHERE [tag] IN ({tag_placeholders})
            AND [date] = ?
            """
            params = tags + [date]

            with self.sql_client.connection_factory.create_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(delete_query, params)
                    deleted_rows = cursor.rowcount
                    connection.commit()

            if deleted_rows == 0:
                self.logger.warning(f"No rows deleted from {table_name} for tags: {tags} and date: {date}.")
                return False

            self.logger.info(f"Deleted {deleted_rows} rows from {table_name}.")
            return True

        except Exception as e:
            self.logger.error(f"Error during delete operation: {e}")
            return False


# テスト用リポジトリ: ダミーデータを生成
class TestSensorDataRepository(AbstractSensorDataRepository):
    def __init__(self, valid_tags=None, logger=None):
        """
        初期化時に有効なタグリストを設定します。
        """
        self.valid_tags = valid_tags if valid_tags is not None else ["sensor_1", "sensor_2"]
        self.logger = logger

    def fetch_sensor_data(self, table_name: str, tags: List[str], date: str) -> pd.DataFrame:
        """
        SQL Serverからのレスポンスを模倣したデータを生成。
        """
        mock_sql_data = self.generate_mock_sql_response(tags, date)
        sensor_df = pd.DataFrame(mock_sql_data, columns=[
            "factory", "tag", "date", "local_tag", "local_id", "name", "unit", "data_division"
        ] + [f"d{i}_{j}" for i in range(4) for j in range(30)])

        self.logger.error("Generated mock SQL response data (fetch_sensor_data):")
        self.logger.error(sensor_df)
        return sensor_df
        
    def generate_mock_sql_response(self, tags: List[str], date: str) -> List[dict]:
            """SQL Serverのレスポンスを模倣した辞書リストを生成。"""
            import numpy as np

            mock_sql_data = []
            try:
                base_date = pd.Timestamp(date)
            except Exception as e:
                raise ValueError("Invalid date format") from e

            for tag in tags:
                if tag not in self.valid_tags:  # 無効なタグはスキップ
                    continue
                row = {
                    "factory": "A",
                    "tag": tag,
                    "date": base_date.strftime("%Y-%m-%d"),
                    "local_tag": f"local_{tag}",
                    "local_id": f"id_{tag}",
                    "name": f"Sensor_{tag}",
                    "unit": "℃",
                    "data_division": "temperature",
                }
                for i in range(30):
                    row[f"d0_{i}"] = 1 if np.random.rand() <= 0.95 else np.random.randint(2, 10)
                    row[f"d1_{i}"] = np.random.randint(100, 200)
                    row[f"d2_{i}"] = np.random.randint(50, 100)
                    row[f"d3_{i}"] = np.random.randint(30, 80)
                mock_sql_data.append(row)

            return mock_sql_data

    def save_sensor_data(self, df: pd.DataFrame, table_name: str) -> bool:
        """保存処理のシミュレーション: 必須列チェックを追加。"""
        required_columns = ["factory", "tag", "date", "d0_0"]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False  # 必須列が欠損している場合は失敗

        if df.empty:
            self.logger.error(f"No data to save for table: {table_name}")
            return False  # 空データは保存失敗を返す

        self.logger.error(f"Simulated saving data to table: {table_name}")
        return True


    def copy_sensor_data(self, source_table: str, target_table: str, tags: List[str], date: str) -> bool:
        """
        モックデータを使用して、データコピー処理をシミュレート。
        """
        self.logger.error(f"Copying data from {source_table} to {target_table} for tags: {tags} and date: {date}")
        source_data = self.fetch_sensor_data(tags, date)
        if source_data.empty:
            self.logger.error(f"No data found in {source_table} for the given tags and date.")
            return False
        return self.save_sensor_data(source_data, target_table)

    def delete_sensor_data(self, table_name: str, tags: List[str], date: str) -> bool:
        """
        モックデータから指定条件に一致するデータを削除。
        """
        self.logger.error(f"Deleting data from {table_name} for tags: {tags} and date: {date}")
        before_count = len(self.mock_data)
        self.mock_data = [row for row in self.mock_data
                          if not (row["tag"] in tags and row["date"] == date)]
        after_count = len(self.mock_data)
        deleted_count = before_count - after_count
        self.logger.error(f"Deleted {deleted_count} rows from {table_name}.")
        return True

# センサーデータリポジトリ: 共通のインターフェース
class SensorDataRepository:
    def __init__(self, repository: AbstractSensorDataRepository, logger):
        self.repository = repository
        self.logger=logger

    def fetch_sensor_data(self, table_name: str, tags: List[str], date: str) -> pd.DataFrame:
        """
        非正規化形式でデータを取得。
        """
        return self.repository.fetch_sensor_data(table_name, tags, date)

    def fetch_as_dto(self, table_name: str, tags: List[str], date: str) -> List[SensorDataDTO]:
        """
        センサーデータをDTOリストとして取得。
        """
        # 共通処理: fetch_sensor_data を利用してデータを取得
        df = self.fetch_sensor_data(table_name, tags, date)
        if df.empty:
            self.logger.error("fetch_as_dto: Received empty DataFrame.")
            return []

        dtos = []
        for _, row in df.iterrows():
            for hour in range(30):  # 30 時間分
                for prefix in ["d0", "d1", "d2", "d3"]:
                    timestamp = f"{row['date']} {hour:02d}:00:00"  # 修正: hour をゼロ埋めで 2 桁に
                    value = row[f"{prefix}_{hour}"]
                    dtos.append(SensorDataDTO(
                        factory=row["factory"],
                        tag=row["tag"],
                        timestamp=timestamp,
                        value=value
                    ))

        self.logger.error(f"Generated DTOs: {dtos[:5]}")  # 最初の5件を表示
        return dtos


    def save_sensor_data(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        データを指定されたテーブル名に保存します。
        """
        return self.repository.save_sensor_data(df, table_name)
    
    def delete_sensor_data(self, table_name: str, tags: List[str], date: str) -> bool:
        """
        データ削除処理のラップ。
        """
        return self.repository.delete_sensor_data(table_name, tags, date)
    
    def copy_sensor_data(self, source_table: str, target_table: str, tags: List[str], date: str) -> bool:
        """
        データコピー処理のラップ。
        """
        return self.repository.copy_sensor_data(source_table, target_table, tags, date)