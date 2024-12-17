
from typing import List
import pandas as pd


class ScheduleDTO:
    """
    スケジュールデータを表現する DTO クラス。
    """
    def __init__(self, schedule_id, title, start_time, end_time, active):
        self.schedule_id = schedule_id
        self.title = title
        self.start_time = start_time
        self.end_time = end_time
        self.active = active

    def __str__(self):
        return (
            f"ScheduleDTO("
            f"id={self.schedule_id}, "
            f"title='{self.title}', "
            f"start_time='{self.start_time}', "
            f"end_time='{self.end_time}', "
            f"active={self.active})"
        )


class ScheduleRepository:
    """
    スケジュールデータを管理するリポジトリクラス。
    """
    def __init__(self, db_connection=None):
        """
        初期化時にデータベース接続を受け取ります。

        :param db_connection: データベース接続オブジェクト
        """
        self.db_connection = db_connection

    def fetch_schedules(self) -> pd.DataFrame:
        """
        スケジュールデータを DataFrame として取得します。

        :return: pd.DataFrame, スケジュールデータフレーム
        """
        # 仮のデータベースクエリ結果
        query_result = [
            {"schedule_id": 1, "title": "Meeting", "start_time": "10:00", "end_time": "11:00", "active": 1},
            {"schedule_id": 2, "title": "Lunch", "start_time": "12:00", "end_time": "13:00", "active": 0},
        ]
        return pd.DataFrame(query_result)

    def save_schedules(self, df: pd.DataFrame, destination: str) -> bool:
        """
        スケジュールデータを指定された場所に保存します（CSV形式）。

        :param df: pd.DataFrame, 保存するスケジュールデータ
        :param destination: str, 保存先のファイルパス
        :return: bool, 保存成功時に True を返す
        """
        try:
            df.to_csv(destination, index=False)
            print(f"スケジュールデータが {destination} に保存されました。")
            return True
        except Exception as e:
            print(f"データ保存中にエラーが発生しました: {e}")
            return False

    def fetch_as_dto(self) -> List['ScheduleDTO']:
        """
        スケジュールデータを DTO リストとして取得します。

        :return: List[ScheduleDTO], DTO のリスト
        """
        df = self.fetch_schedules()
        return [
            ScheduleDTO(
                schedule_id=row["schedule_id"],
                title=row["title"],
                start_time=row["start_time"],
                end_time=row["end_time"],
                active=row["active"]
            )
            for _, row in df.iterrows()
        ]

    def save_from_dto(self, dtos: List['ScheduleDTO'], destination: str) -> bool:
        """
        DTO リストをデータフレームに変換して保存します。

        :param dtos: List[ScheduleDTO], 保存する DTO のリスト
        :param destination: str, 保存先のファイルパス
        :return: bool, 保存成功時に True を返す
        """
        df = pd.DataFrame([dto.__dict__ for dto in dtos])
        return self.save_schedules(df, destination)

