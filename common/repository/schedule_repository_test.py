import pytest
import pandas as pd
from schedule_repository import ScheduleRepository, ScheduleDTO


@pytest.fixture
def repository():
    """テスト用の ScheduleRepository インスタンスを提供するフィクスチャ"""
    return ScheduleRepository()


def test_fetch_schedules(repository):
    """fetch_schedules メソッドのテスト"""
    df = repository.fetch_schedules()

    # スケジュールデータが正しい DataFrame 型か確認
    assert isinstance(df, pd.DataFrame), "結果が DataFrame ではありません"

    # DataFrame のカラムと行数を確認
    assert set(df.columns) == {"schedule_id", "title", "start_time", "end_time", "active"}, "カラムが期待値と異なります"
    assert len(df) == 2, "スケジュールの数が期待値と異なります"

    # 各行の内容を確認
    first_schedule = df.iloc[0]
    assert first_schedule["schedule_id"] == 1, "スケジュール ID が期待値と異なります"
    assert first_schedule["title"] == "Meeting", "タイトルが期待値と異なります"
    assert first_schedule["start_time"] == "10:00", "開始時間が期待値と異なります"
    assert first_schedule["end_time"] == "11:00", "終了時間が期待値と異なります"
    assert first_schedule["active"] == 1, "アクティブ状態が期待値と異なります"


def test_save_schedules(repository, tmp_path):
    """save_schedules メソッドのテスト"""
    # テスト用の DataFrame を用意
    df = pd.DataFrame({
        "schedule_id": [1, 2],
        "title": ["Meeting", "Lunch"],
        "start_time": ["10:00", "12:00"],
        "end_time": ["11:00", "13:00"],
        "active": [1, 0],
    })

    # 保存先ファイルパスを設定
    destination = tmp_path / "schedules.csv"

    # 保存処理を実行
    result = repository.save_schedules(df, destination)

    # 保存結果を確認
    assert result, "スケジュールの保存に失敗しました"
    assert destination.exists(), "保存先のファイルが存在しません"

    # 保存されたファイルの内容を確認
    saved_df = pd.read_csv(destination)
    assert saved_df.equals(df), "保存されたデータが元のデータフレームと一致しません"


def test_fetch_as_dto(repository):
    """fetch_as_dto メソッドのテスト"""
    dtos = repository.fetch_as_dto()

    # DTO のリストであることを確認
    assert isinstance(dtos, list), "結果がリストではありません"
    assert all(isinstance(dto, ScheduleDTO) for dto in dtos), "リスト内の要素が ScheduleDTO ではありません"

    # 各 DTO の内容を確認
    assert len(dtos) == 2, "DTO リストの長さが期待値と異なります"
    first_dto = dtos[0]
    assert first_dto.schedule_id == 1, "スケジュール ID が期待値と異なります"
    assert first_dto.title == "Meeting", "タイトルが期待値と異なります"
    assert first_dto.start_time == "10:00", "開始時間が期待値と異なります"
    assert first_dto.end_time == "11:00", "終了時間が期待値と異なります"
    assert first_dto.active == 1, "アクティブ状態が期待値と異なります"


def test_save_from_dto(repository, tmp_path):
    """save_from_dto メソッドのテスト"""
    # DTO リストを用意
    dtos = [
        ScheduleDTO(1, "Meeting", "10:00", "11:00", 1),
        ScheduleDTO(2, "Lunch", "12:00", "13:00", 0),
    ]

    # 保存先ファイルパスを設定
    destination = tmp_path / "schedules_from_dto.csv"

    # 保存処理を実行
    result = repository.save_from_dto(dtos, destination)

    # 保存結果を確認
    assert result, "DTO リストの保存に失敗しました"
    assert destination.exists(), "保存先のファイルが存在しません"

    # 保存されたファイルの内容を確認
    saved_df = pd.read_csv(destination)

    # DTO リストを DataFrame に変換して比較
    expected_df = pd.DataFrame([dto.__dict__ for dto in dtos])
    assert saved_df.equals(expected_df), "保存されたデータが DTO リストと一致しません"
