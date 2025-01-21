import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from sensor_data_service import SensorDataService
from common.repository.sensor_data_repository import SensorDataDTO


@pytest.fixture
def mock_repository():
    """SensorDataRepository のモックを作成する"""
    return MagicMock()


@pytest.fixture
def sensor_service(mock_repository):
    """SensorService のインスタンスを作成する"""
    return SensorDataService(repository=mock_repository)


def test_get_sensor_data(sensor_service, mock_repository):
    """get_sensor_data の正常系テスト"""
    tags = ['tag1', 'tag2']
    date = '2024-12-06'
    expected_df = pd.DataFrame({
        'tag': ['tag1', 'tag2'],
        'timestamp': ['2024-12-06 00:00:00', '2024-12-06 01:00:00'],
        'value': [100, 200]
    })

    # モックの戻り値を設定
    mock_repository.fetch_sensor_data.return_value = expected_df

    # メソッドの呼び出し
    result = sensor_service.get_sensor_data(tags, date)

    # 検証
    mock_repository.fetch_sensor_data.assert_called_once_with(
        "batch.data_loader_data_load_temp", tags, date
    )
    pd.testing.assert_frame_equal(result, expected_df)


def test_save_sensor_data(sensor_service, mock_repository):
    """save_sensor_data の正常系テスト"""
    df_to_save = pd.DataFrame({
        'tag': ['tag1', 'tag2'],
        'timestamp': ['2024-12-06 00:00:00', '2024-12-06 01:00:00'],
        'value': [100, 200]
    })

    # モックの戻り値を設定
    mock_repository.save_sensor_data.return_value = True

    # メソッドの呼び出し
    result = sensor_service.save_sensor_data(df_to_save)

    # 検証
    mock_repository.save_sensor_data.assert_called_once_with(
        df_to_save, "batch.data_loader_data_load_temp"
    )
    assert result is True

def test_delete_sensor_data(sensor_service, mock_repository):
    """delete_sensor_data の正常系テスト"""
    tags = ['tag1', 'tag2']
    date = '2024-12-06'

    # モックの戻り値を設定
    mock_repository.delete_sensor_data.return_value = True

    # メソッドの呼び出し
    result = sensor_service.delete_sensor_data(tags, date)

    # 検証
    mock_repository.delete_sensor_data.assert_called_once_with(
        "batch.data_loader_data_load_temp", tags, date
    )
    assert result is True

def test_get_sensor_data_with_none_tags(sensor_service, mock_repository):
    tags = None
    date = '2024-12-06'

    with pytest.raises(ValueError, match="Tags and factory codes cannot be empty"):
        sensor_service.get_sensor_data(tags, date)


def test_get_sensor_data_with_invalid_date(sensor_service, mock_repository):
    tags = ['tag1', 'tag2']
    invalid_date = 'invalid-date'

    mock_repository.fetch_sensor_data.side_effect = ValueError("Invalid date format")

    with pytest.raises(ValueError, match="Invalid date format"):
        sensor_service.get_sensor_data(tags, invalid_date)

def test_save_sensor_data_with_empty_dataframe(sensor_service, mock_repository):
    empty_df = pd.DataFrame()

    with pytest.raises(ValueError, match="DataFrame is empty"):
        sensor_service.save_sensor_data(empty_df)

def test_get_sensor_data_large_dataset(sensor_service, mock_repository):
    tags = [f'tag{i}' for i in range(1000)]
    date = '2024-12-06'
    large_df = pd.DataFrame({
        'tag': tags,
        'timestamp': [f'2024-12-06 {i:02d}:00:00' for i in range(1000)],
        'value': [i for i in range(1000)]
    })

    mock_repository.fetch_sensor_data.return_value = large_df

    result = sensor_service.get_sensor_data(tags, date)

    pd.testing.assert_frame_equal(result, large_df)
    mock_repository.fetch_sensor_data.assert_called_once_with("batch.data_loader_data_load_temp", tags, date)

from common.settings import get_table_name

def test_get_sensor_data_with_missing_table_name(sensor_service, mock_repository, monkeypatch):
    def mock_get_table_name(key):
        raise KeyError(f"Table name for {key} is not configured")

    monkeypatch.setattr("sensor_data_service.get_table_name", mock_get_table_name)  # モジュール名を確認して正しい場所にパッチ
    tags = ['tag1', 'tag2']
    date = '2024-12-06'

    with pytest.raises(KeyError, match="Table name for sensor_data_table is not configured"):
        sensor_service.get_sensor_data(tags, date)


def test_get_sensor_data_as_dto_with_invalid_data(sensor_service, mock_repository):
    tags = ['tag1']
    date = '2024-12-06'

    # モックで不正なデータを返すよう設定
    mock_repository.fetch_as_dto.return_value = [
        {"tag": "tag1", "timestamp": "invalid-timestamp", "value": 100}
    ]

    with pytest.raises(ValueError, match="Invalid data format for DTO conversion"):
        sensor_service.get_sensor_data_as_dto(tags, date)

def test_delete_sensor_data_not_found(sensor_service, mock_repository):
    tags = ['tag1']
    date = '2024-12-06'

    # モックで削除対象が見つからないよう設定
    mock_repository.delete_sensor_data.return_value = False

    result = sensor_service.delete_sensor_data(tags, date)

    assert result is False
    mock_repository.delete_sensor_data.assert_called_once_with("batch.data_loader_data_load_temp", tags, date)


import time

def test_large_dataset_performance(sensor_service, mock_repository):
    """非常に大きなデータセットを処理する際の性能テスト"""
    # テスト用の大量データを準備
    tags = [f'tag{i}' for i in range(100000)]
    date = '2024-12-06'
    large_df = pd.DataFrame({
        'tag': tags,
        'timestamp': [f'2024-12-06 {i % 24:02d}:00:00' for i in range(100000)],
        'value': [i for i in range(100000)]
    })

    mock_repository.fetch_sensor_data.return_value = large_df

    # 性能測定開始
    start_time = time.time()

    # 実行
    result = sensor_service.get_sensor_data(tags, date)

    # 性能測定終了
    elapsed_time = time.time() - start_time

    # 検証
    pd.testing.assert_frame_equal(result, large_df)
    mock_repository.fetch_sensor_data.assert_called_once_with("batch.data_loader_data_load_temp", tags, date)

    # 性能目標：3秒以内に完了する
    assert elapsed_time < 3, f"Performance test failed. Elapsed time: {elapsed_time:.2f}s"


from memory_profiler import memory_usage

def test_large_dataset_memory_usage(sensor_service, mock_repository):
    """大量データ処理時のメモリ使用量を測定"""
    tags = [f'tag{i}' for i in range(100000)]
    date = '2024-12-06'
    large_df = pd.DataFrame({
        'tag': tags,
        'timestamp': [f'2024-12-06 {i % 24:02d}:00:00' for i in range(100000)],
        'value': [i for i in range(100000)]
    })

    mock_repository.fetch_sensor_data.return_value = large_df

    def execute_get_sensor_data():
        return sensor_service.get_sensor_data(tags, date)

    mem_usage = memory_usage((execute_get_sensor_data, ))
    max_memory = max(mem_usage)  # 最大メモリ使用量を取得

    # メモリ使用量が目標範囲内であることを確認
    assert max_memory < 500, f"Memory usage exceeded limit: {max_memory} MB"