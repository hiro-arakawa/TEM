import pytest
import pandas as pd
from unittest.mock import MagicMock,ANY
from common.repository.sensor_data_repository import SensorDataRepository, SensorDataDTO, TestSensorDataRepository


@pytest.fixture
def logger_mock():
    """ロガーのモック"""
    return MagicMock()


@pytest.fixture
def production_repository(logger_mock):
    """ProductionSensorDataRepository を使用した SensorDataRepository のテスト用インスタンスを提供"""
    from common.repository.sensor_data_repository import ProductionSensorDataRepository
    from common.SQLServer.client import SQLClient
    sql_client_mock = MagicMock(spec=SQLClient)
    production_repo = ProductionSensorDataRepository(sql_client_mock, logger_mock)
    return SensorDataRepository(production_repo, logger_mock)


@pytest.fixture
def sensor_repository(logger_mock):
    """TestSensorDataRepository を使用した SensorDataRepository のテスト用インスタンスを提供"""
    test_repository = TestSensorDataRepository(logger=logger_mock)
    return SensorDataRepository(test_repository, logger_mock)


# TestSensorDataRepository のテスト
def test_sensor_repository_fetch_data(sensor_repository):
    """TestSensorDataRepository の fetch_sensor_data が正しいデータを返すかを検証"""
    tags = ["sensor_1", "sensor_2"]
    date = "2024-12-31"
    table_name = "sensor_data_table"

    # 実行
    result = sensor_repository.fetch_sensor_data(table_name, tags, date)

    # 検証
    assert not result.empty, "DataFrame が空です"
    assert "factory" in result.columns, "カラム 'factory' が欠損しています"
    assert len(result["tag"].unique()) == 2, "取得されたタグの数が正しくありません"


def test_sensor_repository_fetch_data_invalid_tag(sensor_repository):
    """TestSensorDataRepository で存在しないタグを指定した場合の fetch_sensor_data の動作を検証"""
    tags = ["invalid_tag"]
    date = "2024-12-31"
    table_name = "sensor_data_table"

    # 実行
    result = sensor_repository.fetch_sensor_data(table_name, tags, date)

    # 検証
    assert result.empty, "無効なタグでデータが取得されています"


def test_sensor_repository_save_data(sensor_repository):
    """TestSensorDataRepository の save_sensor_data が正しいデータで成功するかを検証"""
    table_name = "sensor_data_table"
    data = {
        "factory": ["A", "A"],
        "tag": ["sensor_1", "sensor_2"],
        "date": ["2024-12-31", "2024-12-31"],
        "local_tag": ["local_1", "local_2"],
        "local_id": ["id_1", "id_2"],
        "name": ["Sensor_1", "Sensor_2"],
        "unit": ["℃", "℃"],
        "data_division": ["temperature", "temperature"],
        "d0_0": [10, 20],
        "d0_1": [15, 25],
    }
    df = pd.DataFrame(data)

    # 実行
    result = sensor_repository.save_sensor_data(df, table_name)

    # 検証
    assert result, "データ保存が失敗しました"


def test_sensor_repository_save_data_empty(sensor_repository):
    """TestSensorDataRepository の save_sensor_data が空のデータで失敗するかを検証"""
    table_name = "sensor_data_table"
    empty_df = pd.DataFrame()

    # 実行
    result = sensor_repository.save_sensor_data(empty_df, table_name)

    # 検証
    assert not result, "空データの保存が成功しています"


def test_sensor_repository_save_data_missing_columns(sensor_repository):
    """TestSensorDataRepository の save_sensor_data が必須列欠損時に失敗するかを検証"""
    table_name = "sensor_data_table"
    invalid_df = pd.DataFrame({"factory": ["A"], "tag": ["sensor_1"]})  # 必須列が欠損

    # 実行
    result = sensor_repository.save_sensor_data(invalid_df, table_name)

    # 検証
    assert not result, "必須列欠損時の保存が成功しています"


# ProductionSensorDataRepository のテスト
def test_production_repository_fetch_data(production_repository, mocker):
    """ProductionSensorDataRepository の fetch_sensor_data が正しい SQL 応答を DataFrame に変換するかを検証"""
    table_name = "sensor_data_table"
    tags = ["sensor_1"]
    date = "2024-12-31"

    # モックした SQL 応答
    mock_sql_response = [
        ["A", "sensor_1", "2024-12-31", "local_tag1", "local_id1", "name1", "unit1", "division1"] +
        [100] * 120 + [None]  # `last_update` を含む
    ]
    mock_columns = ["factory", "tag", "date", "local_tag", "local_id", "name", "unit", "data_division"] + \
                   [f"d{i}_{j}" for i in range(4) for j in range(30)] + ["last_update"]
    mock_df = pd.DataFrame(mock_sql_response, columns=mock_columns)

    # SQL クエリの結果をモック
    mocker.patch.object(production_repository.repository.sql_client, "execute_query", return_value=mock_sql_response)

    # 実行
    result = production_repository.fetch_sensor_data(table_name, tags, date)

    # 検証
    pd.testing.assert_frame_equal(result, mock_df, check_dtype=False)

def test_production_repository_save_data_missing_columns(production_repository):
    """ProductionSensorDataRepository の save_sensor_data が必須列欠損時にエラーを返すかを検証"""
    table_name = "sensor_data_table"
    invalid_df = pd.DataFrame({"factory": ["A"], "tag": ["sensor_1"]})  # 必須列が欠損

    # 実行
    result = production_repository.save_sensor_data(invalid_df, table_name)

    # 検証
    assert not result, "必須列欠損時にエラーが発生しませんでした"

def test_production_repository_save_sql_error(production_repository, mocker):
    """save_sensor_data が SQL エラー時に適切に失敗するかを検証"""
    table_name = "sensor_data_table"
    data = {
        "factory": ["A"],
        "tag": ["sensor_1"],
        "date": ["2024-12-31"],
        "d0_0": [10],
    }
    df = pd.DataFrame(data)

    # SQL エラーをモック
    mock_cursor = MagicMock()
    mock_cursor.executemany.side_effect = RuntimeError("SQL execution failed")
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_sql_client = MagicMock()
    mock_sql_client.connection_factory.create_connection.return_value = mock_connection
    mocker.patch.object(production_repository.repository, "sql_client", mock_sql_client)

    # 実行
    try:
        result = production_repository.save_sensor_data(df, table_name)
    except Exception as e:
        print(f"Exception raised during save_sensor_data execution: {e}")

    # デバッグ用の出力
    print(f"Mock calls on executemany: {mock_cursor.executemany.mock_calls}")
    print(f"Mock calls on connection: {mock_connection.mock_calls}")
    print(f"Data passed to executemany: {df.values.tolist()}")

    # 検証
    assert not result, "SQL エラー時に保存処理が成功してしまいました"


def test_production_repository_save_empty_data(production_repository):
    """save_sensor_data が空の DataFrame を処理するときに失敗するかを検証"""
    table_name = "sensor_data_table"
    empty_df = pd.DataFrame()

    # 実行
    result = production_repository.save_sensor_data(empty_df, table_name)

    # 検証
    assert not result, "空データで保存処理が成功してしまいました"

def test_production_repository_save_large_data(production_repository, mocker):
    """save_sensor_data が大量データで成功するかを検証"""
    table_name = "sensor_data_table"
    # 大量データ生成
    data = {
        "factory": ["A"] * 10000,
        "tag": [f"sensor_{i}" for i in range(10000)],
        "date": ["2024-12-31"] * 10000,
        "d0_0": [10] * 10000,
    }
    large_df = pd.DataFrame(data)

    # SQL 実行のモック
    mock_cursor = MagicMock()
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_sql_client = MagicMock()
    mock_sql_client.connection_factory = MagicMock()
    mock_sql_client.connection_factory.create_connection.return_value = mock_connection
    mocker.patch.object(production_repository.repository, "sql_client", mock_sql_client)

    # 実行
    result = production_repository.save_sensor_data(large_df, table_name)

    # 検証
    assert result, "大量データで保存処理が失敗しました"

def test_production_repository_save_sql_error(production_repository, mocker):
    """save_sensor_data が SQL エラー時に適切に失敗するかを検証"""
    table_name = "sensor_data_table"
    data = {
        "factory": ["A"],
        "tag": ["sensor_1"],
        "date": ["2024-12-31"],
        "d0_0": [10],
    }
    df = pd.DataFrame(data)

    # SQL エラーをモック
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = RuntimeError("SQL execution failed")
    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_sql_client = MagicMock()
    mock_sql_client.connection_factory.create_connection.return_value.__enter__.return_value = mock_connection
    mocker.patch.object(production_repository.repository, "sql_client", mock_sql_client)

    # 実行
    result = production_repository.save_sensor_data(df, table_name)

    # デバッグ用出力
    print(f"Mock calls on execute: {mock_cursor.execute.mock_calls}")
    print(f"Data passed to execute: {df.values.tolist()}")

    # 検証
    mock_cursor.execute.assert_called()  # 少なくとも `execute` が呼び出されることを確認
    assert not result, "SQL エラー時に保存処理が成功してしまいました"

def test_production_repository_fetch_as_dto_with_anomalous_data(production_repository, mocker):
    """fetch_as_dto が異常値を含むデータで正しく動作するかを検証"""
    table_name = "sensor_data_table"
    tags = ["sensor_1"]
    date = "2024-12-31"

    # 異常値を含むモックデータ
    mock_sql_response = [
        ["A", "sensor_1", "2024-12-31", "local_1", "id_1", "name_1", "℃", "temperature"] +
        [100] * 29 + [99999] +  # d0_0 ~ d0_29
        [200] * 30 +            # d1_0 ~ d1_29
        [300] * 30 +            # d2_0 ~ d2_29
        [400] * 30              # d3_0 ~ d3_29
    ]
    mock_columns = ["factory", "tag", "date", "local_tag", "local_id", "name", "unit", "data_division"] + \
                   [f"d{i}_{j}" for i in range(4) for j in range(30)]  # d0_x ~ d3_x を含む
    mock_df = pd.DataFrame(mock_sql_response, columns=mock_columns)

    # SQL 応答をモック
    mocker.patch.object(production_repository.repository, "fetch_sensor_data", return_value=mock_df)

    # 実行
    dtos = production_repository.fetch_as_dto(table_name, tags, date)

    # 検証
    assert len(dtos) > 0, "DTO リストが空です"
    assert dtos[-1].value == 400, "異常値が正しく処理されていません"

def test_production_repository_delete_no_match(production_repository, mocker):
    """delete_sensor_data が削除対象なしの場合に失敗するかを検証"""
    table_name = "sensor_data_table"
    tags = ["invalid_tag"]
    date = "2024-12-31"

    # SQL 応答をモック
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 0  # 削除対象なし

    mock_connection = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    mock_sql_client = MagicMock()
    mock_sql_client.connection_factory.create_connection.return_value.__enter__.return_value = mock_connection
    mocker.patch.object(production_repository.repository, "sql_client", mock_sql_client)

    # 実行
    result = production_repository.delete_sensor_data(table_name, tags, date)

    # デバッグ出力
    print(f"Mock calls on execute: {mock_cursor.execute.mock_calls}")
    print(f"delete_query: DELETE FROM {table_name} WHERE [tag] IN (?) AND [date] = ?")
    print(f"params: {tags + [date]}")

    # 検証
    mock_sql_client.connection_factory.create_connection.assert_called_once()  # 接続が呼び出されたことを確認
    mock_connection.cursor.assert_called_once()  # cursor が呼び出されたことを確認
    mock_cursor.execute.assert_called_once_with(ANY, tags + [date])  # DELETE クエリが実行されたことを確認
    assert not result, "削除対象なしで削除処理が成功しました"
