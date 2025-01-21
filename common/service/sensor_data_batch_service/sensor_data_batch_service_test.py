import pytest
from unittest.mock import Mock, patch
from common.service.sensor_data_batch_service.sensor_data_batch_service import SensorDataBatchService

@pytest.fixture
def mock_repository():
    return Mock()

@pytest.fixture
def mock_logger():
    return Mock()

@pytest.fixture
def sensor_data_batch_service(mock_repository, mock_logger):
    return SensorDataBatchService(mock_repository, mock_logger)

def test_process_sensor_data_batch_success(sensor_data_batch_service, mock_repository, mock_logger):
    bcp_command = "BCP command"
    factory_code = "A"
    process_date = "2024-12-02"

    result = sensor_data_batch_service.process_sensor_data_batch(bcp_command, factory_code, process_date)

    mock_repository.load_data_to_temp_table.assert_called_once_with(bcp_command)
    mock_repository.merge_temp_to_main.assert_called_once_with(process_date, factory_code)
    mock_logger.info.assert_any_call("センサーデータのバッチ処理を開始します。")
    mock_logger.info.assert_any_call("センサーデータのバッチ処理が正常に完了しました。")
    assert result == {"status": "success", "message": "センサーデータのバッチ処理が完了しました。"}

def test_process_sensor_data_batch_failure(sensor_data_batch_service, mock_repository, mock_logger):
    bcp_command = "BCP command"
    factory_code = "A"
    process_date = "2024-12-02"
    mock_repository.load_data_to_temp_table.side_effect = Exception("Test error")

    result = sensor_data_batch_service.process_sensor_data_batch(bcp_command, factory_code, process_date)

    mock_repository.load_data_to_temp_table.assert_called_once_with(bcp_command)
    mock_repository.merge_temp_to_main.assert_not_called()
    mock_logger.error.assert_called_once_with("バッチ処理中にエラーが発生しました: Test error")
    assert result == {"status": "error", "message": "Test error"}