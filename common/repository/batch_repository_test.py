import pytest
from unittest.mock import Mock, call
from common.repository.batch_repository import BatchRepository

@pytest.fixture
def sql_client_mock():
    return Mock()

@pytest.fixture
def logger_mock():
    return Mock()

@pytest.fixture
def batch_repository(sql_client_mock, logger_mock):
    return BatchRepository(sql_client=sql_client_mock, logger=logger_mock)

def test_execute_stored_procedure_without_result_success(batch_repository, sql_client_mock, logger_mock):
    procedure_name = "test_procedure"
    params = ["param1", "param2"]

    batch_repository.execute_stored_procedure_without_result(procedure_name, params)

    sql_client_mock._execute_procedure.assert_called_once_with(procedure_name, params)
    logger_mock.info.assert_has_calls([
        call(f"Executing stored procedure without result: {procedure_name}"),
        call("ストアドプロシージャの実行が正常に完了しました。")
    ])

def test_execute_stored_procedure_without_result_failure(batch_repository, sql_client_mock, logger_mock):
    procedure_name = "test_procedure"
    params = ["param1", "param2"]
    sql_client_mock._execute_procedure.side_effect = Exception("Execution failed")

    with pytest.raises(Exception, match="Execution failed"):
        batch_repository.execute_stored_procedure_without_result(procedure_name, params)

    sql_client_mock._execute_procedure.assert_called_once_with(procedure_name, params)
    logger_mock.info.assert_called_once_with(f"Executing stored procedure without result: {procedure_name}")
    logger_mock.error.assert_called_once_with("ストアドプロシージャの実行に失敗しました: Execution failed")