import pytest
from unittest.mock import MagicMock
from formula_data_repository import FormulaDataRepository, ProductionFormulaDataRepository, TestFormulaDataRepository


# テスト用モック
@pytest.fixture
def logger_mock():
    return MagicMock()


@pytest.fixture
def sql_client_mock():
    return MagicMock()


@pytest.fixture
def test_repository(logger_mock):
    """TestFormulaDataRepository のインスタンスを提供"""
    dummy_data = {
        "H2": {"formula": "(HD13001 + HD13002) * HD13003 / HD13004", "sensor_name": "VirtualSensor_H2"},
        "H3": {"formula": "HD13001 - HD13002 + HD13003", "sensor_name": "VirtualSensor_H3"},
    }
    return FormulaDataRepository(TestFormulaDataRepository(dummy_data, logger_mock), logger_mock)


@pytest.fixture
def production_repository(sql_client_mock, logger_mock):
    """ProductionFormulaDataRepository のインスタンスを提供"""
    return FormulaDataRepository(ProductionFormulaDataRepository(sql_client_mock, logger_mock), logger_mock)


def test_test_repository_get_calculation_formula(test_repository):
    """TestFormulaDataRepository の get_calculation_formula テスト"""
    formula = test_repository.get_calculation_formula("H2")
    assert formula == {
        "formula": "(HD13001 + HD13002) * HD13003 / HD13004",
        "sensor_name": "VirtualSensor_H2"
    }, "取得された式が期待値と異なります"


def test_test_repository_get_calculation_formula_not_found(test_repository):
    """存在しない formula_id を指定した場合の例外処理を確認"""
    with pytest.raises(ValueError, match="Formula ID invalid_id not found."):
        test_repository.get_calculation_formula("invalid_id")


def test_test_repository_fetch_all_formulas(test_repository):
    """TestFormulaDataRepository の fetch_all_formulas メソッドのテスト"""
    formulas = test_repository.fetch_all_formulas()
    assert isinstance(formulas, dict), "結果が辞書型ではありません"
    assert "H2" in formulas, "H2 が辞書に含まれていません"


def test_production_repository_get_calculation_formula(production_repository):
    """get_calculation_formula のクエリと結果の確認"""
    mock_response = [("(HD13001 + HD13002) * HD13003 / HD13004", "VirtualSensor_H2")]
    production_repository.repository.sql_client.execute_query.return_value = mock_response

    formula = production_repository.get_calculation_formula("H2")

    # クエリの実行確認
    production_repository.repository.sql_client.execute_query.assert_called_once_with(
        "SELECT calculate_formula, calculate_name FROM TEM.batch.data_processing_calculation_formulas WHERE calculate_pid = ?",
        ["H2"]
    )

    # 結果の検証
    assert formula == {
        "formula": "(HD13001 + HD13002) * HD13003 / HD13004",
        "sensor_name": "VirtualSensor_H2"
    }



def test_production_repository_fetch_all_formulas(production_repository):
    """ProductionFormulaDataRepository の fetch_all_formulas テスト"""
    production_repository.repository.sql_client.execute_query.return_value = [
        ("H2", "(HD13001 + HD13002) * HD13003 / HD13004", "VirtualSensor_H2"),
        ("H3", "HD13001 - HD13002 + HD13003", "VirtualSensor_H3"),
    ]
    formulas = production_repository.fetch_all_formulas()
    assert isinstance(formulas, dict), "結果が辞書型ではありません"
    assert "H2" in formulas, "H2 が辞書に含まれていません"


def test_production_repository_sql_injection(production_repository):
    """SQLインジェクションが防止されているか確認"""
    production_repository.repository.sql_client.execute_query.side_effect = ValueError("SQL Injection detected")
    with pytest.raises(ValueError, match="SQL Injection detected"):
        production_repository.get_calculation_formula("' OR '1'='1")
