import pytest
import pandas as pd
from unittest.mock import MagicMock
from formula_data_service import FormulaDataService


@pytest.fixture
def mock_formula_repository():
    """FormulaRepository のモックを作成する"""
    repository = MagicMock()
    repository.formula_data = {}
    repository.get_calculation_formula.side_effect = lambda formula_id: repository.formula_data.get(formula_id)
    repository.fetch_all_formulas.side_effect = lambda: repository.formula_data
    return repository

@pytest.fixture
def mock_sensor_data_service():
    """SensorDataService のモックを作成する"""
    return MagicMock()

@pytest.fixture
def logger_mock():
    """ロガーのモックを提供"""
    return MagicMock()


@pytest.fixture
def formula_service(mock_formula_repository, mock_sensor_data_service, logger_mock):
    """FormulaDataService のインスタンスを作成する"""
    return FormulaDataService(
        formula_repository=mock_formula_repository,
        sensor_data_service=mock_sensor_data_service,
        logger=logger_mock
    )


def test_get_formula_by_id(formula_service, mock_formula_repository):
    # テストデータを設定
    mock_formula_repository.formula_data = {
        "F1": "a + b",
        "F2": "x * y"
    }
    
    # メソッド呼び出し
    formula = formula_service.get_formula_by_id("F1")
    
    # 検証
    mock_formula_repository.get_calculation_formula.assert_called_once_with("F1")
    assert formula == "a + b"


def test_list_all_formulas(formula_service, mock_formula_repository):
    # テストデータを設定
    mock_formula_repository.formula_data = {
        "F1": "a + b",
        "F2": "x * y"
    }
    
    # メソッド呼び出し
    formulas = formula_service.list_all_formulas()
    
    # 検証
    mock_formula_repository.fetch_all_formulas.assert_called_once()
    assert formulas == {
        "F1": "a + b",
        "F2": "x * y"
    }


def test_validate_formula(formula_service):
    # 有効な式を検証
    assert formula_service.validate_formula("a + b") is True
    assert formula_service.validate_formula("x * y + z") is True

    # 無効な式を検証
    assert formula_service.validate_formula("a + ") is False
    assert formula_service.validate_formula("x * (y + z") is False

def test_get_formula_by_id_not_found(formula_service, mock_formula_repository):
    """存在しない formula_id を指定した場合の動作確認"""
    mock_formula_repository.get_calculation_formula.side_effect = ValueError("Formula not found")
    
    with pytest.raises(ValueError, match="Formula not found"):
        formula_service.get_formula_by_id("F3")  # 存在しない ID

    mock_formula_repository.get_calculation_formula.assert_called_once_with("F3")


def test_list_all_formulas_empty(formula_service, mock_formula_repository):
    """全ての演算式をリストする際、データが空の場合の動作確認"""
    mock_formula_repository.formula_data = {}  # データが空

    formulas = formula_service.list_all_formulas()

    mock_formula_repository.fetch_all_formulas.assert_called_once()
    assert formulas == {}  # 空の辞書を期待


def test_validate_formula_empty_or_none(formula_service):
    """空文字や None を検証する際の動作確認"""
    assert formula_service.validate_formula("") is False
    assert formula_service.validate_formula(None) is False


def test_validate_formula_complex(formula_service):
    """複雑な演算式の検証"""
    complex_formula = "(a + b) * (x / y) - (c ** 2) + func(d)"
    assert formula_service.validate_formula(complex_formula) is True

    invalid_complex_formula = "(a + b) * (x / y - (c ** 2) + func(d)"
    assert formula_service.validate_formula(invalid_complex_formula) is False

def test_save_calculation_results_success(formula_service, mock_sensor_data_service):
    """計算結果が正常に保存される場合のテスト"""
    # モックデータを設定
    test_df = pd.DataFrame({
        "factory": ["A", "B"],
        "tag": ["T1", "T2"],
        "date": ["2024-12-12", "2024-12-13"],
        "value1": [100.0, 200.0],
        "value2": [300.0, 400.0],
    })

    mock_sensor_data_service.save_calculation_result.return_value = True

    # メソッド呼び出し
    result = formula_service.save_calculation_results(test_df)

    # 検証
    mock_sensor_data_service.save_calculation_result.assert_called_once_with(test_df)
    assert result is True

def test_save_calculation_results_exception(formula_service, mock_formula_repository):
    """保存時に例外が発生した場合の動作確認"""
    # モックデータを設定
    test_df = pd.DataFrame({
        "factory": ["A"],
        "tag": ["T1"],
        "date": ["2024-12-12"],
        "value1": [100.0],
        "value2": [200.0],
    })

    destination = "test_table"
    mock_formula_repository.save_calculation_data.side_effect = Exception("Database error")

    # メソッド呼び出し
    with pytest.raises(Exception, match="Database error"):
        formula_service.save_calculation_results(test_df, destination)

    # 検証
    mock_formula_repository.save_calculation_data.assert_called_once_with(test_df, destination)

def test_save_calculation_results_empty_dataframe(formula_service):
    """空のデータフレームが渡された場合の動作確認"""
    test_df = pd.DataFrame()  # 空のデータフレーム

    # メソッド呼び出し
    result = formula_service.save_calculation_results(test_df)

    # 検証
    assert result is False

def test_save_calculation_results_failure(formula_service, mock_sensor_data_service):
    """計算結果の保存に失敗する場合のテスト"""
    test_df = pd.DataFrame({
        "factory": ["A"],
        "tag": ["T1"],
        "date": ["2024-12-12"],
        "value1": [100.0],
        "value2": [200.0],
    })

    mock_sensor_data_service.save_calculation_result.return_value = False  # 保存失敗を模擬

    # メソッド呼び出し
    result = formula_service.save_calculation_results(test_df)

    # 検証
    mock_sensor_data_service.save_calculation_result.assert_called_once_with(test_df)
    assert result is False


def test_save_calculation_results_exception(formula_service, mock_sensor_data_service):
    """保存時に例外が発生した場合の動作確認"""
    test_df = pd.DataFrame({
        "factory": ["A"],
        "tag": ["T1"],
        "date": ["2024-12-12"],
        "value1": [100.0],
        "value2": [200.0],
    })

    mock_sensor_data_service.save_calculation_result.side_effect = Exception("Database error")

    with pytest.raises(Exception, match="Database error"):
        formula_service.save_calculation_results(test_df)

    mock_sensor_data_service.save_calculation_result.assert_called_once_with(test_df)

def test_logging_on_save_exception(formula_service, mock_sensor_data_service, logger_mock):
    """保存時の例外発生時にエラーログが記録されるかを確認"""
    test_df = pd.DataFrame({
        "factory": ["A"],
        "tag": ["T1"],
        "date": ["2024-12-12"],
        "value1": [100.0],
        "value2": [200.0],
    })

    mock_sensor_data_service.save_calculation_result.side_effect = Exception("Database error")

    with pytest.raises(Exception, match="Database error"):
        formula_service.save_calculation_results(test_df)

    # ログ記録の検証
    logger_mock.error.assert_called_once_with("Failed to save calculation results: Database error")


def test_save_results_via_sensor_service(formula_service, mock_sensor_data_service):
    """センサーサービスを通じてデータが保存されるか確認"""
    test_df = pd.DataFrame({
        "factory": ["A"],
        "tag": ["T1"],
        "date": ["2024-12-12"],
        "value1": [100.0],
        "value2": [200.0],
    })

    mock_sensor_data_service.save_calculation_result.return_value = True

    # メソッド呼び出し
    result = formula_service.save_calculation_results(test_df)

    # 検証
    mock_sensor_data_service.save_calculation_result.assert_called_once_with(test_df)
    assert result is True

def test_save_large_dataset(formula_service, mock_sensor_data_service):
    """大規模データセットの保存テスト"""
    large_df = pd.DataFrame({
        "factory": ["A"] * 10000,
        "tag": [f"T{i}" for i in range(10000)],
        "date": ["2024-12-12"] * 10000,
        "value1": [100.0] * 10000,
        "value2": [200.0] * 10000,
    })

    mock_sensor_data_service.save_calculation_result.return_value = True

    # メソッド呼び出し
    result = formula_service.save_calculation_results(large_df)

    # 検証
    mock_sensor_data_service.save_calculation_result.assert_called_once_with(large_df)
    assert result is True