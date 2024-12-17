import pytest
import pandas as pd
from unittest.mock import Mock
from typing import List
from formula_processor import FormulaProcessor

@pytest.fixture
def formula_service_mock():
    """Mock for formula service."""
    mock = Mock()
    mock.get_formula_by_id.side_effect = lambda formula_id: {
        "H1": {"formula": "(tag1 + tag2) * tag3 / tag4", "sensor_name": "VirtualSensor_H1"},
        "H2": {"formula": "(tag1 + ) * tag2", "sensor_name": "VirtualSensor_H2"},  # Invalid syntax
        "H3": {"formula": "(tag1 + tagX) * tag2", "sensor_name": "VirtualSensor_H3"},  # Missing tag
        "H4": {"formula": "(tag1 / tag2)", "sensor_name": "VirtualSensor_H4"},  # Division by zero
    }.get(formula_id, None)
    return mock

@pytest.fixture
def sample_data():
    """Sample sensor data."""
    return pd.DataFrame({
        "factory": ["A", "A"],
        "tag": ["tag1", "tag2"],
        "date": ["2024-12-02", "2024-12-02"],
        "unit": ["℃", "℃"],
        "data_division": ["temperature", "temperature"],
        **{f"d0_{i}": [1, 1] for i in range(30)},  # Assurance codes
        **{f"d1_{i}": [10, 0] for i in range(30)},  # Values for division by zero
        **{f"d2_{i}": [20, 5] for i in range(30)},
        **{f"d3_{i}": [30, 15] for i in range(30)},
    })

def test_invalid_syntax(formula_service_mock, sample_data):
    processor = FormulaProcessor(formula_service_mock)
    formula_id = "H2"
    formula_data = formula_service_mock.get_formula_by_id(formula_id)

    with pytest.raises(ValueError, match="Invalid formula syntax"):
        processor.extract_tags_from_formula(formula_data["formula"])

def test_missing_tag(formula_service_mock, sample_data):
    processor = FormulaProcessor(formula_service_mock)
    formula_id = "H3"
    formula_data = formula_service_mock.get_formula_by_id(formula_id)
    tags = processor.extract_tags_from_formula(formula_data["formula"])


    with pytest.raises(ValueError, match="Data missing for variables"):
        processor.validate_tags_in_data(tags, sample_data)

def test_division_by_zero(formula_service_mock, sample_data):
    processor = FormulaProcessor(formula_service_mock)
    formula_id = "H4"
    formula_data = formula_service_mock.get_formula_by_id(formula_id)
    tags = processor.extract_tags_from_formula(formula_data["formula"])

    processor.validate_tags_in_data(tags, sample_data)

    result = processor.calculate_result(sample_data, formula_data["formula"], tags, formula_id, formula_data["sensor_name"])

    # d1, d2, d3 の結果がすべて None であることを確認
    for column in ["d1", "d2", "d3"]:
        assert all(result[f"{column}_{i}"].isna().all() for i in range(30))


@pytest.fixture
def boundary_test_data():
    """Boundary test data for sensors."""
    return pd.DataFrame({
        "factory": ["A", "A", "A", "A"],
        "tag": ["tag1", "tag2", "tag3", "tag4"],
        "date": ["2024-12-02"] * 4,
        "unit": ["℃", "℃", "℃", "℃"],
        "data_division": ["temperature"] * 4,
        # Assurance codes (always valid)
        **{f"d0_{i}": [1, 1, 1, 1] for i in range(30)},
        # Boundary test values
        **{f"d1_{i}": [0, 100, 50, 10] for i in range(30)},  # tag1=0, tag2=100, tag3=50, tag4=10
        **{f"d2_{i}": [None, 50, 25, 5] for i in range(30)},  # tag1=None
        **{f"d3_{i}": [999999, -100, 1, 0.1] for i in range(30)},  # Extreme values
    })

def test_boundary_values(formula_service_mock, boundary_test_data):
    processor = FormulaProcessor(formula_service_mock)
    formula_id = "H1"
    formula_data = formula_service_mock.get_formula_by_id(formula_id)
    tags = processor.extract_tags_from_formula(formula_data["formula"])

def validate_tags_in_data(self, tags: List[str], sql_data: pd.DataFrame):
    self.com.logger.info(f"sql_data type: {type(sql_data)}")
    if not isinstance(sql_data, pd.DataFrame):
        raise TypeError("sql_data must be a pandas DataFrame")

    self.com.logger.info(f"First row of sql_data:\n{sql_data.head()}")
    available_tags = sql_data["tag"].unique()
    missing_tags = [tag for tag in tags if tag not in available_tags]
    if missing_tags:
        self.com.logger.error(f"Missing data for variables: {missing_tags}")
        raise ValueError(f"Data missing for variables: {missing_tags}")

    # 欠損値チェックを追加
    if sql_data.isnull().values.any():
        self.com.logger.error("Data contains missing values.")
        raise ValueError("Data contains missing values.")

    self.com.logger.info("All required variables are available.")

def test_missing_data(formula_service_mock, boundary_test_data):
    processor = FormulaProcessor(formula_service_mock)
    formula_id = "H1"
    formula_data = formula_service_mock.get_formula_by_id(formula_id)
    tags = processor.extract_tags_from_formula(formula_data["formula"])

    # データの一部を欠損値に変更
    boundary_test_data.loc[0, "d1_0"] = None

    # 欠損値が含まれている場合に ValueError が発生することを確認
    with pytest.raises(ValueError, match="Data contains missing values."):
        processor.calculate_result(boundary_test_data, formula_data["formula"], tags, formula_id, formula_data["sensor_name"])

def test_rounding_to_two_decimal_places(formula_service_mock, sample_data):
    # モックデータを設定
    formula_id = "H5"
    formula_data = {
        "formula": "tag1 * 0.1234 + tag2 / 2.0 - 0.5678",
        "sensor_name": "TestSensor_H5"
    }
    formula_service_mock.get_formula_by_id.return_value = formula_data

    processor = FormulaProcessor(formula_service_mock)
    tags = processor.extract_tags_from_formula(formula_data["formula"])

    # タグの存在確認
    processor.validate_tags_in_data(tags, sample_data)

    # 計算結果を取得
    result = processor.calculate_result(sample_data, formula_data["formula"], tags, formula_id, formula_data["sensor_name"])

    # 小数点以下2位で丸められているか確認
    for column in ["d1", "d2", "d3"]:
        for i in range(30):
            rounded_values = result[f"{column}_{i}"].dropna()  # Noneを除外
            for value in rounded_values:
                assert round(value, 2) == value  # 小数点2位と一致するか確認
