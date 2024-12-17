import pandas as pd
from common.settings import get_table_name

class FormulaDataService:
    """
    ビジネスロジックを担当するサービス層クラス。
    """
    def __init__(self, formula_repository, sensor_data_service, logger):
        self.formula_repository = formula_repository
        self.sensor_data_service = sensor_data_service
        self.logger = logger

    def get_formula_by_id(self, formula_id: str) -> str:
        """
        IDに基づいて演算式を取得する。
        """
        try:
            formula = self.formula_repository.get_calculation_formula(formula_id)
            # 必要に応じて追加のビジネスロジックをここに記述
            return formula
        except ValueError:
            # エラーハンドリングやログ記録などをサービス層で実行
            raise

    def list_all_formulas(self) -> dict:
        """
        全ての演算式をリストする。
        """
        return self.formula_repository.fetch_all_formulas()

    def validate_formula(self, formula: str) -> bool:
        """
        演算式が正しい構文かどうかを検証する。

        :param formula: str, 演算式
        :return: bool, 構文が正しい場合は True
        """
        import ast
        if not formula:  # None または空文字のチェック
            return False
        try:
            ast.parse(formula, mode='eval')  # 式を構文解析
            return True
        except SyntaxError:
            return False

    # FormulaDataService の save_calculation_results メソッド修正案
    def save_calculation_results(self, df: pd.DataFrame) -> bool:
        calc_table = get_table_name("calculation_result_table")
        if df.empty:
            self.logger.warning(f"No data to save for table: {calc_table}. DataFrame is empty.")
            return False

        try:
            # SensorDataService を使用した保存処理
            result = self.sensor_data_service.save_calculation_result(df)
            if result:
                self.logger.info(f"Calculation results saved successfully to {calc_table}.")
            else:
                self.logger.warning(f"Failed to save calculation results to {calc_table}.")
            return result
        except Exception as e:
            self.logger.error(f"Failed to save calculation results: {e}")
            raise
