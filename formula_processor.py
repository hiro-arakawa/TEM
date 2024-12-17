import pandas as pd
from typing import List

from sympy import sympify, symbols, lambdify
import numpy as np
from common import common

class FormulaProcessor:
    """
    SQL出力形式のデータを直接処理する FormulaProcessor クラス。
    """
    def __init__(self, com: common):
        self.com = com

    def extract_tags_from_formula(self, formula: str) -> List[str]:
        """
        演算式を解析して、必要な変数（タグ）のリストを抽出します。
        """
        self.com.logger.info(f"Parsing formula: {formula}")
        import ast

        class FormulaVisitor(ast.NodeVisitor):
            def __init__(self):
                self.variables = set()

            def visit_Name(self, node):
                self.variables.add(node.id)

        try:
            tree = ast.parse(formula, mode="eval")
            visitor = FormulaVisitor()
            visitor.visit(tree)
            variables = list(visitor.variables)
            self.com.logger.info(f"Extracted variables: {variables}")
            return variables
        except SyntaxError as e:
            self.com.logger.error(f"Invalid formula syntax: {e}")
            raise ValueError("Invalid formula syntax")

    def validate_tags_in_data(self, tags: List[str], sql_data: pd.DataFrame):
        """
        SQL出力形式のデータ内に必要な変数（タグ）がすべて存在するか確認。
        """
        # デバッグログで `sql_data` の型と内容を確認
        self.com.logger.info(f"sql_data type: {type(sql_data)}")
        if not isinstance(sql_data, pd.DataFrame):
            raise TypeError("sql_data must be a pandas DataFrame")

        # タグの存在確認
        available_tags = sql_data["tag"].unique()
        missing_tags = [tag for tag in tags if tag not in available_tags]
        if missing_tags:
            self.com.logger.error(f"Missing data for variables: {missing_tags}")
            raise ValueError(f"Data missing for variables: {missing_tags}")
        self.com.logger.info("All required variables are available.")


    def calculate_result(self, sql_data: pd.DataFrame, formula: str, tags: List[str], formula_id: str, sensor_name: str) -> pd.DataFrame:
        self.com.logger.info("Calculating result with data assurance codes.")
        try:
            if sql_data.isnull().values.any():
                self.com.logger.error("Data contains missing values.")
                raise ValueError("Data contains missing values.")
            
            # 対象タグのデータフィルタリング
            relevant_data = sql_data[sql_data["tag"].isin(tags)]
            
            # SymPy 式を NumPy 関数に変換
            formula_expr = sympify(formula)
            tag_symbols = [symbols(tag) for tag in tags]  # 順序を明示
            formula_func = lambdify(tag_symbols, formula_expr, modules="numpy")

            # コンテキストデータを NumPy 配列として事前に構築
            context = {
                tag: {
                    f"d{i}": relevant_data.loc[relevant_data["tag"] == tag, [f"d{i}_{j}" for j in range(30)]].to_numpy().flatten()
                    for i in range(4)  # d0, d1, d2, d3
                }
                for tag in tags
            }

            # 計算結果のリスト
            results = []

            # グループ単位で計算を実行
            for (factory, date), group in relevant_data.groupby(["factory", "date"]):
                try:
                    # 各時間ステップごとにデータを取り出す
                    d1_data = np.vstack([context[tag]["d1"] for tag in tags]).T  # 順序を保持
                    d2_data = np.vstack([context[tag]["d2"] for tag in tags]).T
                    d3_data = np.vstack([context[tag]["d3"] for tag in tags]).T
                    d0_data = np.vstack([context[tag]["d0"] for tag in tags]).T

                    # NumPy 配列によるベクトル計算
                    d1_results, d2_results, d3_results, assurances = [], [], [], []
                    for i in range(30):
                        try:
                            with np.errstate(divide='raise', invalid='raise'):
                                d1_result = round(formula_func(*d1_data[i]), 2)
                                d2_result = round(formula_func(*d2_data[i]), 2)
                                d3_result = round(formula_func(*d3_data[i]), 2)
                                assurance = int(np.all(d0_data[i] == 1))
                                d1_results.append(d1_result)
                                d2_results.append(d2_result)
                                d3_results.append(d3_result)
                                assurances.append(assurance)
                        except (FloatingPointError, ZeroDivisionError, ValueError):
                            d1_results.append(None)
                            d2_results.append(None)
                            d3_results.append(None)
                            assurances.append(2)
                        except Exception as e:
                            self.com.logger.error(f"Unexpected error in calculation at index {i}: {e}")
                            d1_results.append(None)
                            d2_results.append(None)
                            d3_results.append(None)
                            assurances.append(2)

                    # 結果をリストに保存
                    results.append(
                        [factory, date, formula_id, formula_id, formula_id, sensor_name, group.iloc[0]["unit"], group.iloc[0]["data_division"]] +
                        d1_results + d2_results + d3_results + assurances
                    )
                except Exception as e:
                    self.com.logger.error(f"Error in calculation for group {factory}, {date}: {e}")
                    continue

            # 結果を DataFrame に変換
            columns = ["factory", "date", "tag", "local_tag", "local_id", "name", "unit", "data_division"] + \
                    [f"d1_{i}" for i in range(30)] + [f"d2_{i}" for i in range(30)] + \
                    [f"d3_{i}" for i in range(30)] + [f"d0_{i}" for i in range(30)]
            result_df = pd.DataFrame(results, columns=columns)

            self.com.logger.info("Calculation with data assurance codes completed successfully.")
            return result_df

        except Exception as e:
            self.com.logger.error(f"Error in calculation with data assurance codes: {e}")
            raise

    def process_formula(self, target_date: str, formula_id: str):
        """
        ターゲット日付と計算式IDを指定して計算を実行し、結果をデータベースに保存する。
        """
        try:
            formula_data = self.com.formula_service.get_formula_by_id(formula_id)
            self.com.logger.info(f"Formula data retrieved for ID {formula_id}: {formula_data}")

            if not formula_data or not isinstance(formula_data, dict):
                self.com.logger.error(f"Formula ID {formula_id} not found or invalid format.")
                raise ValueError(f"Formula ID {formula_id} not found or invalid format.")

            formula = formula_data.get("formula")
            sensor_name = formula_data.get("sensor_name")

            if not formula:
                self.com.logger.error(f"Formula is missing in the data for ID {formula_id}.")
                raise ValueError(f"Formula is missing in the data for ID {formula_id}.")

            if not sensor_name:
                self.com.logger.error(f"Sensor name is missing in the data for ID {formula_id}.")
                raise ValueError(f"Sensor name is missing in the data for ID {formula_id}.")

            tags = self.extract_tags_from_formula(formula)
            self.com.logger.info(f"Extracted tags for formula ID {formula_id}: {tags}")

            sql_data = self.com.sensor_data_service.get_sensor_data(tags, target_date)
            self.com.logger.info("Sensor data retrieved successfully.")

            self.validate_tags_in_data(tags, sql_data)
            self.com.logger.info("Tag validation successful.")

            result_data = self.calculate_result(sql_data, formula, tags, formula_id, sensor_name)
            self.com.logger.info("Calculation completed successfully.")

            if result_data.empty:
                self.com.logger.warning(f"No data processed for formula ID {formula_id} on {target_date}.")
                return

            success = self.com.sensor_data_service.save_calculation_result(result_data)
            if success:
                self.com.logger.info(f"Calculation completed and saved for formula ID {formula_id} on {target_date}.")
            else:
                self.com.logger.error(f"Failed to save calculation results for formula ID {formula_id} on {target_date}.")
                raise RuntimeError(f"Failed to save calculation results for formula ID {formula_id} on {target_date}.")

        except Exception as e:
            self.com.logger.error(f"Error in process_formula: {e}")

if __name__ == "__main__":
    # 実行例
    com = common.CommonFacade()
    processor = FormulaProcessor(com)
    processor.process_formula("2024-12-02", "H3")
