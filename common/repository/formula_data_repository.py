from abc import ABC, abstractmethod
import pandas as pd
from common.SQLServer.client import SQLClient

class AbstractFormulaDataRepository(ABC):
    """
    演算式データを管理および取得する抽象クラス。
    """
    @abstractmethod
    def get_calculation_formula(self, formula_id: str) -> dict:
        pass

    @abstractmethod
    def fetch_all_formulas(self) -> dict:
        pass


class ProductionFormulaDataRepository(AbstractFormulaDataRepository):
    """
    本番環境用の演算式データリポジトリクラス。
    データベースから演算式を管理・取得。
    """
    def __init__(self, sql_client: SQLClient, logger):
        """
        初期化時にデータベース接続クライアントを受け取ります。

        :param sql_client: SQLClient, SQL Serverとの接続を管理するクライアント
        """
        self.sql_client = sql_client
        self.logger=logger

    def get_calculation_formula(self, formula_id: str) -> dict:
        """
        演算式IDを基に演算式と仮想センサー名をデータベースから取得します。
        """
        if not isinstance(formula_id, str):
            raise TypeError("Invalid formula_id type, must be a string")
        
        # クエリを1行で記述してスペースを最小限に
        query = "SELECT calculate_formula, calculate_name FROM TEM.batch.data_processing_calculation_formulas WHERE calculate_pid = ?"
        params = [formula_id]
        result = self.sql_client.execute_query(query, params)

        if not result:
            raise ValueError(f"Formula ID {formula_id} not found.")
        row = result[0]
        return {"formula": row[0], "sensor_name": row[1]}
    

    def fetch_all_formulas(self) -> dict:
        """
        全ての演算式データをデータベースから取得します。

        :return: dict, {formula_id: {"formula": str, "sensor_name": str}}
        """
        query = """
        SELECT calculate_pid, calculate_formula, calculate_name
        FROM TEM.batch.data_processing_calculation_formulas
        """
        results = self.sql_client.execute_query(query)

        return {row[0]: {"formula": row[1], "sensor_name": row[2]} for row in results}

class TestFormulaDataRepository(AbstractFormulaDataRepository):
    """
    テスト環境用の演算式データリポジトリクラス。
    ダミーデータを返します。
    """
    def __init__(self, formula_data: dict, logger):
        """
        初期化時にダミーデータを受け取り、内部で保持します。

        :param formula_data: dict, 演算式IDとその式およびセンサー名のマッピング
        """
        self.formula_data = formula_data or {
            "H2": {"formula": "(HD13001 + HD13002) * HD13003 / HD13004", "sensor_name": "VirtualSensor_H2"},
            "H3": {"formula": "HD13001 - HD13002 + HD13003", "sensor_name": "VirtualSensor_H3"},
            "H4": {"formula": "(HD13001 * HD13002) / (HD13003 - HD13004)", "sensor_name": "VirtualSensor_H4"},
            "H5": {"formula": "HD13001 + HD13002 - HD13003 * HD13004", "sensor_name": "VirtualSensor_H5"},
            "H6": {"formula": "(HD13001 + HD13003) / (HD13002 + HD13004)", "sensor_name": "VirtualSensor_H6"}
        }
        self.formula_data = formula_data if formula_data is not None else {}

        self.logger=logger

    def get_calculation_formula(self, formula_id: str) -> dict:
        """
        演算式IDを基にダミーデータから演算式を取得します。

        :param formula_id: str, 演算式ID
        :return: dict, {"formula": str, "sensor_name": str}
        :raises ValueError: 指定された演算式IDが存在しない場合
        """
        if formula_id not in self.formula_data:
            raise ValueError(f"Formula ID {formula_id} not found.")
        return self.formula_data[formula_id]
    
    def fetch_all_formulas(self) -> dict:
        """
        全てのダミーデータの演算式を取得します。

        :return: dict, {formula_id: {"formula": str, "sensor_name": str}}
        """
        return self.formula_data
    
# Repository定義
class FormulaDataRepository:
    """
    環境に応じたリポジトリインスタンスをラップするクラス。
    """
    def __init__(self, repository: AbstractFormulaDataRepository, logger):
        """
        初期化時に適切なリポジトリインスタンスを受け取ります。

        :param repository: AbstractFormulaDataRepository, 本番またはテストリポジトリ
        """
        self.repository = repository
        self.logger = logger


    def get_calculation_formula(self, formula_id: str) -> dict:
        self.logger.info(f"Fetching calculation formula for formula_id: {formula_id}")
        try:
            return self.repository.get_calculation_formula(formula_id)
        except Exception as e:
            self.logger.error(f"Error fetching formula {formula_id}: {e}")
            raise

    def fetch_all_formulas(self) -> dict:
        self.logger.info("Fetching all calculation formulas.")
        try:
            return self.repository.fetch_all_formulas()
        except Exception as e:
            self.logger.error(f"Error fetching all formulas: {e}")
            raise

