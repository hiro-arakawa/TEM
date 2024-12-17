from common.service.schedule_service import ScheduleService
from common.service.program_service import ProgramService
from common.service.sensor_data_service import SensorDataService
from common.service.formula_data_service import FormulaDataService

from common.repository.schedule_repository import ScheduleRepository
from common.repository.sensor_data_repository import SensorDataRepository, ProductionSensorDataRepository, TestSensorDataRepository
from common.repository.formula_data_repository import FormulaDataRepository, ProductionFormulaDataRepository,TestFormulaDataRepository

from common.logger import Logger
from common.SQLServer.client import SQLClient,ConnectionFactory

from typing import Optional


# CommonFacadeパターンでCommonを呼び出すだけ、という手軽さと、各共通処理を別々に作成できる、という利便性を両立する
class CommonFacade:
    _instance = None
    logger: Optional[Logger] = None

    schedule_service: Optional[ScheduleService] = None
    program_service: Optional[ProgramService] = None
    
    sensor_data_service: Optional[SensorDataService] = None
    formula_service: Optional[FormulaDataService] = None

    _schedule_repository: Optional[ScheduleRepository] = None
    _sensor_data_repository: Optional[SensorDataRepository] = None
    _formula_data_repository: Optional[FormulaDataRepository] = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(CommonFacade, cls).__new__(cls)

            # SQLクライアントとコネクションファクトリ
            factory = ConnectionFactory(
                server="192.168.80.133",
                database="TEM",
                username="tem_prog",
                password="Ladm01#"
            )
            sql_client = SQLClient(factory)

            # ロガーのインスタンス
            cls._instance.logger = Logger()

            # センサーデータリポジトリのラップ
            production_sensor_repo = ProductionSensorDataRepository(sql_client, cls._instance.logger)
            cls._instance._sensor_data_repository = SensorDataRepository(production_sensor_repo, cls._instance.logger)

            # 計算処理リポジトリのラップ
            production_formula_repo = ProductionFormulaDataRepository(sql_client, cls._instance.logger)
            cls._instance._formula_data_repository = FormulaDataRepository(production_formula_repo, cls._instance.logger)

            # リポジトリをサービスに渡す
            cls._instance.schedule_service = ScheduleService(cls._instance._schedule_repository)
            cls._instance.program_service = ProgramService()
            cls._instance.sensor_data_service = SensorDataService(cls._instance._sensor_data_repository)
            cls._instance.formula_service = FormulaDataService(cls._instance._formula_data_repository,cls._instance.sensor_data_service, cls._instance.logger)

        return cls._instance