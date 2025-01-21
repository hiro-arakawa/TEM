from common.service.sensor_data_service.sensor_data_service import SensorDataService
from common.service.formula_data_service.formula_data_service import FormulaDataService

from common.service.sensor_data_batch_service.sensor_data_batch_service import SensorDataBatchService


from common.repository.schedule_repository import ScheduleRepository
from common.repository.sensor_data_repository import SensorDataRepository, ProductionSensorDataRepository, TestSensorDataRepository
from common.repository.formula_data_repository import FormulaDataRepository, ProductionFormulaDataRepository,TestFormulaDataRepository

from common.repository.batch_repository import BatchRepository


from common.logger import Logger
from common.SQLServer.client import SQLClient,ConnectionFactory

from typing import Optional


# CommonFacadeパターンでCommonを呼び出すだけ、という手軽さと、各共通処理を別々に作成できる、という利便性を両立する
class CommonFacade:
    _instance = None
    logger: Optional[Logger] = None

    sensor_data_service: Optional[SensorDataService] = None
    formula_data_service: Optional[FormulaDataService] = None

    sensor_data_batch_service: Optional[SensorDataBatchService] = None


    _schedule_repository: Optional[ScheduleRepository] = None
    _sensor_data_repository: Optional[SensorDataRepository] = None
    _formula_data_repository: Optional[FormulaDataRepository] = None

    _batch_repository: Optional[BatchRepository] = None

    def __new__(cls):
        """
        CommonFacadeクラスのシングルトンインスタンスを作成して返します。
        このメソッドは、CommonFacadeクラスのインスタンスが一つだけ作成されることを保証します。
        既にインスタンスが存在する場合は、既存のインスタンスを返します。そうでない場合は、
        新しいインスタンスを作成し、SQLクライアント、ロガー、センサーデータリポジトリ、
        計算処理リポジトリ、およびそれらのサービスを初期化します。
            CommonFacade: CommonFacadeクラスのシングルトンインスタンス。
        """
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
            cls._instance.sensor_data_service = SensorDataService(cls._instance._sensor_data_repository)
            cls._instance.formula_data_service = FormulaDataService(cls._instance._formula_data_repository,cls._instance.sensor_data_service, cls._instance.logger)

            cls._instance._batch_repository = BatchRepository(sql_client, cls._instance.logger)   
            cls._instance.sensor_data_batch_service = SensorDataBatchService(cls._instance._batch_repository, cls._instance.logger)


        return cls._instance