from common.common import CommonFacade

if __name__ == "__main__":
    # 実行例
    com = CommonFacade()

    result = com.sensor_data_batch_service.process_sensor_data_batch("bcp command", "H","2024-12-20")
    print(result)