import time
from apscheduler.schedulers.background import BackgroundScheduler

from common import common

def view(com):
    # ここに実行したい処理を記述
    print("1秒ごとの処理を実行中...")
    com.logger.info(com.schedule_service.get_schedules())
    com.logger.info(com.program_service.get_Program())


if __name__ == "__main__":
    com = common.CommonFacade()

    scheduler = BackgroundScheduler()
    # 引数付きの関数をスケジュールに追加
    scheduler.add_job(view, 'interval', seconds=1, args=[com])
    scheduler.start()

    # プログラムが終了しないように待機
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()