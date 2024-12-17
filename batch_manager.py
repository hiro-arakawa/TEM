from common.service.batch_service.batch_service import BatchService
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import time

# ログ設定
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# BatchService の初期化
service = BatchService()

# メイン処理
if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.start()

    try:
        logging.info("Batch Manager started. Checking and scheduling tasks...")

        # 循環依存の検出
        try:
            service.detect_circular_dependency()
            logging.info("No circular dependencies detected.")
        except Exception as e:
            logging.error(f"Dependency error: {e}")
            raise SystemExit("Circular dependencies found. Stopping Batch Manager.")

        # スケジュールを登録
        service.schedule_batches(scheduler)

        # 実行中のスケジュール監視ループ
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logging.info("Shutting down Batch Manager...")
        scheduler.shutdown()
