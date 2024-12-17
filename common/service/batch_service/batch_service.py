from datetime import datetime, timedelta
import logging

# ログ設定
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

class BatchService:
    def __init__(self, datetime_module=datetime):
        self.datetime = datetime_module
        # バッチマスタ: 処理依存やスケジュール
        self.batch_master = [
            {"batch_name": "データロード1", "factory_code": "工場A", "depends_on": None, "schedule_days": ["mon", "tue", "wed", "thu", "fri"]},
            {"batch_name": "データロード2", "factory_code": "工場B", "depends_on": None, "schedule_days": ["mon", "wed", "fri"]},
            {"batch_name": "bcp実行", "factory_code": "工場A", "depends_on": "データロード1", "schedule_days": ["mon", "wed", "fri"]},
            {"batch_name": "計算値生成処理", "factory_code": "工場A", "depends_on": "bcp実行", "schedule_days": ["tue", "thu"]},
        ]
        # 最終処理完了日時
        self.last_processed = {}
        # 再試行カウント
        self.retry_counts = {batch["batch_name"]: 0 for batch in self.batch_master}
        self.max_retry_count = 3

    def can_schedule(self, batch):
        """依存ジョブのスケジュール可否判定"""
        dependency = batch.get("depends_on")
        if dependency and self.last_processed.get(dependency) != "SUCCESS":
            return False
        schedule_days = batch.get("schedule_days", ["mon", "tue", "wed", "thu", "fri", "sat", "sun"])
        current_day = self.datetime.now().strftime("%a").lower()
        return current_day in schedule_days

    def schedule_batches(self, scheduler):
        """未処理バッチのスケジュール登録"""
        pending_batches = self.get_pending_batches()
        for batch in pending_batches:
            self.schedule_batch(scheduler, batch)

    def schedule_batch(self, scheduler, batch):
            """単一バッチをスケジュール"""
            if not self.can_schedule(batch):
                logging.warning(f"Batch {batch['batch_name']} skipped due to dependency or schedule constraints.")
                return

            scheduler.add_job(
                self.batch_task,
                'date',
                run_date=self.datetime.now() + timedelta(seconds=5),
                args=[batch["batch_name"], batch["factory_code"]],
                id=f"{batch['batch_name']}_{batch['factory_code']}",
            )
            logging.info(f"Scheduled batch: {batch['batch_name']}")

    def batch_task(self, batch_name, factory_code):
        """バッチ実行ロジック"""
        logging.info(f"Starting batch: {batch_name}, Factory: {factory_code}")
        try:
            # ダミー処理: エラーをシミュレーション
            if batch_name == "bcp実行":
                raise Exception("Simulated error in bcp実行")
            self.handle_success(batch_name)
        except Exception as e:
            self.handle_error(batch_name, factory_code, e)


    def handle_success(self, batch_name):
        """バッチ成功時の処理"""
        logging.info(f"Batch {batch_name} completed successfully.")
        self.update_last_processed(batch_name, "SUCCESS")
        self.schedule_dependent_batches(batch_name)

    def handle_error(self, batch_name, factory_code, error):
        """エラー時の処理"""
        retry_count = self.retry_counts[batch_name]
        logging.error(f"Error in batch {batch_name}: {error}. Retry count: {retry_count}/{self.max_retry_count}")

        if retry_count < self.max_retry_count:
            self.retry_counts[batch_name] += 1
            self.schedule_retry(batch_name, factory_code)
        else:
            logging.error(f"Batch {batch_name} failed after {self.max_retry_count} retries. Giving up.")
            self.update_last_processed(batch_name, "FAILED")
            self.notify_failure(batch_name, factory_code)
            self.skip_dependent_batches(batch_name)


    def schedule_retry(self, batch_name, factory_code):
        """再試行バッチをスケジュール"""
        retry_delay = 10  # 再試行遅延秒数
        logging.info(f"Retrying batch: {batch_name} after {retry_delay} seconds.")
        # スケジューラを再登録する処理は外部で対応

    def schedule_dependent_batches(self, completed_batch_name):
        """依存関係のジョブを再スケジュール"""
        dependent_batches = [
            batch for batch in self.batch_master
            if batch["depends_on"] == completed_batch_name
        ]
        for batch in dependent_batches:
            logging.info(f"Re-scheduling dependent batch: {batch['batch_name']}")

    def skip_dependent_batches(self, failed_batch_name):
        """失敗したジョブに依存するジョブをスキップ"""
        dependent_batches = [
            batch for batch in self.batch_master
            if batch["depends_on"] == failed_batch_name
        ]
        for batch in dependent_batches:
            self.update_last_processed(batch["batch_name"], "SKIPPED")
            logging.warning(f"Skipped dependent batch: {batch['batch_name']} due to failure of {failed_batch_name}.")


    def notify_failure(self, batch_name, factory_code):
        """失敗時の通知"""
        logging.error(f"Sending failure notification for batch: {batch_name}, Factory: {factory_code}")


    def update_last_processed(self, batch_name, status):
        """処理状態を更新"""
        self.last_processed[batch_name] = status
        if status == "SUCCESS":
            self.retry_counts[batch_name] = 0

    def get_pending_batches(self):
        """未処理バッチの取得"""
        current_time = self.datetime.now()
        one_hour_ago = current_time - timedelta(hours=1)

        return [
            batch
            for batch in self.batch_master
            if self.last_processed.get(batch["batch_name"], datetime.min) < one_hour_ago
        ]

    def update_batch_schedule(self, batch_name, new_schedule_days):
        """バッチのスケジュールを動的に変更"""
        for batch in self.batch_master:
            if batch["batch_name"] == batch_name:
                batch["schedule_days"] = new_schedule_days
                logging.info(f"Updated schedule for {batch_name}. New days: {new_schedule_days}")
                return
        logging.warning(f"Batch {batch_name} not found. Cannot update schedule.")

    def increment_retry_count(self, batch_name):
        """再試行回数をインクリメント"""
        if batch_name in self.retry_counts:
            self.retry_counts[batch_name] += 1

    def detect_circular_dependency(self):
        """循環依存を検出"""
        visited = set()

        def visit(batch_name, stack):
            if batch_name in stack:
                raise Exception(f"Circular dependency detected: {' -> '.join(stack)} -> {batch_name}")
            if batch_name not in visited:
                visited.add(batch_name)
                stack.append(batch_name)
                depends_on = next(
                    (batch["depends_on"] for batch in self.batch_master if batch["batch_name"] == batch_name), None
                )
                if depends_on:
                    visit(depends_on, stack)
                stack.pop()

        for batch in self.batch_master:
            visit(batch["batch_name"], [])


