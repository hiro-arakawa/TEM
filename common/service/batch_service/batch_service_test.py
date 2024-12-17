import pytest
from datetime import datetime, timedelta
from batch_service import BatchService

# ダミー日時を固定するためのヘルパークラス
class FixedDatetime(datetime):
    fixed_now = None

    @classmethod
    def now(cls):
        return cls.fixed_now

    @classmethod
    def min(cls):
        return datetime.min


@pytest.fixture
def batch_service():
    """BatchService のインスタンスを提供するフィクスチャ"""
    service = BatchService()

    # テスト用の初期状態
    service.last_processed = {
        "データロード1": datetime.now() - timedelta(hours=2),
        "データロード2": datetime.now() - timedelta(hours=3),
        "bcp実行": datetime.now() - timedelta(hours=4),
        "計算値生成処理": datetime.now() - timedelta(hours=5),
    }
    return service

def test_can_schedule_with_dependency(batch_service):
    """依存関係がある場合のスケジュール判定をテスト"""
    batch = {"batch_name": "bcp実行", "factory_code": "工場A", "depends_on": "データロード1", "schedule_days": ["mon", "tue"]}
    batch_service.last_processed["データロード1"] = "SUCCESS"
    assert batch_service.can_schedule(batch) is True

    # 依存関係が未完了の場合
    batch_service.last_processed["データロード1"] = "FAILED"
    assert batch_service.can_schedule(batch) is False

def test_can_schedule_with_days(batch_service, monkeypatch):
    """特定曜日でのスケジュール判定をテスト"""
    class FixedDatetime(datetime):
        @classmethod
        def now(cls):
            return datetime(2024, 12, 17)  # 火曜日

    monkeypatch.setattr("batch_service.datetime", FixedDatetime)

    batch = {"batch_name": "データロード1", "factory_code": "工場A", "depends_on": None, "schedule_days": ["mon", "tue", "wed"]}
    assert batch_service.can_schedule(batch) is True

    # 火曜日でない場合
    batch["schedule_days"] = ["thu", "fri"]
    assert batch_service.can_schedule(batch) is False

def test_update_last_processed(batch_service):
    """バッチ処理の状態更新をテスト"""
    batch_service.update_last_processed("データロード1", "SUCCESS")
    assert batch_service.last_processed["データロード1"] == "SUCCESS"

    batch_service.update_last_processed("データロード2", "FAILED")
    assert batch_service.last_processed["データロード2"] == "FAILED"


def test_get_pending_batches(batch_service, monkeypatch):
    """未処理バッチの取得をテスト"""
    class FixedDatetime(datetime):
        @classmethod
        def now(cls):
            return datetime(2024, 12, 17, 10, 0)  # 2024年12月17日 10:00

    batch_service.datetime = FixedDatetime

    # 全て未処理状態にリセット
    batch_service.last_processed = {}

    # 初期状態で全て未処理
    pending_batches = batch_service.get_pending_batches()
    expected_batches = {"データロード1", "データロード2", "bcp実行", "計算値生成処理"}
    assert {batch["batch_name"] for batch in pending_batches} == expected_batches


def test_update_batch_schedule(batch_service):
    """バッチのスケジュール変更をテスト"""
    batch_service.update_batch_schedule("データロード1", ["sat", "sun"])
    updated_batch = next(batch for batch in batch_service.batch_master if batch["batch_name"] == "データロード1")
    assert updated_batch["schedule_days"] == ["sat", "sun"]

    # 存在しないバッチ名を指定
    batch_service.update_batch_schedule("存在しないバッチ", ["mon"])
    for batch in batch_service.batch_master:
        assert batch["batch_name"] != "存在しないバッチ"

def test_retry_count_management(batch_service):
    """再試行カウントの管理をテスト"""
    batch_service.increment_retry_count("データロード1")
    assert batch_service.retry_counts["データロード1"] == 1

    batch_service.increment_retry_count("データロード1")
    assert batch_service.retry_counts["データロード1"] == 2

    # リセット時の動作確認
    batch_service.update_last_processed("データロード1", "SUCCESS")
    assert batch_service.retry_counts["データロード1"] == 0

def test_circular_dependency_detection(batch_service):
    """循環依存がある場合にエラーがスローされるか"""
    batch_service.batch_master = [
        {"batch_name": "BatchA", "factory_code": "A", "depends_on": "BatchB", "schedule_days": ["mon"]},
        {"batch_name": "BatchB", "factory_code": "A", "depends_on": "BatchC", "schedule_days": ["tue"]},
        {"batch_name": "BatchC", "factory_code": "A", "depends_on": "BatchA", "schedule_days": ["wed"]},
    ]
    with pytest.raises(Exception, match="Circular dependency detected"):
        batch_service.detect_circular_dependency()

def test_max_retry_behavior(batch_service):
    """再試行回数が最大値に達したときの挙動"""
    batch_name = "データロード1"
    batch_service.retry_counts[batch_name] = batch_service.max_retry_count
    batch_service.handle_error(batch_name, "工場A", Exception("Test error"))
    assert batch_service.retry_counts[batch_name] == batch_service.max_retry_count
    assert batch_service.last_processed[batch_name] == "FAILED"

def test_high_volume_batches(batch_service):
    """大量のバッチ登録時のパフォーマンス確認"""
    batch_service.batch_master = [
        {"batch_name": f"Batch_{i}", "factory_code": "A", "depends_on": None, "schedule_days": ["mon", "tue", "wed"]}
        for i in range(1000)  # 1000ジョブをテスト
    ]

    # モック日時を注入
    class FixedDatetime(datetime):
        @classmethod
        def now(cls):
            return datetime(2024, 12, 17, 10, 0)  # 火曜日

    batch_service.datetime = FixedDatetime

    pending_batches = batch_service.get_pending_batches()
    assert len(pending_batches) == 1000


def test_schedule_update(batch_service):
    """スケジュール変更が反映されるかを確認"""
    batch_service.update_batch_schedule("データロード1", ["sat", "sun"])
    batch = next(b for b in batch_service.batch_master if b["batch_name"] == "データロード1")
    assert batch["schedule_days"] == ["sat", "sun"]

from unittest.mock import patch

def test_notification_on_failure(batch_service):
    """再試行失敗後の通知を確認"""
    with patch.object(batch_service, "notify_failure") as mock_notify:
        batch_service.retry_counts["データロード1"] = batch_service.max_retry_count
        batch_service.handle_error("データロード1", "工場A", Exception("Simulated failure"))
        mock_notify.assert_called_once_with("データロード1", "工場A")

