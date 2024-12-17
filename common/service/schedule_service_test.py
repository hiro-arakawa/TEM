from unittest.mock import MagicMock
from common.repository.schedule_repository import ScheduleDTO
from schedule_service import ScheduleService

def test_get_schedules():
    # モックされたリポジトリ
    repository_mock = MagicMock()
    repository_mock.fetch_schedules.return_value = [
        ScheduleDTO(1, "Meeting", "10:00", "11:00", 1),
        ScheduleDTO(2, "Lunch", "12:00", "13:00", 0),
    ]

    service = ScheduleService(repository_mock)
    result = service.get_schedules()

    # 結果が正しいか検証
    assert len(result) == 2  # 全てのスケジュールが取得されているか確認
    assert result[0].schedule_id == 1
    assert result[0].title == "Meeting"

    # リポジトリのメソッドが1回呼ばれたことを確認
    repository_mock.fetch_schedules.assert_called_once()
