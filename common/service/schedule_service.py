# schedule_service.py
class ScheduleService:
    def __init__(self, schedule_repository):
        self.schedule_repository = schedule_repository

    def get_schedules(self):
        schedules = self.schedule_repository.fetch_schedules()

        return schedules
