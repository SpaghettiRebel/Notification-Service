from sqlalchemy.ext.asyncio import AsyncSession

from notification_service.infrastructure.db import NotificationLog, NotificationStatus
from notification_service.infrastructure.db import async_session_factory


class NotificationRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    def acquire_lock(self, event_id: str) -> str | None:
        ...

    def update_failed(self, event_id: str) -> None:
        ...

    def update_completed(self, event_id: str) -> None:
        ...


def get_pg_repo():
    return NotificationRepository(async_session_factory)
