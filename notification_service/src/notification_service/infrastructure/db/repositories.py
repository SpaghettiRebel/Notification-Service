from sqlalchemy.ext.asyncio import AsyncSession

from notification_service.infrastructure.db import NotificationLog, NotificationStatus


class NotificationRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def acquire_lock(self, event_id: str) -> str | None:
        ...

    async def update_failed(self, event_id: str) -> None:
        ...

    async def update_completed(self, event_id: str) -> None:
        ...
