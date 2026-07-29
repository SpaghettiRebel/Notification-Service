from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert

from notification_service.infrastructure.db import NotificationLog, NotificationStatus


class NotificationRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def acquire_lock(self, event_id: str) -> str | None:
        query = (
            insert(NotificationLog)
            .values(event_id=event_id, status=NotificationStatus.PENDING)
            .on_conflict_do_update(
                index_elements=['event_id'],
                set_={
                    'status': NotificationStatus.PENDING,
                    'updated_at': datetime.now(timezone.utc),
                },
                where=(NotificationLog.status == NotificationStatus.FAILED)
            )
            .returning(NotificationLog.event_id)
        )

        result = await self.session.execute(query)

        await self.session.commit()
        return result.scalar_one_or_none()

    async def update_status(self, event_id: str, new_status: NotificationStatus) -> None:
        query = (
            update(NotificationLog)
            .where(NotificationLog.event_id == event_id)
            .values(status=new_status)
        )

        await self.session.execute(query)
        await self.session.commit()
