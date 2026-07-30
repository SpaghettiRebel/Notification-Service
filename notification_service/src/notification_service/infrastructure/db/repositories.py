import enum
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert

from notification_service.infrastructure.db.models import NotificationLog, NotificationStatus


class AcquisitionState(str, enum.Enum):
    ACQUIRED = "acquired"
    COMPLETED = "completed"
    BUSY = "busy"


@dataclass(frozen=True, slots=True)
class AcquisitionResult:
    state: AcquisitionState
    locked_until: datetime | None = None


class NotificationRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def acquire_lock(
        self,
        event_id: str,
        lease_seconds: int,
    ) -> AcquisitionResult:
        now = datetime.now(timezone.utc)
        locked_until = now + timedelta(seconds=lease_seconds)

        query = (
            insert(NotificationLog)
            .values(
                event_id=event_id,
                status=NotificationStatus.PENDING,
                locked_until=locked_until,
                attempt_count=1,
            )
            .on_conflict_do_update(
                index_elements=['event_id'],
                set_={
                    'status': NotificationStatus.PENDING,
                    'locked_until': locked_until,
                    'attempt_count': NotificationLog.attempt_count + 1,
                    'last_error': None,
                    'updated_at': now,
                },
                where=or_(
                    NotificationLog.status == NotificationStatus.FAILED,
                    (
                        (NotificationLog.status == NotificationStatus.PENDING)
                        & (
                            NotificationLog.locked_until.is_(None)
                            | (NotificationLog.locked_until <= now)
                        )
                    ),
                ),
            )
            .returning(NotificationLog.event_id)
        )

        result = await self.session.execute(query)
        acquired_event_id = result.scalar_one_or_none()
        await self.session.commit()

        if acquired_event_id is not None:
            return AcquisitionResult(AcquisitionState.ACQUIRED, locked_until)

        existing = await self.session.execute(
            select(NotificationLog.status, NotificationLog.locked_until)
            .where(NotificationLog.event_id == event_id)
        )
        row = existing.one_or_none()

        if row is None:
            raise RuntimeError(
                f"Notification row {event_id} disappeared during lock acquisition"
            )

        if row.status == NotificationStatus.COMPLETED:
            return AcquisitionResult(AcquisitionState.COMPLETED)

        return AcquisitionResult(
            AcquisitionState.BUSY,
            row.locked_until,
        )

    async def update_status(
        self,
        event_id: str,
        new_status: NotificationStatus,
        error: str | None = None,
    ) -> None:
        query = (
            update(NotificationLog)
            .where(NotificationLog.event_id == event_id)
            .values(
                status=new_status,
                locked_until=None,
                last_error=error,
            )
        )

        result = await self.session.execute(query)
        if result.rowcount != 1:
            await self.session.rollback()
            raise RuntimeError(
                f"Notification status update affected {result.rowcount} rows"
            )
        await self.session.commit()
