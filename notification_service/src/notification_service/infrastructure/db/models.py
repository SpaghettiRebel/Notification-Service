import enum

from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, Enum

from notification_service.infrastructure.db.session import Base


class NotificationStatus(str, enum.Enum):
    PENDING = 'pending'
    COMPLETED = 'completed'
    FAILED = 'failed'


class NotificationLog(Base):
    __tablename__ = 'notification_log'

    event_id = Column(String, primary_key=True, index=True)
    status = Column(Enum(NotificationStatus), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )
