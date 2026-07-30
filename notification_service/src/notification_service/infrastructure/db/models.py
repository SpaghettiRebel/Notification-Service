import enum
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Enum, Integer, String, Text, func

from notification_service.infrastructure.db.session import Base


class NotificationStatus(str, enum.Enum):
    PENDING = 'pending'
    COMPLETED = 'completed'
    FAILED = 'failed'


class NotificationLog(Base):
    __tablename__ = 'notification_log'

    event_id = Column(String, primary_key=True)
    status = Column(
        Enum(
            NotificationStatus,
            name='notification_status',
            values_callable=lambda enum_class: [item.value for item in enum_class],
        ),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    locked_until = Column(DateTime(timezone=True), nullable=True)
    attempt_count = Column(
        Integer,
        nullable=False,
        default=1,
        server_default='1',
    )
    last_error = Column(Text, nullable=True)
