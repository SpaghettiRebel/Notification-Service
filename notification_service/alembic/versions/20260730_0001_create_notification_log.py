"""Create notification log table.

Revision ID: 20260730_0001
Revises:
Create Date: 2026-07-30
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260730_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    notification_status = sa.Enum(
        "pending",
        "completed",
        "failed",
        name="notification_status",
    )

    op.create_table(
        "notification_log",
        sa.Column("event_id", sa.String(), nullable=False),
        sa.Column("status", notification_status, nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("event_id"),
    )


def downgrade() -> None:
    op.drop_table("notification_log")
    sa.Enum(name="notification_status").drop(op.get_bind(), checkfirst=True)
