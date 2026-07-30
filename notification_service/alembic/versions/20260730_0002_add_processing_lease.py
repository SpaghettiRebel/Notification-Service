"""Add processing lease and retry metadata.

Revision ID: 20260730_0002
Revises: 20260730_0001
Create Date: 2026-07-30
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260730_0002"
down_revision: str | None = "20260730_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "notification_log",
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "notification_log",
        sa.Column(
            "attempt_count",
            sa.Integer(),
            server_default="1",
            nullable=False,
        ),
    )
    op.add_column(
        "notification_log",
        sa.Column("last_error", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("notification_log", "last_error")
    op.drop_column("notification_log", "attempt_count")
    op.drop_column("notification_log", "locked_until")
