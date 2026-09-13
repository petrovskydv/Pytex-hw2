"""создание таблицы агрегированной активности оплат

Revision ID: 0003
Revises: 0002
"""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "event_payment_activity",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("payments_count", sa.Integer(), nullable=False),
        sa.Column("tickets_count", sa.Integer(), nullable=False),
        sa.Column("total_amount", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("event_payment_activity")
