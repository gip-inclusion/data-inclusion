"""add origin to search services events

Revision ID: add_origin_search_events
Revises: 33b15080c36c
Create Date: 2026-04-23 08:11:26

"""

import sqlalchemy as sa
from alembic import op

revision = "add_origin_search_events"
down_revision = "33b15080c36c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__search_services_events_v1",
        sa.Column("origin", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("api__search_services_events_v1", "origin")
