"""drop first_services from search events

Revision ID: drop_first_services_search
Revises: all
Create Date: 2026-04-09 12:35:43

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "drop_first_services_search"
down_revision = "all"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("api__search_services_events", "first_services")
    op.drop_column("api__search_services_events_v1", "first_services")


def downgrade() -> None:
    op.add_column(
        "api__search_services_events",
        sa.Column(
            "first_services",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "api__search_services_events_v1",
        sa.Column(
            "first_services",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
