"""remove date_creation and suspension

Revision ID: 1d0754e89bb2
Revises: 04ed5caccf47
Create Date: 2025-06-11 12:14:05.399820

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1d0754e89bb2"
down_revision = "04ed5caccf47"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("api__services", "date_creation")
    op.drop_column("api__services", "date_suspension")
    op.drop_column("api__services_v1", "date_creation")
    op.drop_column("api__services_v1", "date_suspension")
    op.drop_column("api__list_services_events", "inclure_suspendus")
    op.drop_column("api__search_services_events", "inclure_suspendus")


def downgrade() -> None:
    op.add_column(
        "api__search_services_events",
        sa.Column(
            "inclure_suspendus", sa.BOOLEAN(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "api__list_services_events",
        sa.Column(
            "inclure_suspendus", sa.BOOLEAN(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "api__services_v1",
        sa.Column("date_suspension", sa.Date(), nullable=True),
    )
    op.add_column(
        "api__services_v1",
        sa.Column("date_creation", sa.Date(), nullable=True),
    )
    op.add_column(
        "api__services",
        sa.Column("date_suspension", sa.Date(), nullable=True),
    )
    op.add_column(
        "api__services",
        sa.Column("date_creation", sa.Date(), nullable=True),
    )
