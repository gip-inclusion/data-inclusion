"""search_index

Revision ID: ad210f4de5ee
Revises: c42ca88c816e
Create Date: 2026-04-23 17:03:25.423418

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "ad210f4de5ee"
down_revision = "c42ca88c816e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services_v1",
        sa.Column(
            "search_vector",
            postgresql.TSVECTOR(),
            nullable=True,
        ),
    )
    op.create_index(
        op.f("ix_api__services_v1__search_vector"),
        "api__services_v1",
        ["search_vector"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_api__services_v1__search_vector"),
        table_name="api__services_v1",
        postgresql_using="gin",
    )
    op.drop_column("api__services_v1", "search_vector")
