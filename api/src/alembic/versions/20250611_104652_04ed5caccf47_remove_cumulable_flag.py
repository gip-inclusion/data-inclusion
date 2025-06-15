"""remove cumulable flag

Revision ID: 04ed5caccf47
Revises: 134cbe8e6f1a
Create Date: 2025-06-11 10:46:52.732533

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "04ed5caccf47"
down_revision = "134cbe8e6f1a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("api__services", "cumulable")
    op.drop_column("api__services_v1", "cumulable")


def downgrade() -> None:
    op.add_column(
        "api__services",
        sa.Column("cumulable", sa.BOOLEAN(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "api__services_v1",
        sa.Column("cumulable", sa.BOOLEAN(), autoincrement=False, nullable=True),
    )
