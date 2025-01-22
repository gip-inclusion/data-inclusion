"""add 'cluster_id' to the api__structure table

Revision ID: a06b6f7d1cbe
Revises: c947102bb23f
Create Date: 2025-01-16 16:04:38.426636

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a06b6f7d1cbe"
down_revision = "c947102bb23f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__structures", sa.Column("cluster_id", sa.String(), nullable=True)
    )
    op.create_index(
        op.f("ix_api__structures__cluster_id"),
        "api__structures",
        ["cluster_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_api__structures__cluster_id"), table_name="api__structures")
    op.drop_column("api__structures", "cluster_id")
