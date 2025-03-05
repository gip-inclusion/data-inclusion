"""api__structures : add cluster_master_id column

Revision ID: 9c7df6028d95
Revises: 45104a7fe9e8
Create Date: 2025-03-05 09:12:52.570500

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9c7df6028d95"
down_revision = "45104a7fe9e8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__structures", sa.Column("cluster_master_id", sa.String(), nullable=True)
    )
    op.create_index(
        op.f("ix_api__structures__cluster_master_id"),
        "api__structures",
        ["cluster_master_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_api__structures__cluster_master_id"), table_name="api__structures"
    )
    op.drop_column("api__structures", "cluster_master_id")
