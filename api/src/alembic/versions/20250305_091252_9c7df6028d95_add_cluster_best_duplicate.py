"""api__structures : add cluster_best_duplicate column

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
    op.drop_index(op.f("ix_api__structures__cluster_id"), table_name="api__structures")
    op.drop_column("api__structures", "cluster_id")
    op.add_column(
        "api__structures",
        sa.Column("cluster_best_duplicate", sa.String(), nullable=True),
    )
    op.create_index(
        op.f("ix_api__structures__cluster_best_duplicate"),
        "api__structures",
        ["cluster_best_duplicate"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_api__structures__cluster_best_duplicate"), table_name="api__structures"
    )
    op.drop_column("api__structures", "cluster_best_duplicate")
    op.add_column(
        "api__structures", sa.Column("cluster_id", sa.String(), nullable=True)
    )
    op.create_index(
        op.f("ix_api__structures__cluster_id"),
        "api__structures",
        ["cluster_id"],
        unique=False,
    )
