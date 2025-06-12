"""refactor dedup data

Revision ID: 81ed832cc0e5
Revises: bd947d8fa595
Create Date: 2025-06-12 09:52:22.854674

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "81ed832cc0e5"
down_revision = "bd947d8fa595"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__structures", sa.Column("_is_best_duplicate", sa.Boolean(), nullable=True)
    )
    op.add_column(
        "api__structures", sa.Column("_cluster_id", sa.String(), nullable=True)
    )
    op.drop_index(
        "ix_api__structures__cluster_best_duplicate", table_name="api__structures"
    )
    op.create_index(
        op.f("ix_api__structures___cluster_id"),
        "api__structures",
        ["_cluster_id"],
        unique=False,
    )
    op.drop_column("api__structures", "doublons")
    op.drop_column("api__structures", "cluster_best_duplicate")


def downgrade() -> None:
    op.add_column(
        "api__structures",
        sa.Column(
            "cluster_best_duplicate", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "api__structures",
        sa.Column(
            "doublons",
            postgresql.JSONB(astext_type=sa.Text()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.drop_index(op.f("ix_api__structures___cluster_id"), table_name="api__structures")
    op.create_index(
        "ix_api__structures__cluster_best_duplicate",
        "api__structures",
        ["cluster_best_duplicate"],
        unique=False,
    )
    op.drop_column("api__structures", "_cluster_id")
    op.drop_column("api__structures", "_is_best_duplicate")
