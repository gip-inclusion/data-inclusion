"""refactor dedup data

Revision ID: 81ed832cc0e5
Revises: 31b2fbaa9153
Create Date: 2025-06-12 09:52:22.854674

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "81ed832cc0e5"
down_revision = "31b2fbaa9153"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__structures", sa.Column("_is_best_duplicate", sa.Boolean(), nullable=True)
    )
    op.add_column(
        "api__structures", sa.Column("_cluster_id", sa.String(), nullable=True)
    )
    op.create_index(
        op.f("ix_api__structures___cluster_id"),
        "api__structures",
        ["_cluster_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_api__structures___cluster_id"), table_name="api__structures")
    op.drop_column("api__structures", "_cluster_id")
    op.drop_column("api__structures", "_is_best_duplicate")
