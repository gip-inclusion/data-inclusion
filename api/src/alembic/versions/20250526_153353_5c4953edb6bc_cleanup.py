"""cleanup

Revision ID: 5c4953edb6bc
Revises: b1739bed686e
Create Date: 2025-05-26 15:33:53.188548

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "5c4953edb6bc"
down_revision = "b1739bed686e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # the type of the doublons column has changed from JSON to JSONB
    op.alter_column(
        "api__structures",
        "doublons",
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=True,
    )
    op.drop_index(
        "ix_api__structures__cluster_best_duplicate", table_name="api__structures"
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
    op.create_index(
        "ix_api__structures__cluster_best_duplicate",
        "api__structures",
        ["cluster_best_duplicate"],
        unique=False,
    )
    op.alter_column(
        "api__structures",
        "doublons",
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=True,
    )
