"""add is_valid_* columns

Revision ID: bd947d8fa595
Revises: b1739bed686e
Create Date: 2025-06-10 12:09:13.973614

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "bd947d8fa595"
down_revision = "b1739bed686e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services",
        sa.Column("_is_valid_v0", sa.Boolean(), nullable=False, default=True),
    )
    op.add_column(
        "api__services",
        sa.Column("_is_valid_v1", sa.Boolean(), nullable=False, default=True),
    )
    op.add_column(
        "api__structures",
        sa.Column("_is_valid_v0", sa.Boolean(), nullable=False, default=True),
    )
    op.add_column(
        "api__structures",
        sa.Column("_is_valid_v1", sa.Boolean(), nullable=False, default=True),
    )


def downgrade() -> None:
    op.drop_column("api__structures", "_is_valid_v1")
    op.drop_column("api__structures", "_is_valid_v0")
    op.drop_column("api__services", "_is_valid_v1")
    op.drop_column("api__services", "_is_valid_v0")
