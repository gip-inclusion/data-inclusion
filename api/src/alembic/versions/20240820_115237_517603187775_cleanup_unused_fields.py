"""Cleanup unused fields

Revision ID: 517603187775
Revises: 9f9a66546e3a
Create Date: 2024-08-20 11:52:37.705289

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "517603187775"
down_revision = "9f9a66546e3a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("api__services", "_di_geocodage_score")
    op.drop_column("api__services", "_di_geocodage_code_insee")
    op.drop_column("api__structures", "_di_geocodage_score")
    op.drop_column("api__structures", "_di_geocodage_code_insee")


def downgrade() -> None:
    op.add_column(
        "api__structures",
        sa.Column(
            "_di_geocodage_code_insee", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "api__structures",
        sa.Column(
            "_di_geocodage_score",
            sa.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "api__services",
        sa.Column(
            "_di_geocodage_code_insee", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "api__services",
        sa.Column(
            "_di_geocodage_score",
            sa.DOUBLE_PRECISION(precision=53),
            autoincrement=False,
            nullable=True,
        ),
    )
