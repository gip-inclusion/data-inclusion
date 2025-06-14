"""remove lien_source

Revision ID: c36ca85cf673
Revises: ae16d2186ce8
Create Date: 2025-06-11 14:31:33.773698

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c36ca85cf673"
down_revision = "ae16d2186ce8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("api__services", "lien_source")
    op.drop_column("api__services_v1", "lien_source")
    op.drop_column("api__structures", "lien_source")
    op.drop_column("api__structures_v1", "lien_source")


def downgrade() -> None:
    op.add_column(
        "api__structures",
        sa.Column("lien_source", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "api__services",
        sa.Column("lien_source", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "api__structures_v1",
        sa.Column("lien_source", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "api__services_v1",
        sa.Column("lien_source", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
