"""add sources table relations

Revision ID: 0c9e8a379aa6
Revises: 06e3e22e0541
Create Date: 2024-02-15 16:07:33.512466

"""

import sqlalchemy as sa
from alembic import op

from data_inclusion.api import models

# revision identifiers, used by Alembic.
revision = "0c9e8a379aa6"
down_revision = "06e3e22e0541"
branch_labels = None
depends_on = None


def upgrade() -> None:
    sources_table = op.create_table(
        "sources",
        sa.Column("slug", sa.Text()),
        sa.Column("nom", sa.Text()),
        sa.Column("description", sa.Text()),
        sa.PrimaryKeyConstraint("slug"),
    )
    conn = op.get_bind()
    op.create_unique_constraint("source_slug_unique", "sources", ["slug"])
    sources_slugs_list = list(
        conn.scalars(sa.select(models.Structure.source).distinct())
    )
    op.bulk_insert(sources_table, [{"slug": slug} for slug in sources_slugs_list])


def downgrade() -> None:
    op.alter_column("service", "source", existing_type=sa.TEXT(), nullable=False)
    op.alter_column("structure", "source", existing_type=sa.TEXT(), nullable=False)
