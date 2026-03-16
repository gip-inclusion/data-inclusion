"""add_search_vector

Revision ID: 8d6c0425b1c8
Revises: add_adresse_certifiee
Create Date: 2026-03-09 12:29:40.858515

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "8d6c0425b1c8"
down_revision = "add_adresse_certifiee"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__structures_v1",
        sa.Column(
            "search_vector",
            postgresql.TSVECTOR(),
            sa.Computed("TO_TSVECTOR('french_di', nom)", persisted=True),
            nullable=False,
        ),
    )
    op.create_index(
        op.f("ix_api__structures_v1__search_vector"),
        "api__structures_v1",
        ["search_vector"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_api__structures_v1__search_vector"),
        table_name="api__structures_v1",
        postgresql_using="gin",
    )
    op.drop_column("api__structures_v1", "search_vector")
