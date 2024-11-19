"""add profils_precisions field in service

Revision ID: c947102bb23f
Revises: 68fe052dc63c
Create Date: 2024-10-28 17:22:23.374004

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import TSVECTOR

# revision identifiers, used by Alembic.
revision = "c947102bb23f"
down_revision = "68fe052dc63c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services",
        sa.Column(
            "profils_precisions",
            sa.String(),
            nullable=True,
        ),
    )
    # can't use ARRAY_TO_STRING mutable function in a generation expression.
    # So it must be overiden by an immutable function
    op.execute("""
        CREATE OR REPLACE FUNCTION generate_profils_precisions(
            profils_precisions TEXT,
            profils TEXT[]
        )
        RETURNS TSVECTOR AS $$
        BEGIN
            RETURN to_tsvector(
               'french',
               COALESCE(profils_precisions, '') ||
               ' '||
                COALESCE(ARRAY_TO_STRING(profils, ' '), '')
            );
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;
    """)
    op.add_column(
        "api__services",
        sa.Column(
            "searchable_index_profils_precisions",
            TSVECTOR(),
            sa.Computed(
                "generate_profils_precisions(profils_precisions, profils)",
                persisted=True,
            ),
        ),
    )
    op.create_index(
        "ix_api__services_searchable_index_profils_precisions",
        "api__services",
        ["searchable_index_profils_precisions"],
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_column("api__services", "searchable_index_profils_precisions")
    op.drop_column("api__services", "profils_precisions")
