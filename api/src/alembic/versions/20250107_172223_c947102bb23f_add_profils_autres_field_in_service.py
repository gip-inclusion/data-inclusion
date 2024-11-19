"""add profils_precisions field in service

Revision ID: c947102bb23f
Revises: bc02c1c6f60e
Create Date: 2025-01-07 17:22:23.374004

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import TSVECTOR

# revision identifiers, used by Alembic.
revision = "c947102bb23f"
down_revision = "bc02c1c6f60e"
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
    op.execute("""
        CREATE OR REPLACE FUNCTION generate_profils_searchable(
            profils TEXT[]
        )
        RETURNS TSVECTOR AS $$
        BEGIN
            RETURN to_tsvector(
               'french',
                ARRAY_TO_STRING(profils, ' ')::text
            );
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;
    """)
    op.add_column(
        "api__services",
        sa.Column(
            "searchable_index_profils",
            TSVECTOR(),
            sa.Computed(
                "generate_profils_searchable(profils)",
                persisted=True,
            ),
        ),
    )
    op.create_index(
        "ix_api__services_searchable_index_profils",
        "api__services",
        ["searchable_index_profils"],
        postgresql_using="gin",
    )
    # Create a new dictionary to customize our search without accents
    op.execute("""
        CREATE EXTENSION IF NOT EXISTS unaccent;
        CREATE TEXT SEARCH CONFIGURATION french_di ( COPY = french );
        ALTER TEXT SEARCH CONFIGURATION french_di
            ALTER MAPPING FOR hword, hword_part, word
        WITH unaccent, french_stem;
    """)


def downgrade() -> None:
    op.drop_column("api__services", "searchable_index_profils_precisions")
    op.drop_column("api__services", "profils_precisions")
    op.drop_column("api__services", "searchable_index_profils")
    op.execute("DROP TEXT SEARCH CONFIGURATION french_di")
