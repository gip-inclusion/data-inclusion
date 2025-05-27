"""cleanup_profils_index

Revision ID: 31b2fbaa9153
Revises: 5c4953edb6bc
Create Date: 2025-05-27 08:43:20.039264

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "31b2fbaa9153"
down_revision = "5c4953edb6bc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the wrongly named function and recreate with the correct name
    # Replace dashes with spaces in profils values
    op.execute("""
        DROP INDEX IF EXISTS ix_api__services_searchable_index_profils;
        DROP INDEX IF EXISTS ix_api__services_searchable_index_profils_precisions;
        ALTER TABLE api__services DROP COLUMN searchable_index_profils;
        ALTER TABLE api__services DROP COLUMN searchable_index_profils_precisions;
        DROP FUNCTION IF EXISTS generate_profils;
        DROP FUNCTION IF EXISTS generate_profils_precisions;
        DROP FUNCTION IF EXISTS generate_profils_searchable;
    """)
    op.execute("""
        CREATE FUNCTION generate_profils(
            profils TEXT[]
        )
        RETURNS TSVECTOR AS $$
        BEGIN
            RETURN to_tsvector(
                'french',
                REPLACE(ARRAY_TO_STRING(profils, ' '), '-', ' ')
            );
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;
    """)
    op.execute("""
        CREATE FUNCTION generate_profils_precisions(
            profils_precisions TEXT,
            profils TEXT[]
        )
        RETURNS TSVECTOR AS $$
        BEGIN
            RETURN to_tsvector(
                'french',
                COALESCE(profils_precisions, '')
                || ' '
                || COALESCE(REPLACE(ARRAY_TO_STRING(profils, ' '), '-', ' '), '')
            );
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;
    """)
    op.execute("""
        ALTER TABLE api__services
        ADD COLUMN searchable_index_profils TSVECTOR
            GENERATED ALWAYS AS (generate_profils(profils)) STORED;
    """)
    op.execute("""
        ALTER TABLE api__services
        ADD COLUMN searchable_index_profils_precisions TSVECTOR
            GENERATED ALWAYS AS (generate_profils_precisions(profils_precisions, profils)) STORED;
    """)  # noqa: E501
    op.create_index(
        op.f("ix_api__services__searchable_index_profils"),
        "api__services",
        ["searchable_index_profils"],
        unique=False,
        postgresql_using="gin",
    )
    op.create_index(
        op.f("ix_api__services__searchable_index_profils_precisions"),
        "api__services",
        ["searchable_index_profils_precisions"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    pass
