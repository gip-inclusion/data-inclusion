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
    # 1. Drop the wrongly named function and recreate with the correct name
    # 2. Replace dashes with spaces in profils values
    op.execute("""
        ALTER TABLE api__services
        ALTER searchable_index_profils
        SET EXPRESSION AS (NULL);
    """)
    op.execute("""
        DROP FUNCTION IF EXISTS generate_profils_searchable;
    """)
    op.execute("""
        CREATE OR REPLACE FUNCTION generate_profils(
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
        CREATE OR REPLACE FUNCTION generate_profils_precisions(
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
    # 3. Trigger rewrite of the generated columns
    op.execute("""
        ALTER TABLE api__services
        ALTER searchable_index_profils
        SET EXPRESSION AS (generate_profils(profils));
    """)
    op.execute("""
        ALTER TABLE api__services
        ALTER searchable_index_profils_precisions
        SET EXPRESSION AS (generate_profils_precisions(profils_precisions, profils));
    """)


def downgrade() -> None:
    pass
