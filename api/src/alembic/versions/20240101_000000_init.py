"""Init

Revision ID: init
Revises:
Create Date: 2024-01-01 00:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    database = conn.engine.url.database

    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    op.execute("""
        CREATE EXTENSION IF NOT EXISTS unaccent;

        ALTER TEXT SEARCH CONFIGURATION french
            ALTER MAPPING FOR hword, hword_part, word
                WITH unaccent, french_stem;
    """)

    op.execute(f"""
        ALTER DATABASE "{database}" SET default_text_search_config = 'french';
    """)

    # This creates an immutable array_to_string function
    # that can be used in computed columns (for indexing)
    op.execute("""
        CREATE OR REPLACE FUNCTION TEXT_ARRAY_TO_STRING(
            arr TEXT[],
            delimiter TEXT
        ) RETURNS TEXT AS $$
        BEGIN
            RETURN array_to_string(arr, delimiter);
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;
    """)


def downgrade() -> None:
    raise NotImplementedError()
