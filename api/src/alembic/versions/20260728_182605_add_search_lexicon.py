"""add search lexicon

Revision ID: 6f1b0c4a72de
Revises: ad210f4de5ee
Create Date: 2026-07-28 18:26:05.155560

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6f1b0c4a72de"
down_revision = "ad210f4de5ee"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS unaccent")
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    # may be missing if the init migration was skipped (e.g. staging)
    op.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_ts_config c
                JOIN pg_namespace n ON n.oid = c.cfgnamespace
                WHERE n.nspname = 'public' AND c.cfgname = 'french'
            ) THEN
                CREATE TEXT SEARCH CONFIGURATION public.french (
                    COPY = pg_catalog.french
                );
            END IF;
        END $$;
        ALTER TEXT SEARCH CONFIGURATION public.french
            ALTER MAPPING FOR hword, hword_part, word
                WITH unaccent, french_stem;
    """)
    op.create_table(
        "api__search_lexicon_v1",
        sa.Column("word", sa.String(), nullable=False),
        sa.Column("ndoc", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("word", name=op.f("pk_api__search_lexicon_v1")),
    )
    op.create_index(
        op.f("ix_api__search_lexicon_v1__word"),
        "api__search_lexicon_v1",
        ["word"],
        unique=False,
        postgresql_using="gin",
        postgresql_ops={"word": "gin_trgm_ops"},
    )


def downgrade() -> None:
    op.drop_table("api__search_lexicon_v1")
