"""replace profils by publics

Revision ID: de144d291914
Revises: 62d40df29888
Create Date: 2025-06-17 19:13:34.031934

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from data_inclusion.api.core.db import SortedTextArray

# revision identifiers, used by Alembic.
revision = "de144d291914"
down_revision = "28b91f8435e8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services_v1",
        sa.Column("publics", SortedTextArray(sa.Text()), nullable=True),
    )
    op.add_column(
        "api__services_v1",
        sa.Column("publics_precisions", sa.String(), nullable=True),
    )
    op.drop_index(
        op.f("ix_api__services_v1__searchable_index_profils_precisions"),
        table_name="api__services_v1",
        postgresql_using="gin",
    )
    op.drop_index(
        op.f("ix_api__services_v1__searchable_index_profils"),
        table_name="api__services_v1",
        postgresql_using="gin",
    )
    op.drop_column("api__services_v1", "searchable_index_profils_precisions")
    op.drop_column("api__services_v1", "searchable_index_profils")
    op.drop_column("api__services_v1", "profils")
    op.drop_column("api__services_v1", "profils_precisions")
    op.execute("""
        ALTER TABLE api__services_v1
        ADD COLUMN searchable_index_publics TSVECTOR
            GENERATED ALWAYS AS (generate_profils(publics)) STORED;
    """)
    op.execute("""
        ALTER TABLE api__services_v1
        ADD COLUMN searchable_index_publics_precisions TSVECTOR
            GENERATED ALWAYS AS (generate_profils_precisions(publics_precisions, publics)) STORED;
    """)  # noqa: E501
    op.create_index(
        op.f("ix_api__services_v1__searchable_index_publics"),
        "api__services_v1",
        ["searchable_index_publics"],
        unique=False,
        postgresql_using="gin",
    )
    op.create_index(
        op.f("ix_api__services_v1__searchable_index_publics_precisions"),
        "api__services_v1",
        ["searchable_index_publics_precisions"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_api__services_v1__searchable_index_publics"),
        table_name="api__services_v1",
        postgresql_using="gin",
    )
    op.drop_index(
        op.f("ix_api__services_v1__searchable_index_publics_precisions"),
        table_name="api__services_v1",
        postgresql_using="gin",
    )
    op.drop_column("api__services_v1", "searchable_index_publics_precisions")
    op.drop_column("api__services_v1", "searchable_index_publics")
    op.add_column(
        "api__services_v1",
        sa.Column(
            "profils", postgresql.ARRAY(sa.TEXT()), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "api__services_v1",
        sa.Column("profils_precisions", sa.String(), nullable=True),
    )
    op.drop_column("api__services_v1", "publics")
    op.drop_column("api__services_v1", "publics_precisions")
    op.execute("""
        ALTER TABLE api__services_v1
        ADD COLUMN searchable_index_profils TSVECTOR
            GENERATED ALWAYS AS (generate_profils(profils)) STORED;
    """)
    op.execute("""
        ALTER TABLE api__services_v1
        ADD COLUMN searchable_index_profils_precisions TSVECTOR
            GENERATED ALWAYS AS (generate_profils_precisions(profils_precisions, profils)) STORED;
    """)  # noqa: E501
    op.create_index(
        op.f("ix_api__services_v1__searchable_index_profils"),
        "api__services_v1",
        ["searchable_index_profils"],
        unique=False,
        postgresql_using="gin",
    )
    op.create_index(
        op.f("ix_api__services_v1__searchable_index_profils_precisions"),
        "api__services_v1",
        ["searchable_index_profils_precisions"],
        unique=False,
        postgresql_using="gin",
    )
