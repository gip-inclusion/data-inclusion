"""create analytics tables

Revision ID: d5351b394a5e
Revises: 68fe052dc63c
Create Date: 2024-12-03 17:57:36.162644

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from data_inclusion.api.core.db import SortedTextArray

# revision identifiers, used by Alembic.
revision = "d5351b394a5e"
down_revision = "68fe052dc63c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "api__consult_service_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("user", sa.String(), nullable=False),
        sa.Column("service_id", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("score_qualite", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__consult_service_events")),
    )
    op.create_table(
        "api__consult_structure_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("user", sa.String(), nullable=False),
        sa.Column("structure_id", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__consult_structure_events")),
    )
    op.create_table(
        "api__list_services_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("user", sa.String(), nullable=False),
        sa.Column("sources", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.Column("thematiques", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.Column("code_departement", sa.String(), nullable=True),
        sa.Column("code_region", sa.String(), nullable=True),
        sa.Column("code_commune", sa.String(), nullable=True),
        sa.Column("frais", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.Column("profils", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.Column("modes_accueil", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.Column("types", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.Column("inclure_suspendus", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__list_services_events")),
    )
    op.create_table(
        "api__list_structures_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("user", sa.String(), nullable=False),
        sa.Column("sources", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.Column("structure_id", sa.String(), nullable=True),
        sa.Column("typologie", sa.String(), nullable=True),
        sa.Column("label_national", sa.String(), nullable=True),
        sa.Column("code_departement", sa.String(), nullable=True),
        sa.Column("code_region", sa.String(), nullable=True),
        sa.Column("code_commune", sa.String(), nullable=True),
        sa.Column("thematiques", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__list_structures_events")),
    )
    op.create_table(
        "api__search_services_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("user", sa.String(), nullable=False),
        sa.Column(
            "first_services",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("total_services", sa.Integer(), nullable=True),
        sa.Column("sources", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.Column("code_commune", sa.String(), nullable=True),
        sa.Column("lat", sa.Float(), nullable=True),
        sa.Column("lon", sa.Float(), nullable=True),
        sa.Column("thematiques", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.Column("frais", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.Column("modes_accueil", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.Column("profils", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.Column("types", SortedTextArray(sa.VARCHAR()), nullable=True),
        sa.Column("inclure_suspendus", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__search_services_events")),
    )


def downgrade() -> None:
    op.drop_table("api__search_services_events")
    op.drop_table("api__list_structures_events")
    op.drop_table("api__list_services_events")
    op.drop_table("api__consult_structure_events")
    op.drop_table("api__consult_service_events")
