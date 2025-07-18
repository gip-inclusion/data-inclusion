"""duplicate analytics models

Revision ID: 879314ff2c3d
Revises: ff6728d79cee
Create Date: 2025-07-17 17:23:56.683233

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from data_inclusion.api.core.db import SortedTextArray

# revision identifiers, used by Alembic.
revision = "879314ff2c3d"
down_revision = "ff6728d79cee"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "api__consult_service_events_v1",
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
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__consult_service_events_v1")),
    )
    op.create_table(
        "api__consult_structure_events_v1",
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
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__consult_structure_events_v1")),
    )
    op.create_table(
        "api__list_services_events_v1",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("user", sa.String(), nullable=False),
        sa.Column("sources", SortedTextArray(sa.Text()), nullable=True),
        sa.Column(
            "thematiques",
            SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("code_departement", sa.String(), nullable=True),
        sa.Column("code_region", sa.String(), nullable=True),
        sa.Column("code_commune", sa.String(), nullable=True),
        sa.Column("frais", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("publics", SortedTextArray(sa.Text()), nullable=True),
        sa.Column(
            "modes_accueil",
            SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("types", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("recherche_public", sa.String(), nullable=True),
        sa.Column("score_qualite_minimum", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__list_services_events_v1")),
    )
    op.create_table(
        "api__list_structures_events_v1",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("user", sa.String(), nullable=False),
        sa.Column("sources", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("typologie", sa.String(), nullable=True),
        sa.Column("label_national", sa.String(), nullable=True),
        sa.Column("code_departement", sa.String(), nullable=True),
        sa.Column("code_region", sa.String(), nullable=True),
        sa.Column("code_commune", sa.String(), nullable=True),
        sa.Column("exclure_doublons", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__list_structures_events_v1")),
    )
    op.create_table(
        "api__search_services_events_v1",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("user", sa.String(), nullable=False),
        sa.Column(
            "first_services", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column("total_services", sa.Integer(), nullable=True),
        sa.Column("sources", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("code_commune", sa.String(), nullable=True),
        sa.Column("lat", sa.Float(), nullable=True),
        sa.Column("lon", sa.Float(), nullable=True),
        sa.Column(
            "thematiques",
            SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("frais", SortedTextArray(sa.Text()), nullable=True),
        sa.Column(
            "modes_accueil",
            SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("publics", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("types", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("recherche_public", sa.String(), nullable=True),
        sa.Column("score_qualite_minimum", sa.Float(), nullable=True),
        sa.Column("exclure_doublons", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__search_services_events_v1")),
    )
    op.drop_column("api__consult_service_events", "schema_version")
    op.drop_column("api__consult_structure_events", "schema_version")
    op.drop_column("api__list_services_events", "schema_version")
    op.drop_column("api__list_structures_events", "schema_version")
    op.drop_column("api__search_services_events", "schema_version")


def downgrade() -> None:
    op.add_column(
        "api__search_services_events",
        sa.Column("schema_version", sa.VARCHAR(), autoincrement=False, nullable=False),
    )
    op.add_column(
        "api__list_structures_events",
        sa.Column("schema_version", sa.VARCHAR(), autoincrement=False, nullable=False),
    )
    op.add_column(
        "api__list_services_events",
        sa.Column("schema_version", sa.VARCHAR(), autoincrement=False, nullable=False),
    )
    op.add_column(
        "api__consult_structure_events",
        sa.Column("schema_version", sa.VARCHAR(), autoincrement=False, nullable=False),
    )
    op.add_column(
        "api__consult_service_events",
        sa.Column("schema_version", sa.VARCHAR(), autoincrement=False, nullable=False),
    )
    op.drop_table("api__search_services_events_v1")
    op.drop_table("api__list_structures_events_v1")
    op.drop_table("api__list_services_events_v1")
    op.drop_table("api__consult_structure_events_v1")
    op.drop_table("api__consult_service_events_v1")
