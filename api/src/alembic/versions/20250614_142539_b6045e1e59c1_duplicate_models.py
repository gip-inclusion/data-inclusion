"""duplicate models

Revision ID: b6045e1e59c1
Revises: 81ed832cc0e5
Create Date: 2025-06-14 14:25:39.688331

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from data_inclusion.api.core.db import SortedTextArray

# revision identifiers, used by Alembic.
revision = "b6045e1e59c1"
down_revision = "81ed832cc0e5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "api__structures_v1",
        sa.Column("_di_surrogate_id", sa.String(), nullable=False),
        sa.Column("_is_best_duplicate", sa.Boolean(), nullable=True),
        sa.Column("_cluster_id", sa.String(), nullable=True),
        sa.Column("accessibilite", sa.String(), nullable=True),
        sa.Column("antenne", sa.Boolean(), nullable=True),
        sa.Column("courriel", sa.String(), nullable=True),
        sa.Column("date_maj", sa.Date(), nullable=True),
        sa.Column("horaires_ouverture", sa.String(), nullable=True),
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("labels_autres", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("labels_nationaux", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("lien_source", sa.String(), nullable=True),
        sa.Column("nom", sa.String(), nullable=False),
        sa.Column("presentation_detail", sa.String(), nullable=True),
        sa.Column("presentation_resume", sa.String(), nullable=True),
        sa.Column("rna", sa.String(), nullable=True),
        sa.Column("siret", sa.String(), nullable=True),
        sa.Column("site_web", sa.String(), nullable=True),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("telephone", sa.String(), nullable=True),
        sa.Column("typologie", sa.String(), nullable=True),
        sa.Column("score_qualite", sa.Float(), nullable=False),
        sa.Column("adresse", sa.String(), nullable=True),
        sa.Column("complement_adresse", sa.String(), nullable=True),
        sa.Column("code_insee", sa.String(), nullable=True),
        sa.Column("code_postal", sa.String(), nullable=True),
        sa.Column("commune", sa.String(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(
            ["code_insee"],
            ["api__communes.code"],
            name=op.f("fk_api__structures_v1__code_insee__api__communes"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("_di_surrogate_id", name=op.f("pk_api__structures_v1")),
    )
    op.create_index(
        op.f("ix_api__structures_v1___cluster_id"),
        "api__structures_v1",
        ["_cluster_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_api__structures_v1__source"),
        "api__structures_v1",
        ["source"],
        unique=False,
    )

    op.create_table(
        "api__services_v1",
        sa.Column("_di_surrogate_id", sa.String(), nullable=False),
        sa.Column("_di_structure_surrogate_id", sa.String(), nullable=False),
        sa.Column("contact_nom_prenom", sa.String(), nullable=True),
        sa.Column("contact_public", sa.Boolean(), nullable=True),
        sa.Column("courriel", sa.String(), nullable=True),
        sa.Column("cumulable", sa.Boolean(), nullable=True),
        sa.Column("date_creation", sa.Date(), nullable=True),
        sa.Column("date_maj", sa.Date(), nullable=True),
        sa.Column("date_suspension", sa.Date(), nullable=True),
        sa.Column("formulaire_en_ligne", sa.String(), nullable=True),
        sa.Column("frais_autres", sa.String(), nullable=True),
        sa.Column("frais", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("justificatifs", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("lien_source", sa.String(), nullable=True),
        sa.Column("modes_accueil", SortedTextArray(sa.Text()), nullable=True),
        sa.Column(
            "modes_orientation_accompagnateur_autres", sa.String(), nullable=True
        ),
        sa.Column(
            "modes_orientation_accompagnateur",
            SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("modes_orientation_beneficiaire_autres", sa.String(), nullable=True),
        sa.Column(
            "modes_orientation_beneficiaire", SortedTextArray(sa.Text()), nullable=True
        ),
        sa.Column("nom", sa.String(), nullable=False),
        sa.Column("page_web", sa.String(), nullable=True),
        sa.Column("pre_requis", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("presentation_detail", sa.String(), nullable=True),
        sa.Column("presentation_resume", sa.String(), nullable=True),
        sa.Column("prise_rdv", sa.String(), nullable=True),
        sa.Column("profils", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("profils_precisions", sa.String(), nullable=True),
        sa.Column(
            "searchable_index_profils_precisions",
            postgresql.TSVECTOR(),
            sa.Computed(
                "generate_profils_precisions(profils_precisions, profils)",
                persisted=True,
            ),
            nullable=True,
        ),
        sa.Column(
            "searchable_index_profils",
            postgresql.TSVECTOR(),
            sa.Computed("generate_profils(profils)", persisted=True),
            nullable=True,
        ),
        sa.Column("recurrence", sa.String(), nullable=True),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("structure_id", sa.String(), nullable=False),
        sa.Column("telephone", sa.String(), nullable=True),
        sa.Column("thematiques", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("types", SortedTextArray(sa.Text()), nullable=True),
        sa.Column("zone_diffusion_code", sa.String(), nullable=True),
        sa.Column("zone_diffusion_nom", sa.String(), nullable=True),
        sa.Column("zone_diffusion_type", sa.String(), nullable=True),
        sa.Column("score_qualite", sa.Float(), nullable=False),
        sa.Column("adresse", sa.String(), nullable=True),
        sa.Column("complement_adresse", sa.String(), nullable=True),
        sa.Column("code_insee", sa.String(), nullable=True),
        sa.Column("code_postal", sa.String(), nullable=True),
        sa.Column("commune", sa.String(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(
            ["_di_structure_surrogate_id"],
            ["api__structures_v1._di_surrogate_id"],
            name=op.f(
                "fk_api__services_v1___di_structure_surrogate_id__api__structures_v1"
            ),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["code_insee"],
            ["api__communes.code"],
            name=op.f("fk_api__services_v1__code_insee__api__communes"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("_di_surrogate_id", name=op.f("pk_api__services_v1")),
    )
    op.create_index(
        "ix_api__services_v1__geography",
        "api__services_v1",
        [
            sa.literal_column(
                "CAST(ST_MakePoint(longitude, latitude) AS geography(geometry, 4326))"
            )
        ],
        unique=False,
        postgresql_using="gist",
    )
    op.create_index(
        op.f("ix_api__services_v1___di_structure_surrogate_id"),
        "api__services_v1",
        ["_di_structure_surrogate_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_api__services_v1__modes_accueil"),
        "api__services_v1",
        ["modes_accueil"],
        unique=False,
        postgresql_using="gin",
    )
    op.create_index(
        op.f("ix_api__services_v1__source"),
        "api__services_v1",
        ["source"],
        unique=False,
    )
    op.create_index(
        op.f("ix_api__services_v1__thematiques"),
        "api__services_v1",
        ["thematiques"],
        unique=False,
        postgresql_using="gin",
    )
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


def downgrade() -> None:
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
    op.drop_index(
        op.f("ix_api__services_v1__thematiques"),
        table_name="api__services_v1",
        postgresql_using="gin",
    )
    op.drop_index(op.f("ix_api__services_v1__source"), table_name="api__services_v1")
    op.drop_index(
        op.f("ix_api__services_v1__modes_accueil"),
        table_name="api__services_v1",
        postgresql_using="gin",
    )
    op.drop_index(
        op.f("ix_api__services_v1___di_structure_surrogate_id"),
        table_name="api__services_v1",
    )
    op.drop_index(
        "ix_api__services_v1__geography",
        table_name="api__services_v1",
        postgresql_using="gist",
    )
    op.drop_table("api__services_v1")
    op.drop_index(
        op.f("ix_api__structures_v1__source"), table_name="api__structures_v1"
    )
    op.drop_index(
        op.f("ix_api__structures_v1___cluster_id"), table_name="api__structures_v1"
    )
    op.drop_table("api__structures_v1")
