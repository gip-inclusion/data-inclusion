"""All tables

Revision ID: all
Revises: init
Create Date: 2026-03-10 14:59:38.157709

"""

import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from data_inclusion.api.core import db

# revision identifiers, used by Alembic.
revision = "all"
down_revision = "init"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "api__communes",
        sa.Column("code", sa.String(), nullable=False),
        sa.Column("nom", sa.String(), nullable=False),
        sa.Column("departement", sa.String(), nullable=False),
        sa.Column("region", sa.String(), nullable=False),
        sa.Column("siren_epci", sa.String(), nullable=True),
        sa.Column(
            "codes_postaux",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "centre",
            geoalchemy2.types.Geometry(
                srid=4326,
                dimension=2,
                from_text="ST_GeomFromEWKT",
                name="geometry",
                spatial_index=False,
            ),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("code", name=op.f("pk_api__communes")),
    )
    op.create_index(
        "ix_api__communes__centre",
        "api__communes",
        ["centre"],
        unique=False,
        postgresql_using="gist",
    )

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
        sa.Column("score_qualite", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__consult_service_events_v1")),
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
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__consult_structure_events_v1")),
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
        sa.Column("sources", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column(
            "thematiques",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("code_departement", sa.String(), nullable=True),
        sa.Column("code_region", sa.String(), nullable=True),
        sa.Column("code_commune", sa.String(), nullable=True),
        sa.Column("frais", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("profils", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column(
            "modes_accueil",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("types", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("recherche_public", sa.String(), nullable=True),
        sa.Column("score_qualite_minimum", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__list_services_events")),
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
        sa.Column("sources", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column(
            "thematiques",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("code_departement", sa.String(), nullable=True),
        sa.Column("code_region", sa.String(), nullable=True),
        sa.Column("code_commune", sa.String(), nullable=True),
        sa.Column("frais", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("publics", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column(
            "modes_accueil",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("types", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("recherche_public", sa.String(), nullable=True),
        sa.Column("score_qualite_minimum", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__list_services_events_v1")),
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
        sa.Column("sources", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("typologie", sa.String(), nullable=True),
        sa.Column("label_national", sa.String(), nullable=True),
        sa.Column("code_departement", sa.String(), nullable=True),
        sa.Column("code_region", sa.String(), nullable=True),
        sa.Column("code_commune", sa.String(), nullable=True),
        sa.Column(
            "thematiques",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("exclure_doublons", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__list_structures_events")),
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
        sa.Column("sources", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("reseaux_porteurs", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("code_departement", sa.String(), nullable=True),
        sa.Column("code_region", sa.String(), nullable=True),
        sa.Column("code_commune", sa.String(), nullable=True),
        sa.Column("exclure_doublons", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__list_structures_events_v1")),
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
            "first_services", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column("total_services", sa.Integer(), nullable=True),
        sa.Column("sources", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("code_commune", sa.String(), nullable=True),
        sa.Column("lat", sa.Float(), nullable=True),
        sa.Column("lon", sa.Float(), nullable=True),
        sa.Column(
            "thematiques",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("frais", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column(
            "modes_accueil",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("profils", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("types", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("recherche_public", sa.String(), nullable=True),
        sa.Column("score_qualite_minimum", sa.Float(), nullable=True),
        sa.Column("exclure_doublons", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__search_services_events")),
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
        sa.Column("sources", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("code_commune", sa.String(), nullable=True),
        sa.Column("lat", sa.Float(), nullable=True),
        sa.Column("lon", sa.Float(), nullable=True),
        sa.Column(
            "thematiques",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("frais", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column(
            "modes_accueil",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("publics", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("types", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("recherche_public", sa.String(), nullable=True),
        sa.Column("score_qualite_minimum", sa.Float(), nullable=True),
        sa.Column("exclure_doublons", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__search_services_events_v1")),
    )
    op.create_table(
        "api__structures",
        sa.Column("_di_surrogate_id", sa.String(), nullable=False),
        sa.Column("_cluster_id", sa.String(), nullable=True),
        sa.Column("accessibilite", sa.String(), nullable=True),
        sa.Column("courriel", sa.String(), nullable=True),
        sa.Column("date_maj", sa.Date(), nullable=True),
        sa.Column("horaires_ouverture", sa.String(), nullable=True),
        sa.Column("id", sa.String(), nullable=False),
        sa.Column(
            "labels_autres",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "labels_nationaux",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("nom", sa.String(), nullable=False),
        sa.Column("presentation_detail", sa.String(), nullable=True),
        sa.Column("presentation_resume", sa.String(), nullable=True),
        sa.Column("rna", sa.String(), nullable=True),
        sa.Column("siret", sa.String(), nullable=True),
        sa.Column("site_web", sa.String(), nullable=True),
        sa.Column("lien_source", sa.String(), nullable=True),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("telephone", sa.String(), nullable=True),
        sa.Column(
            "thematiques",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("typologie", sa.String(), nullable=True),
        sa.Column("score_qualite", sa.Float(), nullable=False),
        sa.Column("cluster_best_duplicate", sa.String(), nullable=True),
        sa.Column("doublons", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
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
            name=op.f("fk_api__structures__code_insee__api__communes"),
        ),
        sa.PrimaryKeyConstraint("_di_surrogate_id", name=op.f("pk_api__structures")),
    )
    op.create_index(
        op.f("ix_api__structures___cluster_id"),
        "api__structures",
        ["_cluster_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_api__structures__cluster_best_duplicate"),
        "api__structures",
        ["cluster_best_duplicate"],
        unique=False,
    )
    op.create_index(
        "ix_api__structures__cluster_dedup",
        "api__structures",
        [
            "_cluster_id",
            sa.literal_column("score_qualite DESC"),
            sa.literal_column("date_maj DESC"),
        ],
        unique=False,
    )
    op.create_index(
        op.f("ix_api__structures__source"), "api__structures", ["source"], unique=False
    )
    op.create_table(
        "api__structures_v1",
        sa.Column("_cluster_id", sa.String(), nullable=True),
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("nom", sa.String(), nullable=False),
        sa.Column("score_qualite", sa.Float(), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("accessibilite_lieu", sa.String(), nullable=True),
        sa.Column("courriel", sa.String(), nullable=True),
        sa.Column("date_maj", sa.Date(), nullable=True),
        sa.Column("horaires_accueil", sa.String(), nullable=True),
        sa.Column(
            "reseaux_porteurs",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("siret", sa.String(), nullable=True),
        sa.Column("site_web", sa.String(), nullable=True),
        sa.Column("lien_source", sa.String(), nullable=True),
        sa.Column("telephone", sa.String(), nullable=True),
        sa.Column("adresse", sa.String(), nullable=True),
        sa.Column("complement_adresse", sa.String(), nullable=True),
        sa.Column("code_insee", sa.String(), nullable=True),
        sa.Column("code_postal", sa.String(), nullable=True),
        sa.Column("commune", sa.String(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("_has_valid_address", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["code_insee"],
            ["api__communes.code"],
            name=op.f("fk_api__structures_v1__code_insee__api__communes"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__structures_v1")),
    )
    op.create_index(
        op.f("ix_api__structures_v1___cluster_id"),
        "api__structures_v1",
        ["_cluster_id"],
        unique=False,
    )
    op.create_index(
        "ix_api__structures_v1__cluster_dedup",
        "api__structures_v1",
        [
            "_cluster_id",
            sa.literal_column("score_qualite DESC"),
            sa.literal_column("date_maj DESC"),
        ],
        unique=False,
    )
    op.create_index(
        op.f("ix_api__structures_v1__score_qualite"),
        "api__structures_v1",
        ["score_qualite"],
        unique=False,
    )
    op.create_index(
        op.f("ix_api__structures_v1__source"),
        "api__structures_v1",
        ["source"],
        unique=False,
    )
    op.create_table(
        "api__services",
        sa.Column("_di_surrogate_id", sa.String(), nullable=False),
        sa.Column("_di_structure_surrogate_id", sa.String(), nullable=False),
        sa.Column("contact_nom_prenom", sa.String(), nullable=True),
        sa.Column("courriel", sa.String(), nullable=True),
        sa.Column("date_maj", sa.Date(), nullable=True),
        sa.Column("formulaire_en_ligne", sa.String(), nullable=True),
        sa.Column("frais_autres", sa.String(), nullable=True),
        sa.Column("frais", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("id", sa.String(), nullable=False),
        sa.Column(
            "justificatifs",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "modes_accueil",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "modes_orientation_accompagnateur_autres", sa.String(), nullable=True
        ),
        sa.Column(
            "modes_orientation_accompagnateur",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("modes_orientation_beneficiaire_autres", sa.String(), nullable=True),
        sa.Column(
            "modes_orientation_beneficiaire",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("nom", sa.String(), nullable=False),
        sa.Column("page_web", sa.String(), nullable=True),
        sa.Column("lien_source", sa.String(), nullable=True),
        sa.Column(
            "pre_requis",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("presentation_detail", sa.String(), nullable=True),
        sa.Column("presentation_resume", sa.String(), nullable=True),
        sa.Column("prise_rdv", sa.String(), nullable=True),
        sa.Column("profils", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("profils_precisions", sa.String(), nullable=True),
        sa.Column(
            "searchable_index_profils_precisions",
            postgresql.TSVECTOR(),
            sa.Computed(
                "TO_TSVECTOR('french', COALESCE(profils_precisions, ''))",
                persisted=True,
            ),
        ),
        sa.Column(
            "searchable_index_profils",
            postgresql.TSVECTOR(),
            sa.Computed(
                "TO_TSVECTOR('french', REPLACE(TEXT_ARRAY_TO_STRING(profils, ' '), '-', ' '))",  # noqa: E501
                persisted=True,
            ),
        ),
        sa.Column("recurrence", sa.String(), nullable=True),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("structure_id", sa.String(), nullable=False),
        sa.Column("telephone", sa.String(), nullable=True),
        sa.Column(
            "thematiques",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("types", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("zone_diffusion_code", sa.String(), nullable=True),
        sa.Column("zone_diffusion_nom", sa.String(), nullable=True),
        sa.Column("zone_diffusion_type", sa.String(), nullable=True),
        sa.Column("score_qualite", sa.Float(), nullable=False),
        sa.Column("volume_horaire_hebdomadaire", sa.Float(), nullable=True),
        sa.Column("nombre_semaines", sa.Integer(), nullable=True),
        sa.Column("adresse", sa.String(), nullable=True),
        sa.Column("complement_adresse", sa.String(), nullable=True),
        sa.Column("code_insee", sa.String(), nullable=True),
        sa.Column("code_postal", sa.String(), nullable=True),
        sa.Column("commune", sa.String(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(
            ["_di_structure_surrogate_id"],
            ["api__structures._di_surrogate_id"],
            name=op.f("fk_api__services___di_structure_surrogate_id__api__structures"),
        ),
        sa.ForeignKeyConstraint(
            ["code_insee"],
            ["api__communes.code"],
            name=op.f("fk_api__services__code_insee__api__communes"),
        ),
        sa.PrimaryKeyConstraint("_di_surrogate_id", name=op.f("pk_api__services")),
    )
    op.create_index(
        op.f("ix_api__services___di_structure_surrogate_id"),
        "api__services",
        ["_di_structure_surrogate_id"],
        unique=False,
    )
    op.create_index(
        "ix_api__services__geography",
        "api__services",
        [
            sa.literal_column(
                "CAST(ST_MakePoint(longitude, latitude) AS geography(geometry, 4326))"
            )
        ],
        unique=False,
        postgresql_using="gist",
    )
    op.create_index(
        op.f("ix_api__services__modes_accueil"),
        "api__services",
        ["modes_accueil"],
        unique=False,
        postgresql_using="gin",
    )
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
    op.create_index(
        op.f("ix_api__services__source"), "api__services", ["source"], unique=False
    )
    op.create_index(
        op.f("ix_api__services__thematiques"),
        "api__services",
        ["thematiques"],
        unique=False,
        postgresql_using="gin",
    )
    op.create_table(
        "api__services_v1",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("structure_id", sa.String(), nullable=False),
        sa.Column("nom", sa.String(), nullable=False),
        sa.Column("score_qualite", sa.Float(), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("date_maj", sa.Date(), nullable=True),
        sa.Column("type", sa.String(), nullable=True),
        sa.Column(
            "thematiques",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("publics", db.SortedTextArray(sa.Text()), nullable=True),
        sa.Column("publics_precisions", sa.String(), nullable=True),
        sa.Column("conditions_acces", sa.String(), nullable=True),
        sa.Column("contact_nom_prenom", sa.String(), nullable=True),
        sa.Column("courriel", sa.String(), nullable=True),
        sa.Column("frais", sa.String(), nullable=True),
        sa.Column("frais_precisions", sa.String(), nullable=True),
        sa.Column("horaires_accueil", sa.String(), nullable=True),
        sa.Column(
            "modes_accueil",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "modes_mobilisation",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("mobilisation_precisions", sa.String(), nullable=True),
        sa.Column(
            "mobilisable_par",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("lien_mobilisation", sa.String(), nullable=True),
        sa.Column("lien_source", sa.String(), nullable=True),
        sa.Column(
            "searchable_index_publics_precisions",
            postgresql.TSVECTOR(),
            sa.Computed(
                "TO_TSVECTOR('french', COALESCE(publics_precisions, ''))",
                persisted=True,
            ),
        ),
        sa.Column(
            "searchable_index_publics",
            postgresql.TSVECTOR(),
            sa.Computed(
                "TO_TSVECTOR('french', REPLACE(TEXT_ARRAY_TO_STRING(publics, ' '), '-', ' '))",  # noqa: E501
                persisted=True,
            ),
        ),
        sa.Column("telephone", sa.String(), nullable=True),
        sa.Column("volume_horaire_hebdomadaire", sa.Float(), nullable=True),
        sa.Column(
            "zone_eligibilite",
            db.SortedTextArray(sa.Text()),
            nullable=True,
        ),
        sa.Column("nombre_semaines", sa.Integer(), nullable=True),
        sa.Column("extra", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("adresse", sa.String(), nullable=True),
        sa.Column("complement_adresse", sa.String(), nullable=True),
        sa.Column("code_insee", sa.String(), nullable=True),
        sa.Column("code_postal", sa.String(), nullable=True),
        sa.Column("commune", sa.String(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("_has_valid_address", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["code_insee"],
            ["api__communes.code"],
            name=op.f("fk_api__services_v1__code_insee__api__communes"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["structure_id"],
            ["api__structures_v1.id"],
            name=op.f("fk_api__services_v1__structure_id__api__structures_v1"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__services_v1")),
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
        op.f("ix_api__services_v1__modes_accueil"),
        "api__services_v1",
        ["modes_accueil"],
        unique=False,
        postgresql_using="gin",
    )
    op.create_index(
        op.f("ix_api__services_v1__score_qualite"),
        "api__services_v1",
        ["score_qualite"],
        unique=False,
    )
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
    op.create_index(
        op.f("ix_api__services_v1__source"),
        "api__services_v1",
        ["source"],
        unique=False,
    )
    op.create_index(
        op.f("ix_api__services_v1__structure_id"),
        "api__services_v1",
        ["structure_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_api__services_v1__thematiques"),
        "api__services_v1",
        ["thematiques"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    raise NotImplementedError()
