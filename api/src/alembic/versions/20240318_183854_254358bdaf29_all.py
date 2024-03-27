"""All

Revision ID: 254358bdaf29
Revises: 0c9e8a379aa6
Create Date: 2024-03-18 18:38:54.205563

"""

import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "254358bdaf29"
down_revision = "0c9e8a379aa6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "api__communes",
        sa.Column("code", sa.String(), nullable=False),
        sa.Column("nom", sa.String(), nullable=False),
        sa.Column("departement", sa.String(), nullable=False),
        sa.Column("region", sa.String(), nullable=False),
        sa.Column("siren_epci", sa.String(), nullable=False),
        sa.Column(
            "geom",
            geoalchemy2.types.Geometry(
                srid=4326,
                spatial_index=False,
                from_text="ST_GeomFromEWKT",
                name="geometry",
            ),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("code", name=op.f("pk_api__communes")),
    )
    op.create_index(
        "ix_api__communes__geography",
        "api__communes",
        [sa.text("CAST(ST_Simplify(geom, 0.01) AS geography(geometry, 4326))")],
        unique=False,
    )
    op.create_table(
        "api__departements",
        sa.Column("code", sa.String(), nullable=False),
        sa.Column("nom", sa.String(), nullable=False),
        sa.Column("insee_reg", sa.String(), nullable=False),
        sa.Column(
            "geom",
            geoalchemy2.types.Geometry(
                srid=4326,
                spatial_index=False,
                from_text="ST_GeomFromEWKT",
                name="geometry",
            ),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("code", name=op.f("pk_api__departements")),
    )
    op.create_index(
        "ix_api__departements__geography",
        "api__departements",
        [sa.text("CAST(ST_Simplify(geom, 0.01) AS geography(geometry, 4326))")],
        unique=False,
    )
    op.create_table(
        "api__epcis",
        sa.Column("code", sa.String(), nullable=False),
        sa.Column("nom", sa.String(), nullable=False),
        sa.Column("nature", sa.String(), nullable=False),
        sa.Column(
            "geom",
            geoalchemy2.types.Geometry(
                srid=4326,
                spatial_index=False,
                from_text="ST_GeomFromEWKT",
                name="geometry",
            ),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("code", name=op.f("pk_api__epcis")),
    )
    op.create_index(
        "ix_api__epcis__geography",
        "api__epcis",
        [sa.text("CAST(ST_Simplify(geom, 0.01) AS geography(geometry, 4326))")],
        unique=False,
    )
    op.create_table(
        "api__regions",
        sa.Column("code", sa.String(), nullable=False),
        sa.Column("nom", sa.String(), nullable=False),
        sa.Column(
            "geom",
            geoalchemy2.types.Geometry(
                srid=4326,
                spatial_index=False,
                from_text="ST_GeomFromEWKT",
                name="geometry",
            ),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("code", name=op.f("pk_api__regions")),
    )
    op.create_index(
        "ix_api__regions__geography",
        "api__regions",
        [sa.text("CAST(ST_Simplify(geom, 0.01) AS geography(geometry, 4326))")],
        unique=False,
    )
    op.create_table(
        "api__requests",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("method", sa.String(), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("base_url", sa.String(), nullable=False),
        sa.Column("user", sa.String(), nullable=True),
        sa.Column(
            "path_params", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column(
            "query_params", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column("client_host", sa.String(), nullable=True),
        sa.Column("client_port", sa.Integer(), nullable=True),
        sa.Column("endpoint_name", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__requests")),
    )
    op.create_table(
        "api__structures",
        sa.Column("_di_surrogate_id", sa.String(), nullable=False),
        sa.Column("_di_geocodage_code_insee", sa.String(), nullable=True),
        sa.Column("_di_geocodage_score", sa.Float(), nullable=True),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("siret", sa.String(), nullable=True),
        sa.Column("rna", sa.String(), nullable=True),
        sa.Column("nom", sa.String(), nullable=False),
        sa.Column("commune", sa.String(), nullable=True),
        sa.Column("code_postal", sa.String(), nullable=True),
        sa.Column("code_insee", sa.String(), nullable=True),
        sa.Column("adresse", sa.String(), nullable=True),
        sa.Column("complement_adresse", sa.String(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("typologie", sa.String(), nullable=True),
        sa.Column("telephone", sa.String(), nullable=True),
        sa.Column("courriel", sa.String(), nullable=True),
        sa.Column("site_web", sa.String(), nullable=True),
        sa.Column("presentation_resume", sa.String(), nullable=True),
        sa.Column("presentation_detail", sa.String(), nullable=True),
        sa.Column("date_maj", sa.Date(), nullable=True),
        sa.Column("antenne", sa.Boolean(), nullable=True),
        sa.Column("lien_source", sa.String(), nullable=True),
        sa.Column("horaires_ouverture", sa.String(), nullable=True),
        sa.Column("accessibilite", sa.String(), nullable=True),
        sa.Column("labels_nationaux", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("labels_autres", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("thematiques", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("_di_surrogate_id", name=op.f("pk_api__structures")),
    )
    op.create_index(
        op.f("ix_api__structures__source"), "api__structures", ["source"], unique=False
    )
    op.create_table(
        "api__services",
        sa.Column("_di_surrogate_id", sa.String(), nullable=False),
        sa.Column("_di_structure_surrogate_id", sa.String(), nullable=False),
        sa.Column("_di_geocodage_code_insee", sa.String(), nullable=True),
        sa.Column("_di_geocodage_score", sa.Float(), nullable=True),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("structure_id", sa.String(), nullable=False),
        sa.Column("nom", sa.String(), nullable=False),
        sa.Column("presentation_resume", sa.String(), nullable=True),
        sa.Column("presentation_detail", sa.String(), nullable=True),
        sa.Column("types", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("thematiques", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("prise_rdv", sa.String(), nullable=True),
        sa.Column("frais", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("frais_autres", sa.String(), nullable=True),
        sa.Column("profils", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("pre_requis", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("cumulable", sa.Boolean(), nullable=True),
        sa.Column("justificatifs", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("formulaire_en_ligne", sa.String(), nullable=True),
        sa.Column("commune", sa.String(), nullable=True),
        sa.Column("code_postal", sa.String(), nullable=True),
        sa.Column("code_insee", sa.String(), nullable=True),
        sa.Column("adresse", sa.String(), nullable=True),
        sa.Column("complement_adresse", sa.String(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("recurrence", sa.String(), nullable=True),
        sa.Column("date_creation", sa.Date(), nullable=True),
        sa.Column("date_suspension", sa.Date(), nullable=True),
        sa.Column("lien_source", sa.String(), nullable=True),
        sa.Column("telephone", sa.String(), nullable=True),
        sa.Column("courriel", sa.String(), nullable=True),
        sa.Column("contact_public", sa.Boolean(), nullable=True),
        sa.Column("contact_nom_prenom", sa.String(), nullable=True),
        sa.Column("date_maj", sa.Date(), nullable=True),
        sa.Column("modes_accueil", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column(
            "modes_orientation_accompagnateur",
            postgresql.ARRAY(sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "modes_orientation_accompagnateur_autres", sa.String(), nullable=True
        ),
        sa.Column(
            "modes_orientation_beneficiaire", postgresql.ARRAY(sa.Text()), nullable=True
        ),
        sa.Column("modes_orientation_beneficiaire_autres", sa.String(), nullable=True),
        sa.Column("zone_diffusion_type", sa.String(), nullable=True),
        sa.Column("zone_diffusion_code", sa.String(), nullable=True),
        sa.Column("zone_diffusion_nom", sa.String(), nullable=True),
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
            sa.text(
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
        op.f("ix_api__services__source"), "api__services", ["source"], unique=False
    )
    op.create_index(
        op.f("ix_api__services__thematiques"),
        "api__services",
        ["thematiques"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_api__services__thematiques"),
        table_name="api__services",
        postgresql_using="gin",
    )
    op.drop_index(op.f("ix_api__services__source"), table_name="api__services")
    op.drop_index(
        op.f("ix_api__services__modes_accueil"),
        table_name="api__services",
        postgresql_using="gin",
    )
    op.drop_index(
        "ix_api__services__geography",
        table_name="api__services",
        postgresql_using="gist",
    )
    op.drop_index(
        op.f("ix_api__services___di_structure_surrogate_id"), table_name="api__services"
    )
    op.drop_table("api__services")
    op.drop_index(op.f("ix_api__structures__source"), table_name="api__structures")
    op.drop_table("api__structures")
    op.drop_table("api__requests")
    op.drop_index("ix_api__regions__geography", table_name="api__regions")
    op.drop_table("api__regions")
    op.drop_index("ix_api__epcis__geography", table_name="api__epcis")
    op.drop_table("api__epcis")
    op.drop_index("ix_api__departements__geography", table_name="api__departements")
    op.drop_table("api__departements")
    op.drop_index("ix_api__communes__geography", table_name="api__communes")
    op.drop_table("api__communes")
