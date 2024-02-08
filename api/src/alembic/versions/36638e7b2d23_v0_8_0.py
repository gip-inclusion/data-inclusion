"""v0.8.0

Revision ID: 36638e7b2d23
Revises: c001f354838f
Create Date: 2023-03-08 12:03:16.895480

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "36638e7b2d23"
down_revision = "c001f354838f"
branch_labels = None
depends_on = None


def column_exists(table_name, column_name):
    bind = op.get_context().bind
    insp = sa.inspect(bind)
    columns = insp.get_columns(table_name)
    return any(c["name"] == column_name for c in columns)


def upgrade() -> None:
    op.add_column("service", sa.Column("presentation_detail", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("pre_requis", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("cumulable", sa.Boolean(), nullable=True))
    op.add_column("service", sa.Column("justificatifs", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("formulaire_en_ligne", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("commune", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("code_postal", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("code_insee", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("adresse", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("complement_adresse", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("longitude", sa.Float(), nullable=True))
    op.add_column("service", sa.Column("latitude", sa.Float(), nullable=True))
    op.add_column("service", sa.Column("recurrence", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("date_creation", sa.Date(), nullable=True))
    op.add_column("service", sa.Column("lien_source", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("telephone", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("courriel", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("contact_public", sa.Boolean(), nullable=True))
    op.add_column("service", sa.Column("date_maj", sa.Date(), nullable=True))
    op.add_column(
        "service",
        sa.Column("modes_accueil", postgresql.ARRAY(sa.Text()), nullable=True),
    )

    # these columns might have already been created by dbt
    if not column_exists("service", "date_suspension"):
        op.add_column("service", sa.Column("date_suspension", sa.Date(), nullable=True))
    if not column_exists("service", "zone_diffusion_type"):
        op.add_column(
            "service",
            sa.Column("zone_diffusion_type", sa.Text(), nullable=True),
        )
    if not column_exists("service", "zone_diffusion_code"):
        op.add_column(
            "service",
            sa.Column("zone_diffusion_code", sa.Text(), nullable=True),
        )
    if not column_exists("service", "zone_diffusion_nom"):
        op.add_column(
            "service",
            sa.Column("zone_diffusion_nom", sa.Text(), nullable=True),
        )


def downgrade() -> None:
    op.drop_column("service", "zone_diffusion_nom")
    op.drop_column("service", "zone_diffusion_code")
    op.drop_column("service", "zone_diffusion_type")
    op.drop_column("service", "modes_accueil")
    op.drop_column("service", "date_maj")
    op.drop_column("service", "contact_public")
    op.drop_column("service", "courriel")
    op.drop_column("service", "telephone")
    op.drop_column("service", "lien_source")
    op.drop_column("service", "date_suspension")
    op.drop_column("service", "date_creation")
    op.drop_column("service", "recurrence")
    op.drop_column("service", "latitude")
    op.drop_column("service", "longitude")
    op.drop_column("service", "complement_adresse")
    op.drop_column("service", "adresse")
    op.drop_column("service", "code_insee")
    op.drop_column("service", "code_postal")
    op.drop_column("service", "commune")
    op.drop_column("service", "formulaire_en_ligne")
    op.drop_column("service", "justificatifs")
    op.drop_column("service", "cumulable")
    op.drop_column("service", "pre_requis")
    op.drop_column("service", "presentation_detail")
