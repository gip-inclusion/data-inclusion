"""commune

Revision ID: 7f177bfb0108
Revises: 2daaedf28c12
Create Date: 2023-05-24 18:27:45.820006

"""
import geoalchemy2
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "7f177bfb0108"
down_revision = "2daaedf28c12"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "admin_express_communes",
        sa.Column("code", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("departement", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("siren_epci", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("nom", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("region", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("geom", geoalchemy2.Geometry("Geometry", srid=4326)),
        sa.PrimaryKeyConstraint("code"),
    )
    op.create_table(
        "admin_express_epcis",
        sa.Column("code", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("nom", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("nature", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("geom", geoalchemy2.Geometry("Geometry", srid=4326)),
        sa.PrimaryKeyConstraint("code"),
    )
    op.create_table(
        "admin_express_departements",
        sa.Column("code", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("nom", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("insee_reg", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("geom", geoalchemy2.Geometry("Geometry", srid=4326)),
        sa.PrimaryKeyConstraint("code"),
    )
    op.create_table(
        "admin_express_regions",
        sa.Column("code", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("nom", sa.TEXT(), autoincrement=False, nullable=True),
        sa.Column("geom", geoalchemy2.Geometry("Geometry", srid=4326)),
        sa.PrimaryKeyConstraint("code"),
    )


def downgrade() -> None:
    pass
