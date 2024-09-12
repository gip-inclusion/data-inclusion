"""Modified api__communes

Revision ID: e3f3dfa4ad01
Revises: 517603187775
Create Date: 2024-08-30 17:58:54.747630

"""

import geoalchemy2
import sqlalchemy as sa
from alembic import op

from data_inclusion.api.core.db import SortedTextArray

# revision identifiers, used by Alembic.
revision = "e3f3dfa4ad01"
down_revision = "517603187775"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "api__communes", "siren_epci", existing_type=sa.VARCHAR(), nullable=True
    )
    op.add_column(
        "api__communes",
        sa.Column(
            "codes_postaux",
            SortedTextArray(sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "api__communes",
        sa.Column(
            "centre",
            geoalchemy2.types.Geometry(
                srid=4326, from_text="ST_GeomFromEWKT", name="geometry"
            ),
            nullable=True,
        ),
    )
    op.drop_index("ix_api__communes__geography", table_name="api__communes")
    op.drop_column("api__communes", "geom")


def downgrade() -> None:
    op.add_column(
        "api__communes",
        sa.Column(
            "geom",
            geoalchemy2.types.Geometry(
                srid=4326,
                spatial_index=False,
                from_text="ST_GeomFromEWKT",
                name="geometry",
                _spatial_index_reflected=True,
            ),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.create_index(
        "ix_api__communes__geography",
        "api__communes",
        [
            sa.text(
                "(st_simplify(geom, 0.01::double precision)::geography(Geometry,4326))"
            )
        ],
        unique=False,
    )
    op.drop_column("api__communes", "centre")
    op.drop_column("api__communes", "codes_postaux")
    op.alter_column(
        "api__communes", "siren_epci", existing_type=sa.VARCHAR(), nullable=False
    )
