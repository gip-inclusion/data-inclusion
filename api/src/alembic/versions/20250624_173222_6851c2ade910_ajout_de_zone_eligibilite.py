"""ajout de zone_eligibilite

Revision ID: 6851c2ade910
Revises: 638e9fdba063
Create Date: 2025-06-24 17:32:22.128775

"""

import sqlalchemy as sa
from alembic import op

from data_inclusion.api.core.db import SortedTextArray

# revision identifiers, used by Alembic.
revision = "6851c2ade910"
down_revision = "638e9fdba063"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services_v1",
        sa.Column("zone_eligibilite", SortedTextArray(sa.Text()), nullable=True),
    )
    op.drop_column("api__services_v1", "zone_diffusion_nom")
    op.drop_column("api__services_v1", "zone_diffusion_type")
    op.drop_column("api__services_v1", "zone_diffusion_code")


def downgrade() -> None:
    op.add_column(
        "api__services_v1",
        sa.Column(
            "zone_diffusion_code", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "api__services_v1",
        sa.Column(
            "zone_diffusion_type", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "api__services_v1",
        sa.Column(
            "zone_diffusion_nom", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.drop_column("api__services_v1", "zone_eligibilite")
