"""Add fields volume_horaire_hebdomadaire and nombre_semaines

Revision ID: 17b9e9172d02
Revises: 8fb6fcb65868
Create Date: 2025-05-22 11:51:48.858037

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "17b9e9172d02"
down_revision = "8fb6fcb65868"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services",
        sa.Column("nombre_semaines", sa.INTEGER(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "api__services",
        sa.Column(
            "volume_horaire_hebdomadaire",
            sa.INTEGER(),
            autoincrement=False,
            nullable=True,
        ),
    )

    # ### end Alembic commands ###


def downgrade() -> None:
    op.drop_column("api__services", "volume_horaire_hebdomadaire")
    op.drop_column("api__services", "nombre_semaines")
