"""add volume horaire fields

Revision ID: bc1e2107c00a
Revises: c36ca85cf673
Create Date: 2025-06-05 10:43:45.516071

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "bc1e2107c00a"
down_revision = "c36ca85cf673"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services",
        sa.Column("volume_horaire_hebdomadaire", sa.Float(), nullable=True),
    )
    op.add_column(
        "api__services", sa.Column("nombre_semaines", sa.Integer(), nullable=True)
    )
    op.add_column(
        "api__services_v1",
        sa.Column("volume_horaire_hebdomadaire", sa.Float(), nullable=True),
    )
    op.add_column(
        "api__services_v1", sa.Column("nombre_semaines", sa.Integer(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("api__services", "nombre_semaines")
    op.drop_column("api__services", "volume_horaire_hebdomadaire")
    op.drop_column("api__services_v1", "nombre_semaines")
    op.drop_column("api__services_v1", "volume_horaire_hebdomadaire")
