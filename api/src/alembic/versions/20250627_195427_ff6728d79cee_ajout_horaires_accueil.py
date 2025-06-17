"""Add horaires_accueil to services and structures

Revision ID: ff6728d79cee
Revises: 4abb26303436
Create Date: 2025-06-27 19:54:27.734635

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ff6728d79cee"
down_revision = "4abb26303436"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services_v1", sa.Column("horaires_accueil", sa.String(), nullable=True)
    )
    op.drop_column("api__services_v1", "recurrence")
    op.add_column(
        "api__structures_v1", sa.Column("horaires_accueil", sa.String(), nullable=True)
    )
    op.drop_column("api__structures_v1", "horaires_ouverture")


def downgrade() -> None:
    op.add_column(
        "api__structures_v1",
        sa.Column(
            "horaires_ouverture", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.drop_column("api__structures_v1", "horaires_accueil")
    op.add_column(
        "api__services_v1",
        sa.Column("recurrence", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.drop_column("api__services_v1", "horaires_accueil")
