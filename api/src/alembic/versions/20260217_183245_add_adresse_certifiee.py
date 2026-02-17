"""add adresse_certifiee field

Revision ID: add_adresse_certifiee
Revises: add_score_qualite_idx
Create Date: 2026-02-17 18:32:45

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "add_adresse_certifiee"
down_revision = "add_score_qualite_idx"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__structures_v1",
        sa.Column(
            "_has_valid_address",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "api__services_v1",
        sa.Column(
            "_has_valid_address",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    op.drop_column("api__services_v1", "_has_valid_address")
    op.drop_column("api__structures_v1", "_has_valid_address")
