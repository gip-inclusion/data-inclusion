"""v0.10.0

Revision ID: 06e3e22e0541
Revises: 7f177bfb0108
Create Date: 2023-09-11 15:34:37.042108

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "06e3e22e0541"
down_revision = "7f177bfb0108"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "service",
        sa.Column("modes_orientation_accompagnateur_autres", sa.Text(), nullable=True),
    )
    op.add_column(
        "service",
        sa.Column("modes_orientation_beneficiaire_autres", sa.Text(), nullable=True),
    )
    op.drop_column("service", "pre_requis")
    op.drop_column("service", "justificatifs")
    op.add_column(
        "service",
        sa.Column("pre_requis", postgresql.ARRAY(sa.Text()), nullable=True),
    )
    op.add_column(
        "service",
        sa.Column("justificatifs", postgresql.ARRAY(sa.Text()), nullable=True),
    )


def downgrade() -> None:
    pass
