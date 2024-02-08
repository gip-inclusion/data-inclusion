"""v0_9_0

Revision ID: ca282a8389ef
Revises: 749d9944cbda
Create Date: 2023-06-30 12:57:06.161184

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "ca282a8389ef"
down_revision = "749d9944cbda"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "service",
        sa.Column(
            "modes_orientation_beneficiaire",
            postgresql.ARRAY(sa.TEXT()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "service",
        sa.Column(
            "modes_orientation_accompagnateur",
            postgresql.ARRAY(sa.TEXT()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column("service", sa.Column("contact_nom_prenom", sa.TEXT(), nullable=True))


def downgrade() -> None:
    pass
