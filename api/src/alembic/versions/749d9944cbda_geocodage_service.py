"""geocodage_service

Revision ID: 749d9944cbda
Revises: 36638e7b2d23
Create Date: 2023-06-22 19:47:47.947332

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "749d9944cbda"
down_revision = "36638e7b2d23"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "service",
        sa.Column(
            "_di_geocodage_code_insee", sa.Text(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "service",
        sa.Column(
            "_di_geocodage_score", sa.Float(), autoincrement=False, nullable=True
        ),
    )


def downgrade() -> None:
    op.drop_column("service", "_di_geocodage_code_insee")
    op.drop_column("service", "_di_geocodage_score")
