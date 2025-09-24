"""cleanup_structures_v1

Revision ID: 1f1fb64a89da
Revises: 89b377e6e620
Create Date: 2025-10-03 11:07:56.558507

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1f1fb64a89da"
down_revision = "89b377e6e620"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__structures_v1",
        sa.Column("accessibilite_lieu", sa.String(), nullable=True),
    )
    op.drop_column("api__structures_v1", "rna")
    op.drop_column("api__structures_v1", "accessibilite")


def downgrade() -> None:
    op.add_column(
        "api__structures_v1",
        sa.Column("accessibilite", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "api__structures_v1",
        sa.Column("rna", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.drop_column("api__structures_v1", "accessibilite_lieu")
