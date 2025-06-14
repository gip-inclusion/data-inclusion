"""remove antenne flag

Revision ID: 134cbe8e6f1a
Revises: b6045e1e59c1
Create Date: 2025-06-11 10:44:28.423913

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "134cbe8e6f1a"
down_revision = "b6045e1e59c1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("api__structures", "antenne")
    op.drop_column("api__structures_v1", "antenne")


def downgrade() -> None:
    op.add_column(
        "api__structures",
        sa.Column("antenne", sa.BOOLEAN(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "api__structures_v1",
        sa.Column("antenne", sa.BOOLEAN(), autoincrement=False, nullable=True),
    )
