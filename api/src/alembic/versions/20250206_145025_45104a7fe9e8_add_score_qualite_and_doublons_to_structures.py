"""Add score_qualite and doublons to structures

Revision ID: 45104a7fe9e8
Revises: a06b6f7d1cbe
Create Date: 2025-02-06 14:50:25.092515

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "45104a7fe9e8"
down_revision = "a06b6f7d1cbe"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__structures", sa.Column("score_qualite", sa.Float(), nullable=True)
    )
    op.add_column(
        "api__structures",
        sa.Column("doublons", sa.JSON(), nullable=True),
    )
    op.execute("UPDATE api__structures SET score_qualite = 0.0")
    op.alter_column("api__structures", "score_qualite", nullable=False)


def downgrade() -> None:
    op.drop_column("api__structures", "doublons")
    op.drop_column("api__structures", "score_qualite")
