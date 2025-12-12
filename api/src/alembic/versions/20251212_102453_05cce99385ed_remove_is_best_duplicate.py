"""remove _is_best_duplicate column

Revision ID: 05cce99385ed
Revises: 1f1fb64a89da
Create Date: 2025-12-11 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "05cce99385ed"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("api__structures", "_is_best_duplicate")
    op.drop_column("api__structures_v1", "_is_best_duplicate")


def downgrade() -> None:
    op.add_column(
        "api__structures",
        sa.Column(
            "_is_best_duplicate", sa.BOOLEAN(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "api__structures_v1",
        sa.Column(
            "_is_best_duplicate", sa.BOOLEAN(), autoincrement=False, nullable=True
        ),
    )
