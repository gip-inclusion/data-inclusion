"""add deduplication indexes

Revision ID: a1b2c3d4e5f6
Revises: 1f1fb64a89da
Create Date: 2025-12-11 23:59:59.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "1f1fb64a89da"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_api__structures__cluster_dedup",
        "api__structures",
        [
            "_cluster_id",
            sa.text("score_qualite DESC"),
            sa.text("date_maj DESC"),
        ],
    )
    op.create_index(
        "ix_api__structures_v1__cluster_dedup",
        "api__structures_v1",
        [
            "_cluster_id",
            sa.text("score_qualite DESC"),
            sa.text("date_maj DESC"),
        ],
    )


def downgrade() -> None:
    op.drop_index("ix_api__structures_v1__cluster_dedup", "api__structures_v1")
    op.drop_index("ix_api__structures__cluster_dedup", "api__structures")
