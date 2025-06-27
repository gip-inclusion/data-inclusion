"""creation du champ type de service

Revision ID: 4abb26303436
Revises: 2999d568ae42
Create Date: 2025-06-25 19:50:44.910321

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "4abb26303436"
down_revision = "2999d568ae42"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("api__services_v1", sa.Column("type", sa.String(), nullable=True))
    op.drop_column("api__services_v1", "types")


def downgrade() -> None:
    op.add_column(
        "api__services_v1",
        sa.Column(
            "types", postgresql.ARRAY(sa.TEXT()), autoincrement=False, nullable=True
        ),
    )
    op.drop_column("api__services_v1", "type")
