"""Add page_web to service model

Revision ID: 170af30febde
Revises: 14252f787143
Create Date: 2024-06-10 09:28:47.160120

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "170af30febde"
down_revision = "14252f787143"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("api__services", sa.Column("page_web", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("api__services", "page_web")
