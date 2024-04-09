"""Session id

Revision ID: 84d303fbb413
Revises: 14252f787143
Create Date: 2024-04-09 18:00:06.160213

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "84d303fbb413"
down_revision = "14252f787143"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("api__requests", sa.Column("session_id", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("api__requests", "session_id")
