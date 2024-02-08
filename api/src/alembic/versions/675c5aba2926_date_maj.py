"""date_maj

Revision ID: 675c5aba2926
Revises: 9878b7ed8f7d
Create Date: 2023-02-11 11:08:40.328973

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "675c5aba2926"
down_revision = "9878b7ed8f7d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("structure", "date_maj", type_=sa.Date())


def downgrade() -> None:
    pass
