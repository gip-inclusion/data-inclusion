"""all_services_fields

Revision ID: 1985ea330ce4
Revises: 1c10bd35ebad
Create Date: 2023-02-11 11:18:14.418652

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1985ea330ce4"
down_revision = "1c10bd35ebad"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("service", sa.Column("structure_id", sa.Text(), nullable=True))
    op.add_column("service", sa.Column("source", sa.Text(), nullable=True))


def downgrade() -> None:
    pass
