"""add original_id column

Revision ID: dc9cbc18552e
Revises: e92c9ae2d062
Create Date: 2025-07-22 16:39:14.838357

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "dc9cbc18552e"
down_revision = "e92c9ae2d062"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("api__structures_v1", "id", new_column_name="original_id")
    op.alter_column("api__services_v1", "id", new_column_name="original_id")

    op.add_column("api__structures_v1", sa.Column("id", sa.String(), nullable=True))
    op.execute("UPDATE api__structures_v1 SET id = source || '--' || original_id")
    op.alter_column("api__structures_v1", sa.Column("id", nullable=False))

    op.add_column("api__services_v1", sa.Column("id", sa.String(), nullable=True))
    op.execute("UPDATE api__services_v1 SET id = source || '--' || original_id")
    op.alter_column("api__services_v1", sa.Column("id", nullable=False))


def downgrade() -> None:
    op.drop_column("api__structures_v1", "id")
    op.drop_column("api__services_v1", "id")
    op.alter_column("api__structures_v1", "original_id", new_column_name="id")
    op.alter_column("api__services_v1", "original_id", new_column_name="id")
