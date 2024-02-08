"""surrogate_ids

Revision ID: 9878b7ed8f7d
Revises: ec9086574314
Create Date: 2023-02-11 10:38:46.324187

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9878b7ed8f7d"
down_revision = "ec9086574314"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("service_structure_index_fkey", "service", type_="foreignkey")
    op.alter_column(
        "structure",
        "index",
        new_column_name="surrogate_id",
        type_=sa.Text(),
        server_default=None,
    )
    op.alter_column(
        "service",
        "index",
        new_column_name="surrogate_id",
        type_=sa.Text(),
        server_default=None,
    )
    op.alter_column(
        "service",
        "structure_index",
        new_column_name="structure_surrogate_id",
        type_=sa.Text(),
    )
    op.create_foreign_key(
        None, "service", "structure", ["structure_surrogate_id"], ["surrogate_id"]
    )


def downgrade() -> None:
    pass
