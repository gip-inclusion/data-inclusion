"""modification de la valeur de la colonne 'id' en v1

Revision ID: dc9cbc18552e
Revises: e92c9ae2d062
Create Date: 2025-07-22 16:39:14.838357

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "dc9cbc18552e"
down_revision = "e92c9ae2d062"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE api__services_v1 SET id = source || '--' || id")
    op.execute("UPDATE api__structures_v1 SET id = source || '--' || id")
    op.create_index(
        op.f("ix_api__services_v1__id"), "api__services_v1", ["id"], unique=True
    )
    op.create_index(
        op.f("ix_api__structures_v1__id"), "api__structures_v1", ["id"], unique=True
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_api__structures_v1__id"), table_name="api__structures_v1")
    op.drop_index(op.f("ix_api__services_v1__id"), table_name="api__services_v1")
    op.execute(
        "UPDATE api__services_v1 SET id = RIGHT(id, LENGTH(id) - LENGTH(source) - 2) "
        "WHERE id LIKE source || '--%'"
    )
    op.execute(
        "UPDATE api__structures_v1 SET id = RIGHT(id, LENGTH(id) - LENGTH(source) - 2) "
        "WHERE id LIKE source || '--%'"
    )
