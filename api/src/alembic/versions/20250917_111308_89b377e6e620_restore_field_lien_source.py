"""restore field lien_source

Revision ID: 89b377e6e620
Revises: 5427ba88af50
Create Date: 2025-09-17 11:13:08.607120

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "89b377e6e620"
down_revision = "5427ba88af50"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("api__services", sa.Column("lien_source", sa.String(), nullable=True))
    op.add_column(
        "api__services_v1", sa.Column("lien_source", sa.String(), nullable=True)
    )
    op.add_column(
        "api__structures", sa.Column("lien_source", sa.String(), nullable=True)
    )
    op.add_column(
        "api__structures_v1", sa.Column("lien_source", sa.String(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("api__structures_v1", "lien_source")
    op.drop_column("api__structures", "lien_source")
    op.drop_column("api__services_v1", "lien_source")
    op.drop_column("api__services", "lien_source")
