"""remove contact_public

Revision ID: ae16d2186ce8
Revises: 1d0754e89bb2
Create Date: 2025-06-11 12:16:15.692842

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ae16d2186ce8"
down_revision = "1d0754e89bb2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("api__services", "contact_public")
    op.drop_column("api__services_v1", "contact_public")


def downgrade() -> None:
    op.add_column(
        "api__services",
        sa.Column("contact_public", sa.BOOLEAN(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "api__services_v1",
        sa.Column("contact_public", sa.BOOLEAN(), autoincrement=False, nullable=True),
    )
