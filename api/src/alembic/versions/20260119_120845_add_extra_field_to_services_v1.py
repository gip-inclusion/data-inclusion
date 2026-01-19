"""add extra field to services v1

Revision ID: add_extra_services_v1
Revises: 05cce99385ed
Create Date: 2026-01-19 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = "add_extra_services_v1"
down_revision = "05cce99385ed"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("api__services_v1", sa.Column("extra", JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column("api__services_v1", "extra")
