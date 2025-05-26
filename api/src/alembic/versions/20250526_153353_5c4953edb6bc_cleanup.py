"""cleanup

Revision ID: 5c4953edb6bc
Revises: 8fb6fcb65868
Create Date: 2025-05-26 15:33:53.188548

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "5c4953edb6bc"
down_revision = "8fb6fcb65868"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # the type of the doublons column has changed from JSON to JSONB
    op.alter_column(
        "api__structures",
        "doublons",
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "api__structures",
        "doublons",
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=True,
    )
