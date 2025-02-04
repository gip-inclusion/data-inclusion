"""add mobilisable par

Revision ID: c55f8eccaf3f
Revises: c947102bb23f
Create Date: 2025-02-04 14:14:23.826247

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "c55f8eccaf3f"
down_revision = "c947102bb23f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services",
        sa.Column(
            "mobilisable_par",
            postgresql.ARRAY(sa.TEXT()),
            server_default=sa.text("ARRAY['professionnels'::text]"),
            autoincrement=False,
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("api__services", "mobilisable_par")
