"""change exclure_doublons to string type

Revision ID: f1e2d3c4b5a6
Revises: 05cce99385ed
Create Date: 2025-12-26 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "f1e2d3c4b5a6"
down_revision = "05cce99385ed"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "api__search_services_events_v1",
        "exclure_doublons",
        existing_type=sa.Boolean(),
        type_=sa.String(),
        existing_nullable=True,
        postgresql_using=(
            "CASE WHEN exclure_doublons = true THEN 'strict' ELSE NULL END"
        ),
    )


def downgrade() -> None:
    op.alter_column(
        "api__search_services_events_v1",
        "exclure_doublons",
        existing_type=sa.String(),
        type_=sa.Boolean(),
        existing_nullable=True,
        postgresql_using=(
            "CASE WHEN exclure_doublons = 'strict' THEN true ELSE NULL END"
        ),
    )
