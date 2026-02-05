"""add score qualite indexes

Revision ID: add_score_qualite_idx
Revises: add_extra_services_v1
Create Date: 2026-02-05 16:38:23

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "add_score_qualite_idx"
down_revision = "add_extra_services_v1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_api__structures_v1__score_qualite",
        "api__structures_v1",
        ["score_qualite"],
    )
    op.create_index(
        "ix_api__services_v1__score_qualite",
        "api__services_v1",
        ["score_qualite"],
    )


def downgrade() -> None:
    op.drop_index("ix_api__services_v1__score_qualite", table_name="api__services_v1")
    op.drop_index(
        "ix_api__structures_v1__score_qualite", table_name="api__structures_v1"
    )
