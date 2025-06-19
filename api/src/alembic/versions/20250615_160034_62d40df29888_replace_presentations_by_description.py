"""replace presentations by description

Revision ID: 62d40df29888
Revises: bc1e2107c00a
Create Date: 2025-06-15 16:00:34.523863

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "62d40df29888"
down_revision = "bc1e2107c00a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services_v1", sa.Column("description", sa.String(), nullable=True)
    )
    op.drop_column("api__services_v1", "presentation_resume")
    op.drop_column("api__services_v1", "presentation_detail")
    op.add_column(
        "api__structures_v1", sa.Column("description", sa.String(), nullable=True)
    )
    op.drop_column("api__structures_v1", "presentation_resume")
    op.drop_column("api__structures_v1", "presentation_detail")


def downgrade() -> None:
    op.add_column(
        "api__structures_v1",
        sa.Column(
            "presentation_detail", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "api__structures_v1",
        sa.Column(
            "presentation_resume", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.drop_column("api__structures_v1", "description")
    op.add_column(
        "api__services_v1",
        sa.Column(
            "presentation_detail", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "api__services_v1",
        sa.Column(
            "presentation_resume", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.drop_column("api__services_v1", "description")
