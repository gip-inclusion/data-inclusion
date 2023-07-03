"""extended_request

Revision ID: 2daaedf28c12
Revises: ca282a8389ef
Create Date: 2023-07-01 16:11:01.969832

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "2daaedf28c12"
down_revision = "ca282a8389ef"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("api_request", sa.Column("base_url", sa.Text(), nullable=True))
    op.add_column(
        "api_request",
        sa.Column(
            "path_params", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
    )
    op.add_column(
        "api_request",
        sa.Column(
            "query_params", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
    )
    op.add_column("api_request", sa.Column("client_host", sa.Text(), nullable=True))
    op.add_column("api_request", sa.Column("client_port", sa.Integer(), nullable=True))
    op.add_column("api_request", sa.Column("endpoint_name", sa.Text(), nullable=True))


def downgrade() -> None:
    pass
