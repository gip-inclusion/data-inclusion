"""remove request

Revision ID: bc02c1c6f60e
Revises: df6029fc3a1f
Create Date: 2024-12-17 10:34:25.120009

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "bc02c1c6f60e"
down_revision = "df6029fc3a1f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("api__requests")
    pass


def downgrade() -> None:
    op.create_table(
        "api__requests",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("method", sa.String(), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("base_url", sa.String(), nullable=False),
        sa.Column("user", sa.String(), nullable=True),
        sa.Column(
            "path_params", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column(
            "query_params", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column("client_host", sa.String(), nullable=True),
        sa.Column("client_port", sa.Integer(), nullable=True),
        sa.Column("endpoint_name", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api__requests")),
    )
    with op.get_context().autocommit_block():
        op.create_index(
            op.f("ix_api__requests__created_at"),
            "api__requests",
            ["created_at"],
            unique=False,
            postgresql_concurrently=True,
        )
        op.create_index(
            op.f("ix_api__requests__endpoint_name"),
            "api__requests",
            ["endpoint_name"],
            unique=False,
            postgresql_concurrently=True,
        )
        op.create_index(
            op.f("ix_api__requests__method"),
            "api__requests",
            ["method"],
            unique=False,
            postgresql_concurrently=True,
        )
        op.create_index(
            op.f("ix_api__requests__status_code"),
            "api__requests",
            ["status_code"],
            unique=False,
            postgresql_concurrently=True,
        )
        op.create_index(
            op.f("ix_api__requests__user"),
            "api__requests",
            ["user"],
            unique=False,
            postgresql_concurrently=True,
        )
