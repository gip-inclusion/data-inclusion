"""Index requests

Revision ID: 14252f787143
Revises: 254358bdaf29
Create Date: 2024-04-09 11:06:35.996711

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "14252f787143"
down_revision = "254358bdaf29"
branch_labels = None
depends_on = None


def upgrade() -> None:
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


def downgrade() -> None:
    op.drop_index(
        op.f("ix_api__requests__user"),
        table_name="api__requests",
    )
    op.drop_index(
        op.f("ix_api__requests__status_code"),
        table_name="api__requests",
    )
    op.drop_index(
        op.f("ix_api__requests__method"),
        table_name="api__requests",
    )
    op.drop_index(
        op.f("ix_api__requests__endpoint_name"),
        table_name="api__requests",
    )
    op.drop_index(
        op.f("ix_api__requests__created_at"),
        table_name="api__requests",
    )
