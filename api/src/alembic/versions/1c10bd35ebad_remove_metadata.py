"""remove_metadata

Revision ID: 1c10bd35ebad
Revises: 675c5aba2926
Create Date: 2023-02-11 11:12:56.583348

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "1c10bd35ebad"
down_revision = "675c5aba2926"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("structure", "created_at")
    op.drop_column("structure", "src_url")
    op.drop_column("structure", "batch_id")


def downgrade() -> None:
    op.add_column(
        "structure",
        sa.Column("batch_id", sa.TEXT(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "structure", sa.Column("src_url", sa.TEXT(), autoincrement=False, nullable=True)
    )
    op.add_column(
        "structure",
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            autoincrement=False,
            nullable=True,
        ),
    )
