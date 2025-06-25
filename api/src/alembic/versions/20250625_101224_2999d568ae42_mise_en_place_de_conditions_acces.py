"""mise en place de conditions_acces

Revision ID: 2999d568ae42
Revises: 6851c2ade910
Create Date: 2025-06-25 10:12:24.139284

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "2999d568ae42"
down_revision = "6851c2ade910"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services_v1", sa.Column("conditions_acces", sa.String(), nullable=True)
    )
    op.drop_column("api__services_v1", "justificatifs")
    op.drop_column("api__services_v1", "pre_requis")


def downgrade() -> None:
    op.add_column(
        "api__services_v1",
        sa.Column(
            "pre_requis",
            postgresql.ARRAY(sa.TEXT()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "api__services_v1",
        sa.Column(
            "justificatifs",
            postgresql.ARRAY(sa.TEXT()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.drop_column("api__services_v1", "conditions_acces")
