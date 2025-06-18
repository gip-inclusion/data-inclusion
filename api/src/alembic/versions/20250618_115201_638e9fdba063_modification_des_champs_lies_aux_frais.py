"""modification des champs liés aux frais

Revision ID: 638e9fdba063
Revises: de144d291914
Create Date: 2025-06-18 11:52:01.825474

"""

import sqlalchemy as sa
from alembic import op

from data_inclusion.api.core.db import SortedTextArray

# revision identifiers, used by Alembic.
revision = "638e9fdba063"
down_revision = "de144d291914"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services_v1", sa.Column("frais_precisions", sa.String(), nullable=True)
    )
    op.drop_column("api__services_v1", "frais")
    op.add_column("api__services_v1", sa.Column("frais", sa.VARCHAR(), nullable=True))
    op.drop_column("api__services_v1", "frais_autres")


def downgrade() -> None:
    op.add_column(
        "api__services_v1",
        sa.Column("frais_autres", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.drop_column("api__services_v1", "frais")
    op.add_column(
        "api__services_v1",
        sa.Column("frais", SortedTextArray(sa.TEXT()), nullable=True),
    )
    op.drop_column("api__services_v1", "frais_precisions")
