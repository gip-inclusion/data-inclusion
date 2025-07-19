"""refactor reseaux porteurs

Revision ID: e92c9ae2d062
Revises: 879314ff2c3d
Create Date: 2025-07-19 09:04:34.063012

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from data_inclusion.api.core.db import SortedTextArray

# revision identifiers, used by Alembic.
revision = "e92c9ae2d062"
down_revision = "879314ff2c3d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__structures_v1",
        sa.Column("reseaux_porteurs", SortedTextArray(sa.Text()), nullable=True),
    )
    op.drop_column("api__structures_v1", "labels_nationaux")
    op.drop_column("api__structures_v1", "labels_autres")
    op.drop_column("api__structures_v1", "typologie")
    op.add_column(
        "api__list_structures_events_v1",
        sa.Column("reseaux_porteurs", SortedTextArray(sa.Text()), nullable=True),
    )
    op.drop_column("api__list_structures_events_v1", "typologie")

    op.drop_column("api__list_structures_events_v1", "label_national")


def downgrade() -> None:
    op.add_column(
        "api__list_structures_events_v1",
        sa.Column("label_national", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.drop_column("api__list_structures_events_v1", "reseaux_porteurs")
    op.add_column(
        "api__structures_v1",
        sa.Column("typologie", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "api__structures_v1",
        sa.Column(
            "labels_autres",
            postgresql.ARRAY(sa.TEXT()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "api__structures_v1",
        sa.Column(
            "labels_nationaux",
            postgresql.ARRAY(sa.TEXT()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "api__list_structures_events_v1",
        sa.Column("typologie", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.drop_column("api__structures_v1", "reseaux_porteurs")
