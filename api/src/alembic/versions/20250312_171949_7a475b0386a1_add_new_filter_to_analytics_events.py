"""Add new filter to analytics events

Revision ID: 7a475b0386a1
Revises: 9c7df6028d95
Create Date: 2025-03-12 17:19:49.059379

"""

import sqlalchemy as sa
from alembic import op

revision = "7a475b0386a1"
down_revision = "9c7df6028d95"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__list_services_events",
        sa.Column("recherche_public", sa.String(), nullable=True),
    )
    op.add_column(
        "api__list_services_events",
        sa.Column("score_qualite_minimum", sa.Float(), nullable=True),
    )
    op.add_column(
        "api__list_structures_events",
        sa.Column("exclure_doublons", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "api__search_services_events",
        sa.Column("recherche_public", sa.String(), nullable=True),
    )
    op.add_column(
        "api__search_services_events",
        sa.Column("score_qualite_minimum", sa.Float(), nullable=True),
    )
    op.add_column(
        "api__search_services_events",
        sa.Column("exclure_doublons", sa.Boolean(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("api__search_services_events", "exclure_doublons")
    op.drop_column("api__search_services_events", "score_qualite_minimum")
    op.drop_column("api__search_services_events", "recherche_public")
    op.drop_column("api__list_structures_events", "exclure_doublons")
    op.drop_column("api__list_services_events", "score_qualite_minimum")
    op.drop_column("api__list_services_events", "recherche_public")
