"""refactor mobilisation data

Revision ID: 28b91f8435e8
Revises: 62d40df29888
Create Date: 2025-06-19 22:17:48.974510

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from data_inclusion.api.core.db import SortedTextArray

# revision identifiers, used by Alembic.
revision = "28b91f8435e8"
down_revision = "62d40df29888"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services_v1",
        sa.Column("modes_mobilisation", SortedTextArray(sa.Text()), nullable=True),
    )
    op.add_column(
        "api__services_v1",
        sa.Column("mobilisation_precisions", sa.String(), nullable=True),
    )
    op.add_column(
        "api__services_v1",
        sa.Column("mobilisable_par", SortedTextArray(sa.Text()), nullable=True),
    )
    op.add_column(
        "api__services_v1", sa.Column("lien_mobilisation", sa.String(), nullable=True)
    )
    op.drop_column("api__services_v1", "modes_orientation_beneficiaire_autres")
    op.drop_column("api__services_v1", "modes_orientation_accompagnateur_autres")
    op.drop_column("api__services_v1", "formulaire_en_ligne")
    op.drop_column("api__services_v1", "modes_orientation_beneficiaire")
    op.drop_column("api__services_v1", "prise_rdv")
    op.drop_column("api__services_v1", "page_web")
    op.drop_column("api__services_v1", "modes_orientation_accompagnateur")


def downgrade() -> None:
    op.add_column(
        "api__services_v1",
        sa.Column(
            "modes_orientation_accompagnateur",
            postgresql.ARRAY(sa.TEXT()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "api__services_v1",
        sa.Column("page_web", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "api__services_v1",
        sa.Column("prise_rdv", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "api__services_v1",
        sa.Column(
            "modes_orientation_beneficiaire",
            postgresql.ARRAY(sa.TEXT()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "api__services_v1",
        sa.Column(
            "formulaire_en_ligne", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "api__services_v1",
        sa.Column(
            "modes_orientation_accompagnateur_autres",
            sa.VARCHAR(),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "api__services_v1",
        sa.Column(
            "modes_orientation_beneficiaire_autres",
            sa.VARCHAR(),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.drop_column("api__services_v1", "lien_mobilisation")
    op.drop_column("api__services_v1", "mobilisable_par")
    op.drop_column("api__services_v1", "mobilisation_precisions")
    op.drop_column("api__services_v1", "modes_mobilisation")
