"""seed schema

Revision ID: c42ca88c816e
Revises: add_origin_search_events
Create Date: 2026-04-23 10:15:15.510358

"""

import sqlalchemy as sa
from alembic import op

from data_inclusion.schema import v1

# revision identifiers, used by Alembic.
revision = "c42ca88c816e"
down_revision = "add_origin_search_events"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table_name, enum_class in [
        ("api__frais_v1", v1.Frais),
        ("api__modes_accueil_v1", v1.ModeAccueil),
        ("api__modes_mobilisation_v1", v1.ModeMobilisation),
        ("api__personnes_mobilisatrices_v1", v1.PersonneMobilisatrice),
        ("api__publics_v1", v1.Public),
        ("api__reseaux_porteurs_v1", v1.ReseauPorteur),
        ("api__thematiques_v1", v1.Thematique),
        ("api__types_services_v1", v1.TypeService),
    ]:
        table = op.create_table(
            table_name,
            sa.Column("value", sa.String(), nullable=False),
            sa.Column("label", sa.String(), nullable=False),
            sa.Column("description", sa.String(), nullable=True),
            sa.PrimaryKeyConstraint("value", name=op.f(f"pk_{table_name}")),
        )

        op.bulk_insert(
            table,
            [
                {
                    "value": item.value,
                    "label": item.label,
                    "description": item.description,
                }
                for item in enum_class
            ],
        )


def downgrade() -> None:
    op.drop_table("api__frais_v1")
    op.drop_table("api__modes_accueil_v1")
    op.drop_table("api__modes_mobilisation_v1")
    op.drop_table("api__personnes_mobilisatrices_v1")
    op.drop_table("api__publics_v1")
    op.drop_table("api__reseaux_porteurs_v1")
    op.drop_table("api__thematiques_v1")
    op.drop_table("api__types_services_v1")
