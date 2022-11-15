"""antenne_flag

Revision ID: c936c618f9e3
Revises: b7204394ed29
Create Date: 2022-11-15 15:52:12.511403

"""
import sqlalchemy as sa
from sqlalchemy import orm

from alembic import op

# revision identifiers, used by Alembic.
revision = "c936c618f9e3"
down_revision = "b7204394ed29"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("structure", sa.Column("antenne", sa.Boolean(), nullable=True))

    structure_tbl = sa.table(
        "structure",
        sa.column("id", sa.Text),
        sa.column("source", sa.Text),
        sa.column("siret", sa.Text),
        sa.column("structure_parente", sa.Text),
        sa.column("antenne", sa.Boolean),
    )

    parent_structure_tbl = orm.aliased(structure_tbl)

    op.execute(
        structure_tbl.update()
        .values(
            siret=parent_structure_tbl.c.siret,
            antenne=True,
        )
        .where(
            structure_tbl.c.structure_parente.is_not(None),
            parent_structure_tbl.c.id == structure_tbl.c.structure_parente,
            parent_structure_tbl.c.source == structure_tbl.c.source,
        )
    )

    op.drop_column("structure", "structure_parente")


def downgrade() -> None:
    op.add_column(
        "structure",
        sa.Column("structure_parente", sa.TEXT(), autoincrement=False, nullable=True),
    )

    op.drop_column("structure", "antenne")
