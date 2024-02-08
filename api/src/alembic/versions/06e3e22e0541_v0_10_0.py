"""v0.10.0

Revision ID: 06e3e22e0541
Revises: 7f177bfb0108
Create Date: 2023-09-11 15:34:37.042108

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "06e3e22e0541"
down_revision = "7f177bfb0108"
branch_labels = None
depends_on = None


def column_exists(table_name, column_name):
    bind = op.get_context().bind
    insp = sa.inspect(bind)
    columns = insp.get_columns(table_name)
    return any(c["name"] == column_name for c in columns)


def upgrade() -> None:
    op.drop_column("service", "pre_requis")
    op.drop_column("service", "justificatifs")
    op.add_column(
        "service",
        sa.Column("pre_requis", postgresql.ARRAY(sa.Text()), nullable=True),
    )
    op.add_column(
        "service",
        sa.Column("justificatifs", postgresql.ARRAY(sa.Text()), nullable=True),
    )

    # these columns might have already been created by dbt
    if not column_exists("service", "modes_orientation_accompagnateur_autres"):
        op.add_column(
            "service",
            sa.Column(
                "modes_orientation_accompagnateur_autres", sa.Text(), nullable=True
            ),
        )
    if not column_exists("service", "modes_orientation_beneficiaire_autres"):
        op.add_column(
            "service",
            sa.Column(
                "modes_orientation_beneficiaire_autres", sa.Text(), nullable=True
            ),
        )


def downgrade() -> None:
    pass
