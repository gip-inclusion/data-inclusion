"""service_publics

Revision ID: b7204394ed29
Revises: b9513e81ddc2
Create Date: 2022-10-26 12:54:36.325043

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "b7204394ed29"
down_revision = "b9513e81ddc2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "service", sa.Column("profils", postgresql.ARRAY(sa.Text()), nullable=True)
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("service", "profils")
    # ### end Alembic commands ###
