"""geocodage_fields

Revision ID: aa35af49e9df
Revises: 1985ea330ce4
Create Date: 2023-02-11 19:08:25.249576

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "aa35af49e9df"
down_revision = "1985ea330ce4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "structure", sa.Column("geocodage_code_insee", sa.Text(), nullable=True)
    )
    op.add_column("structure", sa.Column("geocodage_score", sa.Float(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("structure", "geocodage_score")
    op.drop_column("structure", "geocodage_code_insee")
    # ### end Alembic commands ###
