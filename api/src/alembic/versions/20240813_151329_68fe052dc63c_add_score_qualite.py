"""add-score-qualite

Revision ID: 68fe052dc63c
Revises: e3f3dfa4ad01
Create Date: 2024-08-13 15:13:29.690054

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "68fe052dc63c"
down_revision = "e3f3dfa4ad01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "api__services", sa.Column("score_qualite", sa.Float(), nullable=True)
    )
    # set an arbitrary default score for this migration
    # the value itself is not important, it will be totally overriden
    # as soon as the data is loaded after the migration
    op.execute("UPDATE api__services SET score_qualite = 0.5")
    op.alter_column("api__services", "score_qualite", nullable=False)


def downgrade() -> None:
    op.drop_column("api__services", "score_qualite")
