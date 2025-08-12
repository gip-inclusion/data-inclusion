"""retrait de source des evenements consultation v1

Revision ID: d81b7c17f40b
Revises: dc9cbc18552e
Create Date: 2025-08-12 11:04:34.574481

"""

import sqlalchemy as sa
from alembic import op

from data_inclusion.api.config import settings

# revision identifiers, used by Alembic.
revision = "d81b7c17f40b"
down_revision = "dc9cbc18552e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("api__consult_service_events_v1", "source")
    op.drop_column("api__consult_structure_events_v1", "source")


def downgrade() -> None:
    if settings.ENV in ["dev", "test"]:
        op.add_column(
            "api__consult_service_events_v1",
            sa.Column("source", sa.Text(), nullable=True),
        )
        op.add_column(
            "api__consult_structure_events_v1",
            sa.Column("source", sa.Text(), nullable=True),
        )
    else:
        raise NotImplementedError("can't downgrade this operation")
