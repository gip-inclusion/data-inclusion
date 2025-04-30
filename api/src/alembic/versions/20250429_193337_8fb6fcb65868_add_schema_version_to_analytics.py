"""add_schema_version_to_analytics

Revision ID: 8fb6fcb65868
Revises: 7a475b0386a1
Create Date: 2025-04-29 19:33:37.070708

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8fb6fcb65868"
down_revision = "7a475b0386a1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table_name in [
        "api__consult_service_events",
        "api__consult_structure_events",
        "api__list_services_events",
        "api__list_structures_events",
        "api__search_services_events",
    ]:
        op.add_column(
            table_name, sa.Column("schema_version", sa.String(), nullable=True)
        )
        op.execute(f"UPDATE {table_name} SET schema_version = 'v0'")
        op.alter_column(table_name, "schema_version", nullable=False)


def downgrade() -> None:
    op.drop_column("api__search_services_events", "schema_version")
    op.drop_column("api__list_structures_events", "schema_version")
    op.drop_column("api__list_services_events", "schema_version")
    op.drop_column("api__consult_structure_events", "schema_version")
    op.drop_column("api__consult_service_events", "schema_version")
