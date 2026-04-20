"""drop_v0

Revision ID: 33b15080c36c
Revises: drop_first_services_search
Create Date: 2026-04-20 17:57:15.601872

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "33b15080c36c"
down_revision = "drop_first_services_search"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("api__services")
    op.drop_table("api__structures")
    op.drop_table("api__consult_service_events")
    op.drop_table("api__consult_structure_events")
    op.drop_table("api__list_services_events")
    op.drop_table("api__list_structures_events")
    op.drop_table("api__search_services_events")


def downgrade() -> None:
    pass
