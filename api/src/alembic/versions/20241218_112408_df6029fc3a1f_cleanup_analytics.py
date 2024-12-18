"""cleanup analytics

Revision ID: df6029fc3a1f
Revises: 89e1ece4f56e
Create Date: 2024-12-18 11:24:08.555320

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "df6029fc3a1f"
down_revision = "89e1ece4f56e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("api__list_structures_events", "structure_id")
    op.execute("""
        UPDATE public.api__list_services_events
            SET
                code_departement = COALESCE(query_params ->> 'code_departement', query_params ->> 'departement'),
                code_commune = COALESCE(query_params ->> 'code_commune', query_params ->> 'code_insee'),
                thematiques = COALESCE(STRING_TO_ARRAY(query_params ->> 'thematiques', ','), ARRAY[query_params ->> 'thematique'])
        FROM public.api__requests
        WHERE public.api__list_services_events.id = public.api__requests.id
    """)  # noqa: E501
    op.execute("""
        UPDATE public.api__list_structures_events
            SET
                code_departement = COALESCE(query_params ->> 'code_departement', query_params ->> 'departement'),
                code_commune = COALESCE(query_params ->> 'code_commune', query_params ->> 'code_insee')
        FROM public.api__requests
        WHERE public.api__list_structures_events.id = public.api__requests.id
    """)  # noqa: E501
    op.execute("""
        UPDATE public.api__search_services_events
            SET
                code_commune = COALESCE(query_params ->> 'code_commune', query_params ->> 'code_insee')
        FROM public.api__requests
        WHERE public.api__search_services_events.id = public.api__requests.id
    """)  # noqa: E501


def downgrade() -> None:
    op.add_column(
        "api__list_structures_events",
        sa.Column("structure_id", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
