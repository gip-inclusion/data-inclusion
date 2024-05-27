"""add-fk-structure-commune

Revision ID: 9f9a66546e3a
Revises: 170af30febde
Create Date: 2024-05-27 13:06:09.931428

"""

import sqlalchemy as sa
from alembic import op

from data_inclusion.api.code_officiel_geo import constants
from data_inclusion.api.code_officiel_geo.models import Commune
from data_inclusion.api.inclusion_data.models import Service, Structure

# revision identifiers, used by Alembic.
revision = "9f9a66546e3a"
down_revision = "170af30febde"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # must clean up the data before adding the foreign key
    for model in [Structure, Service]:
        # remove district codes
        for k, v in constants._DISTRICTS_BY_CITY.items():
            conn.execute(
                sa.update(model)
                .where(model.code_insee.startswith(v[0][:3]))
                .values({model.code_insee: k})
                .returning(1)
            )

        # remove invalid codes
        conn.execute(
            sa.update(model)
            .where(model.code_insee.not_in(sa.select(Commune.code)))
            .values({model.code_insee: None})
        )

    op.create_foreign_key(
        op.f("fk_api__structures__code_insee__api__communes"),
        "api__structures",
        "api__communes",
        ["code_insee"],
        ["code"],
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("fk_api__structures__code_insee__api__communes"),
        "api__structures",
        type_="foreignkey",
    )
