"""add-fk-structure-commune

Revision ID: 9f9a66546e3a
Revises: 170af30febde
Create Date: 2024-05-27 13:06:09.931428

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "9f9a66546e3a"
down_revision = "170af30febde"
branch_labels = None
depends_on = None


def upgrade() -> None:
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
