"""set on delete options

Revision ID: b1739bed686e
Revises: bc1e2107c00a
Create Date: 2025-06-09 15:16:06.764931

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "b1739bed686e"
down_revision = "bc1e2107c00a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "fk_api__services___di_structure_surrogate_id__api__structures",
        "api__services",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_api__services__code_insee__api__communes",
        "api__services",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_api__structures__code_insee__api__communes",
        "api__structures",
        type_="foreignkey",
    )
    op.create_foreign_key(
        op.f("fk_api__services___di_structure_surrogate_id__api__structures"),
        "api__services",
        "api__structures",
        ["_di_structure_surrogate_id"],
        ["_di_surrogate_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        op.f("fk_api__services__code_insee__api__communes"),
        "api__services",
        "api__communes",
        ["code_insee"],
        ["code"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        op.f("fk_api__structures__code_insee__api__communes"),
        "api__structures",
        "api__communes",
        ["code_insee"],
        ["code"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("fk_api__services___di_structure_surrogate_id__api__structures"),
        "api__services",
        type_="foreignkey",
    )
    op.drop_constraint(
        op.f("fk_api__services__code_insee__api__communes"),
        "api__services",
        type_="foreignkey",
    )
    op.drop_constraint(
        op.f("fk_api__structures__code_insee__api__communes"),
        "api__structures",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_api__services___di_structure_surrogate_id__api__structures",
        "api__services",
        "api__structures",
        ["_di_structure_surrogate_id"],
        ["_di_surrogate_id"],
    )
    op.create_foreign_key(
        "fk_api__services__code_insee__api__communes",
        "api__services",
        "api__communes",
        ["code_insee"],
        ["code"],
    )
    op.create_foreign_key(
        "fk_api__structures__code_insee__api__communes",
        "api__structures",
        "api__communes",
        ["code_insee"],
        ["code"],
    )
