"""cleanup _di_surrogate_ids

Revision ID: af22f4097dd0
Revises: d81b7c17f40b
Create Date: 2025-08-27 12:23:45.465599

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "af22f4097dd0"
down_revision = "d81b7c17f40b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(
        op.f("ix_api__services_v1___di_structure_surrogate_id"),
        table_name="api__services_v1",
    )
    op.drop_index(op.f("ix_api__services_v1__id"), table_name="api__services_v1")
    op.create_index(
        op.f("ix_api__services_v1__structure_id"),
        "api__services_v1",
        ["structure_id"],
        unique=False,
    )
    op.drop_constraint(
        op.f("fk_api__services_v1___di_structure_surrogate_id__api__s_0226"),
        "api__services_v1",
        type_="foreignkey",
    )
    op.execute(
        "UPDATE api__services_v1 SET structure_id = source || '--' || structure_id"
    )
    op.drop_column("api__services_v1", "_di_structure_surrogate_id")
    op.create_foreign_key(
        op.f("fk_api__services_v1__structure_id__api__structures_v1"),
        "api__services_v1",
        "api__structures_v1",
        ["structure_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.drop_column("api__services_v1", "_di_surrogate_id")
    op.drop_column("api__structures_v1", "_di_surrogate_id")


def downgrade() -> None:
    op.drop_constraint(
        op.f("fk_api__services_v1__structure_id__api__structures_v1"),
        "api__services_v1",
        type_="foreignkey",
    )
    # reintroduce _di_surrogate_id in structures
    op.add_column(
        "api__structures_v1",
        sa.Column("_di_surrogate_id", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.execute(
        "UPDATE api__structures_v1 "
        "SET _di_surrogate_id = source || '-' || RIGHT(id, "
        "LENGTH(id) - LENGTH(source) - 2) "
        "WHERE id LIKE source || '--%'"
    )
    op.alter_column("api__structures_v1", "_di_surrogate_id", nullable=False)
    # reintroduce _di_surrogate_id in services
    op.add_column(
        "api__services_v1",
        sa.Column("_di_surrogate_id", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.execute(
        "UPDATE api__services_v1 "
        "SET _di_surrogate_id = source || '-' || RIGHT(id, "
        "LENGTH(id) - LENGTH(source) - 2) "
        "WHERE id LIKE source || '--%'"
    )
    op.alter_column("api__services_v1", "_di_surrogate_id", nullable=False)
    # reintroduce _di_structure_surrogate_id in structures
    op.add_column(
        "api__services_v1",
        sa.Column(
            "_di_structure_surrogate_id",
            sa.VARCHAR(),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.execute(
        "UPDATE api__services_v1 "
        "SET _di_structure_surrogate_id = source || '-' || RIGHT(structure_id, "
        "LENGTH(structure_id) - LENGTH(source) - 2) "
        "WHERE structure_id LIKE source || '--%'"
    )
    op.alter_column("api__services_v1", "_di_structure_surrogate_id", nullable=False)
    # Fix structure_id to remove the source
    op.execute(
        "UPDATE api__services_v1 "
        "SET structure_id = RIGHT(structure_id, "
        "LENGTH(structure_id) - LENGTH(source) - 2) "
        "WHERE structure_id LIKE source || '--%'"
    )
    op.create_index(
        op.f("ix_api__services_v1__id"),
        "api__services_v1",
        ["_di_surrogate_id"],
        unique=True,
    )
    op.create_index(
        op.f("ix_api__structures_v1__di_surrogate_id"),
        "api__structures_v1",
        ["_di_surrogate_id"],
        unique=True,
    )
    op.create_index(
        op.f("ix_api__services_v1___di_structure_surrogate_id"),
        "api__services_v1",
        ["_di_structure_surrogate_id"],
        unique=False,
    )
    op.create_foreign_key(
        op.f("fk_api__services_v1___di_structure_surrogate_id__api__s_0226"),
        "api__services_v1",
        "api__structures_v1",
        ["_di_structure_surrogate_id"],
        ["_di_surrogate_id"],
        ondelete="CASCADE",
    )
    op.drop_index(
        op.f("ix_api__services_v1__structure_id"), table_name="api__services_v1"
    )
