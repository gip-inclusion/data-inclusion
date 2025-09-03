"""refactor v1 id

Revision ID: 5427ba88af50
Revises: e92c9ae2d062
Create Date: 2025-09-03 10:48:16.530788

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5427ba88af50"
down_revision = "e92c9ae2d062"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # update events models
    op.execute("""
        UPDATE api__consult_structure_events_v1
        SET structure_id = source || '--' || structure_id
    """)
    op.execute("""
        UPDATE api__consult_service_events_v1
        SET service_id = source || '--' || service_id
    """)
    op.drop_column("api__consult_structure_events_v1", "source")
    op.drop_column("api__consult_service_events_v1", "source")

    # change primary keys, update id and remove `_di_surrogate_id` columns
    op.drop_constraint(op.f("pk_api__services_v1"), "api__services_v1")
    op.drop_constraint(
        op.f("fk_api__services_v1___di_structure_surrogate_id__api__s_0226"),
        "api__services_v1",
    )
    op.drop_constraint(op.f("pk_api__structures_v1"), "api__structures_v1")

    op.execute("UPDATE api__services_v1 SET id = source || '--' || id")
    op.execute(
        "UPDATE api__services_v1 SET structure_id = source || '--' || structure_id"
    )
    op.execute("UPDATE api__structures_v1 SET id = source || '--' || id")

    op.drop_column("api__services_v1", "_di_surrogate_id")
    op.drop_column("api__services_v1", "_di_structure_surrogate_id")
    op.drop_column("api__structures_v1", "_di_surrogate_id")

    op.create_primary_key(op.f("pk_api__services_v1"), "api__services_v1", ["id"])
    op.create_primary_key(op.f("pk_api__structures_v1"), "api__structures_v1", ["id"])
    op.create_foreign_key(
        op.f("fk_api__services_v1__structure_id__api__structures_v1"),
        "api__services_v1",
        "api__structures_v1",
        ["structure_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_index(
        op.f("ix_api__services_v1__structure_id"),
        "api__services_v1",
        ["structure_id"],
        unique=False,
    )


def downgrade() -> None:
    # update events models
    op.add_column(
        "api__consult_structure_events_v1",
        sa.Column("source", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "api__consult_service_events_v1",
        sa.Column("source", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.execute("""
        UPDATE api__consult_structure_events_v1
        SET source = SPLIT_PART(structure_id, '--', 1)
    """)
    op.execute("""
        UPDATE api__consult_service_events_v1
        SET source = SPLIT_PART(service_id, '--', 1)
    """)
    op.alter_column("api__consult_structure_events_v1", "source", nullable=False)
    op.alter_column("api__consult_service_events_v1", "source", nullable=False)
    op.execute("""
        UPDATE api__consult_structure_events_v1
        SET structure_id = SPLIT_PART(structure_id, '--', 2)
    """)
    op.execute("""
        UPDATE api__consult_service_events_v1
        SET service_id = SPLIT_PART(service_id, '--', 2)
    """)

    # change primary keys, update id and bring back `_di_surrogate_id`
    op.add_column(
        "api__services_v1",
        sa.Column("_di_surrogate_id", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.add_column(
        "api__services_v1",
        sa.Column(
            "_di_structure_surrogate_id",
            sa.VARCHAR(),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "api__structures_v1",
        sa.Column("_di_surrogate_id", sa.VARCHAR(), autoincrement=False, nullable=True),
    )

    op.drop_constraint(op.f("pk_api__services_v1"), "api__services_v1")
    op.drop_constraint(
        op.f("fk_api__services_v1__structure_id__api__structures_v1"),
        "api__services_v1",
    )
    op.drop_constraint(op.f("pk_api__structures_v1"), "api__structures_v1")
    op.drop_index(
        op.f("ix_api__services_v1__structure_id"), table_name="api__services_v1"
    )

    op.execute("""
        UPDATE api__services_v1
        SET
            _di_surrogate_id = SPLIT_PART(id, '--', 1) || '-' || SPLIT_PART(id, '--', 2),
            id = SPLIT_PART(id, '--', 2)
    """)  # noqa: E501
    op.execute("""
        UPDATE api__services_v1
        SET
            _di_structure_surrogate_id = SPLIT_PART(structure_id, '--', 1) || '-' || SPLIT_PART(structure_id, '--', 2),
            structure_id = SPLIT_PART(structure_id, '--', 2)
    """)  # noqa: E501
    op.execute("""
        UPDATE api__structures_v1
        SET
            _di_surrogate_id = SPLIT_PART(id, '--', 1) || '-' || SPLIT_PART(id, '--', 2),
            id = SPLIT_PART(id, '--', 2)
    """)  # noqa: E501
    op.alter_column("api__services_v1", "_di_surrogate_id", nullable=False)
    op.alter_column("api__services_v1", "_di_structure_surrogate_id", nullable=False)
    op.alter_column("api__structures_v1", "_di_surrogate_id", nullable=False)

    op.create_primary_key(
        op.f("pk_api__structures_v1"), "api__structures_v1", ["_di_surrogate_id"]
    )
    op.create_foreign_key(
        op.f("fk_api__services_v1___di_structure_surrogate_id__api__s_0226"),
        "api__services_v1",
        "api__structures_v1",
        ["_di_structure_surrogate_id"],
        ["_di_surrogate_id"],
        ondelete="CASCADE",
    )
    op.create_primary_key(
        op.f("pk_api__services_v1"), "api__services_v1", ["_di_surrogate_id"]
    )
