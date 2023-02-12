"""rename_di_fields

Revision ID: c001f354838f
Revises: aa35af49e9df
Create Date: 2023-02-12 12:31:30.617608

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "c001f354838f"
down_revision = "aa35af49e9df"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("structure", "surrogate_id", new_column_name="_di_surrogate_id")
    op.alter_column(
        "service",
        "structure_surrogate_id",
        new_column_name="_di_structure_surrogate_id",
    )
    op.alter_column("service", "surrogate_id", new_column_name="_di_surrogate_id")
    op.create_foreign_key(
        "services_structure_surrogate_id_fk",
        "service",
        "structure",
        ["_di_structure_surrogate_id"],
        ["_di_surrogate_id"],
    )
    op.alter_column(
        "structure", "geocodage_code_insee", new_column_name="_di_geocodage_code_insee"
    )
    op.alter_column(
        "structure", "geocodage_score", new_column_name="_di_geocodage_score"
    )


def downgrade() -> None:
    pass
