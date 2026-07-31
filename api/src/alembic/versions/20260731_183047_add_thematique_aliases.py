"""add thematique aliases

Revision ID: c4a8e2f91b03
Revises: 6f1b0c4a72de
Create Date: 2026-07-31 18:30:47.284193

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c4a8e2f91b03"
down_revision = "6f1b0c4a72de"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "api__thematique_aliases_v1",
        sa.Column("thematique_code", sa.String(), nullable=False),
        sa.Column("alias", sa.String(), nullable=False),
        sa.Column("lexemes", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["thematique_code"],
            ["api__thematiques_v1.value"],
            name=op.f("fk_api__thematique_aliases_v1__thematique_code"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "thematique_code",
            "alias",
            name=op.f("pk_api__thematique_aliases_v1"),
        ),
    )
    op.create_index(
        op.f("ix_api__thematique_aliases_v1__lexemes"),
        "api__thematique_aliases_v1",
        ["lexemes"],
        unique=False,
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_table("api__thematique_aliases_v1")
