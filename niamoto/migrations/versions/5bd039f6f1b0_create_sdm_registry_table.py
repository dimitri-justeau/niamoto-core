"""Create sdm_registry table

Revision ID: 5bd039f6f1b0
Revises: 6ce4db1c6760
Create Date: 2017-12-11 14:57:00.014681

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '5bd039f6f1b0'
down_revision = '6ce4db1c6760'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('sdm_registry',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('sdm_table_name', sa.String(length=100), nullable=False),
        sa.Column(
            'taxon_id',
            sa.Integer(),
            nullable=True,
            index=True
        ),
        sa.Column('date_create', sa.DateTime(), nullable=False),
        sa.Column('date_update', sa.DateTime(), nullable=True),
        sa.Column('properties', postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ['taxon_id'],
            ['niamoto.taxon.id'],
            ondelete='SET NULL',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('sdm_table_name', name=op.f('uq_sdm_registry_sdm_table_name')),
        sa.UniqueConstraint('taxon_id', name=op.f('uq_sdm_registry_taxon_id')),
        schema='niamoto'
    )


def downgrade():
    op.drop_table('sdm_registry', schema='niamoto')

