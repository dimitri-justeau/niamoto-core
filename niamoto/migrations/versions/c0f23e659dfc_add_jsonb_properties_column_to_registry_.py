"""Add JSONB properties column to registry tables

Revision ID: c0f23e659dfc
Revises: c017cc9221b0
Create Date: 2017-07-28 12:41:55.086477

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'c0f23e659dfc'
down_revision = 'c017cc9221b0'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'dimension_registry',
        sa.Column(
            'properties',
            postgresql.JSONB(),
            nullable=False,
            server_default='{}',
        )
    )
    op.add_column(
        'fact_table_registry',
        sa.Column(
            'properties',
            postgresql.JSONB(),
            nullable=False,
            server_default='{}',
        )
    )
    op.add_column(
        'raster_registry',
        sa.Column(
            'properties',
            postgresql.JSONB(),
            nullable=False,
            server_default='{}',
        )
    )
    op.add_column(
        'vector_registry',
        sa.Column(
            'properties',
            postgresql.JSONB(),
            nullable=False,
            server_default='{}',
        )
    )
    op.alter_column(
        'dimension_registry',
        'properties',
        server_default=None
    )
    op.alter_column(
        'fact_table_registry',
        'properties',
        server_default=None
    )
    op.alter_column(
        'raster_registry',
        'properties',
        server_default=None
    )
    op.alter_column(
        'vector_registry',
        'properties',
        server_default=None
    )


def downgrade():
    op.drop_column('vector_registry', 'properties')
    op.drop_column('raster_registry', 'properties')
    op.drop_column('fact_table_registry', 'properties')
    op.drop_column('dimension_registry', 'properties')
