"""Update dimension registry table

Revision ID: 831d427c8a18
Revises: 7f158c64c86e
Create Date: 2017-07-20 13:37:57.952120

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '831d427c8a18'
down_revision = '7f158c64c86e'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        'dimension_registry',
        'dimension_key',
        new_column_name='dimension_type_key'
    )
    op.add_column(
        'dimension_registry',
        sa.Column(
            'label_column',
            sa.String(length=100),
            nullable=False,
            server_default='label',
        )
    )
    op.alter_column(
        'dimension_registry',
        'label_column',
        server_default=None
    )


def downgrade():
    op.alter_column(
        'dimension_registry',
        'dimension_type_key',
        new_column_name='dimension_key'
    )
    op.drop_column('dimension_registry', 'label_column')
