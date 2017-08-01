"""Remove provider type table

Revision ID: 6ce4db1c6760
Revises: c0f23e659dfc
Create Date: 2017-08-01 12:37:15.867730

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6ce4db1c6760'
down_revision = 'c0f23e659dfc'
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    connection.execute("DELETE FROM niamoto.data_provider;")
    op.drop_index('ix_data_provider_niamoto_data_provider_provider_type_id', table_name='data_provider')
    op.drop_constraint('data_provider_provider_type_id_fkey', 'data_provider', type_='foreignkey')
    op.drop_column('data_provider', 'provider_type_id')
    op.add_column(
        'data_provider',
        sa.Column('provider_type_key', sa. String(100), nullable=False)
    )
    op.drop_table('data_provider_type')


def downgrade():
    op.create_foreign_key('occurrence_provider_id_fkey', 'occurrence', 'data_provider', ['provider_id'], ['id'], onupdate='CASCADE', ondelete='CASCADE')
    op.add_column('data_provider', sa.Column('provider_type_id', sa.INTEGER(), autoincrement=False, nullable=False))
    op.create_foreign_key('data_provider_provider_type_id_fkey', 'data_provider', 'data_provider_type', ['provider_type_id'], ['id'])
    op.create_index('ix_data_provider_niamoto_data_provider_provider_type_id', 'data_provider', ['provider_type_id'], unique=False)
    op.drop_column('data_provider', 'provider_type_key')
    op.create_table(
        'data_provider_type',
        sa.Column('id', sa.INTEGER(), nullable=False),
        sa.Column('name', sa.VARCHAR(length=100), autoincrement=False, nullable=False),
        sa.PrimaryKeyConstraint('id', name='data_provider_type_pkey'),
        sa.UniqueConstraint('name', name='uq_data_provider_type_name'),
        schema='niamoto'
    )
