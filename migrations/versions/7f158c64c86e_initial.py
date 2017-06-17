"""initial

Revision ID: 7f158c64c86e
Revises:
Create Date: 2017-05-31 18:14:48.012005

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

from niamoto.data_providers.plantnote_provider import PlantnoteDataProvider
from niamoto.data_providers.csv_provider import CsvDataProvider
from niamoto.taxonomy.taxonomy_manager import TaxonomyManager

# revision identifiers, used by Alembic.
revision = '7f158c64c86e'
down_revision = None
branch_labels = None
depends_on = None


taxon_rank_enum = sa.Enum(
    'REGNUM',
    'PHYLUM',
    'CLASSIS',
    'ORDO',
    'FAMILIA',
    'GENUS',
    'SPECIES',
    'INFRASPECIES',
    name='taxon_rank_enum',
)


def upgrade():
    op.create_table(
        'data_provider_type',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', name=op.f('uq_data_provider_type_name')),
        schema='niamoto'
    )
    op.create_table(
        'taxon',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('full_name', sa.Text(), nullable=False),
        sa.Column('rank_name', sa.Text(), nullable=False),
        sa.Column('rank', taxon_rank_enum, nullable=False),
        sa.Column('parent_id', sa.Integer(), nullable=True, index=True),
        sa.Column('synonyms', postgresql.JSONB(), nullable=False),
        sa.Column('mptt_left', sa.Integer(), nullable=False),
        sa.Column('mptt_right', sa.Integer(), nullable=False),
        sa.Column('mptt_tree_id', sa.Integer(), nullable=False),
        sa.Column('mptt_depth', sa.Integer(), nullable=False),
        sa.CheckConstraint(
            'mptt_depth >= 0',
            name=op.f('ck_taxon_mptt_depth_gt_0')
        ),
        sa.CheckConstraint(
            'mptt_left >= 0',
            name=op.f('ck_taxon_mptt_left_gt_0')
        ),
        sa.CheckConstraint(
            'mptt_right >= 0',
            name=op.f('ck_taxon_mptt_right_gt_0')
        ),
        sa.CheckConstraint(
            'mptt_tree_id >= 0',
            name=op.f('ck_taxon_mptt_tree_id_gt_0')
        ),
        sa.ForeignKeyConstraint(
            ['parent_id'],
            ['niamoto.taxon.id'],
            deferrable=True,
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'full_name',
            name=op.f('uq_taxon_full_name')
        ),
        schema='niamoto'
    )
    op.create_table(
        'synonym_key_registry',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('date_create', sa.DateTime(), nullable=False),
        sa.Column('date_update', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', name=op.f('uq_synonym_key_registry_name')),
        schema='niamoto'
    )
    op.create_table(
        'raster_registry',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('srid', sa.Integer(), nullable=False),
        sa.Column('date_create', sa.DateTime(), nullable=False),
        sa.Column('date_update', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', name=op.f('uq_raster_registry_name')),
        schema='niamoto_raster'
    )
    op.create_table(
        'data_provider',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column(
            'provider_type_id',
            sa.Integer(),
            nullable=False,
            index=True
        ),
        sa.Column(
            'synonym_key_id',
            sa.Integer(),
            nullable=True,
            index=True
        ),
        sa.Column('properties', postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ['provider_type_id'],
            ['niamoto.data_provider_type.id'],
        ),
        sa.ForeignKeyConstraint(
            ['synonym_key_id'],
            ['niamoto.synonym_key_registry.id'],
            ondelete='SET NULL',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', name=op.f('uq_data_provider_name')),
        schema='niamoto'
    )
    op.create_table(
        'occurrence',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('provider_id', sa.Integer(), nullable=False, index=True),
        sa.Column('provider_pk', sa.Integer(), nullable=False),
        sa.Column(
            'location',
            geoalchemy2.types.Geometry(geometry_type='POINT', srid=4326),
        ),
        sa.Column('taxon_id', sa.Integer(), nullable=True, index=True),
        sa.Column('provider_taxon_id', sa.Integer(), nullable=True),
        sa.Column('properties', postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ['provider_id'],
            ['niamoto.data_provider.id'],
            onupdate='CASCADE',
            ondelete='CASCADE',
        ),
        sa.ForeignKeyConstraint(
            ['taxon_id'],
            ['niamoto.taxon.id'],
            onupdate="CASCADE",
            ondelete="SET NULL",
            deferrable=True,
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'id',
            'provider_id',
            'provider_pk',
            name=op.f('uq_occurrence_id__provider_id__provider_pk')
        ),
        schema='niamoto'
    )
    op.create_table(
        'plot',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('provider_id', sa.Integer(), nullable=False, index=True),
        sa.Column('provider_pk', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column(
            'location',
            geoalchemy2.types.Geometry(geometry_type='POINT', srid=4326),
            nullable=False
        ),
        sa.Column('properties', postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ['provider_id'],
            ['niamoto.data_provider.id'],
            onupdate='CASCADE',
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'id',
            'provider_id',
            'provider_pk',
            name=op.f('uq_plot_id__provider_id__provider_pk')
        ),
        sa.UniqueConstraint('name', name=op.f('uq_plot_name')),
        schema='niamoto'
    )
    op.create_table(
        'plot_occurrence',
        sa.Column('plot_id', sa.Integer(), nullable=False, index=True),
        sa.Column('occurrence_id', sa.Integer(), nullable=False, index=True),
        sa.Column('provider_id', sa.Integer(), nullable=True, index=True),
        sa.Column('provider_plot_pk', sa.Integer(), nullable=True),
        sa.Column('provider_occurrence_pk', sa.Integer(), nullable=True),
        sa.Column(
            'occurrence_identifier',
            sa.String(length=50),
            nullable=True
        ),
        sa.ForeignKeyConstraint(
            ['occurrence_id', 'provider_id', 'provider_occurrence_pk'],
            [
                'niamoto.occurrence.id',
                'niamoto.occurrence.provider_id',
                'niamoto.occurrence.provider_pk'
            ],
            onupdate='CASCADE',
            ondelete='CASCADE',
        ),
        sa.ForeignKeyConstraint(
            ['plot_id', 'provider_id', 'provider_plot_pk'],
            [
                'niamoto.plot.id',
                'niamoto.plot.provider_id',
                'niamoto.plot.provider_pk'
            ],
            onupdate='CASCADE',
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('plot_id', 'occurrence_id'),
        sa.UniqueConstraint(
            'plot_id',
            'occurrence_identifier',
            name=op.f('uq_plot_occurrence_plot_id__occurrence_identifier'),
            deferrable=True,
        ),
        schema='niamoto'
    )
    connection = op.get_bind()
    PlantnoteDataProvider.register_data_provider_type(
        bind=connection,
    )
    CsvDataProvider.register_data_provider_type(
        bind=connection,
    )
    TaxonomyManager.register_synonym_key(
        'niamoto',
        bind=connection,
    )


def downgrade():
    op.drop_table('plot_occurrence', schema='niamoto')
    op.drop_table('plot', schema='niamoto')
    op.drop_table('occurrence', schema='niamoto')
    op.drop_table('data_provider', schema='niamoto')
    op.drop_table('raster_registry', schema='niamoto_raster')
    op.drop_table('taxon', schema='niamoto')
    op.drop_table('data_provider_type', schema='niamoto')
    op.drop_table('synonym_key_registry', schema='niamoto')
    taxon_rank_enum.drop(op.get_bind(), checkfirst=False)
