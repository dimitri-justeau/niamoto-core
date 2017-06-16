# coding: utf-8

"""
Module describing the metadata of a Niamoto database.
"""

import enum

from sqlalchemy import *
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import *

from niamoto.conf import settings


metadata = MetaData(
    naming_convention={
        "ix": "ix_%(table_name)s_%(column_0_label)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "uq": "uq_%(table_name)s_%(constraint_name)s",
    },
)


# ------------------ #
#  Occurrence table  #
# ------------------ #

occurrence = Table(
    'occurrence',
    metadata,
    Column('id', Integer, primary_key=True),
    Column(
        'provider_id',
        ForeignKey(
            '{}.data_provider.id'.format(settings.NIAMOTO_SCHEMA),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
    ),
    Column('provider_pk', Integer, nullable=False),
    Column('location', Geometry('POINT', srid=4326), nullable=False),
    Column(
        'taxon_id',
        ForeignKey(
            '{}.taxon.id'.format(settings.NIAMOTO_SCHEMA),
            onupdate="CASCADE",
            ondelete="SET NULL",
        ),
        nullable=True,
        index=True,
    ),
    Column('provider_taxon_id', Integer, nullable=True),
    Column('properties', JSONB, nullable=False),
    UniqueConstraint(
        'id',
        'provider_id',
        'provider_pk',
        name='id__provider_id__provider_pk'
    ),
    schema=settings.NIAMOTO_SCHEMA
)


# ------------- #
#  Taxon table  #
# ------------- #

class TaxonRankEnum(enum.Enum):
    REGNUM = "REGNUM"
    PHYLUM = "PHYLUM"
    CLASSIS = "CLASSIS"
    ORDO = "ORDO"
    FAMILIA = "FAMILIA"
    GENUS = "GENUS"
    SPECIES = "SPECIES"
    INFRASPECIES = "INFRASPECIES"


taxon = Table(
    'taxon',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('full_name', Text, nullable=False),
    Column('rank_name', Text, nullable=False),
    Column(
        'rank',
        Enum(TaxonRankEnum, name='taxon_rank_enum'),
        nullable=False
    ),
    Column(
        'parent_id',
        ForeignKey(
            '{}.taxon.id'.format(settings.NIAMOTO_SCHEMA),
            deferrable=True
        ),
        nullable=True,
        index=True,
    ),
    Column('synonyms', JSONB, nullable=False),
    #  MPTT (Modified Pre-order Tree Traversal) columns
    Column('mptt_left', Integer, nullable=False),
    Column('mptt_right', Integer, nullable=False),
    Column('mptt_tree_id', Integer, nullable=False),
    Column('mptt_depth', Integer, nullable=False),
    UniqueConstraint('full_name', name='full_name'),
    CheckConstraint('mptt_depth >= 0', name='mptt_depth_gt_0'),
    CheckConstraint('mptt_left >= 0', name='mptt_left_gt_0'),
    CheckConstraint('mptt_right >= 0', name='mptt_right_gt_0'),
    CheckConstraint('mptt_tree_id >= 0', name='mptt_tree_id_gt_0'),
    schema=settings.NIAMOTO_SCHEMA,
)


# -------------------- #
# Synonym key registry #
# -------------------- #

synonym_key_registry = Table(
    'synonym_key_registry',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(100)),
    Column('date_create', DateTime, nullable=False),
    Column('date_update', DateTime, nullable=False),
    UniqueConstraint('name', name='name'),
    schema=settings.NIAMOTO_SCHEMA,
)


# ---------- #
# Plot table #
# ---------- #

plot = Table(
    'plot',
    metadata,
    Column('id', Integer, primary_key=True),
    Column(
        'provider_id',
        ForeignKey(
            '{}.data_provider.id'.format(settings.NIAMOTO_SCHEMA),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        nullable=False,
        index=True,
    ),
    Column('provider_pk', Integer, nullable=False),
    Column('name', String(100), nullable=False),
    Column('location', Geometry('POINT', srid=4326), nullable=False),
    Column('properties', JSONB, nullable=False),
    UniqueConstraint('name', name='name'),
    UniqueConstraint(
        'id',
        'provider_id',
        'provider_pk',
        name='id__provider_id__provider_pk'
    ),
    schema=settings.NIAMOTO_SCHEMA,
)

# --------------------------- #
#  Plot <-> Occurrence table  #
# --------------------------- #

plot_occurrence = Table(
    'plot_occurrence',
    metadata,
    Column('plot_id', primary_key=True, index=True),
    Column('occurrence_id', primary_key=True, index=True),
    Column('provider_id', index=True),
    Column('provider_plot_pk'),
    Column('provider_occurrence_pk'),
    Column('occurrence_identifier', String(50)),
    UniqueConstraint(
        'plot_id',
        'occurrence_identifier',
        name='plot_id__occurrence_identifier',
        deferrable=True
    ),
    ForeignKeyConstraint(
        [
            'plot_id',
            'provider_id',
            'provider_plot_pk'
        ],
        [
            '{}.plot.id'.format(settings.NIAMOTO_SCHEMA),
            '{}.plot.provider_id'.format(settings.NIAMOTO_SCHEMA),
            '{}.plot.provider_pk'.format(settings.NIAMOTO_SCHEMA)
        ],
        onupdate="CASCADE",
        ondelete="CASCADE",
    ),
    ForeignKeyConstraint(
        [
            'occurrence_id',
            'provider_id',
            'provider_occurrence_pk'
        ],
        [
            '{}.occurrence.id'.format(settings.NIAMOTO_SCHEMA),
            '{}.occurrence.provider_id'.format(settings.NIAMOTO_SCHEMA),
            '{}.occurrence.provider_pk'.format(settings.NIAMOTO_SCHEMA)
        ],
        onupdate="CASCADE",
        ondelete="CASCADE",
    ),
    schema=settings.NIAMOTO_SCHEMA,
)

# ---------------------- #
#  Data provider tables  #
# ---------------------- #

data_provider_type = Table(
    'data_provider_type',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(100), nullable=False),
    UniqueConstraint('name', name='name'),
    schema=settings.NIAMOTO_SCHEMA,
)

data_provider = Table(
    'data_provider',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(100), nullable=False),
    Column(
        'provider_type_id',
        ForeignKey('{}.data_provider_type.id'.format(settings.NIAMOTO_SCHEMA)),
        nullable=False,
        index=True,
    ),
    Column(
        'synonym_key_id',
        ForeignKey(
            '{}.synonym_key_registry.id'.format(settings.NIAMOTO_SCHEMA),
            ondelete='SET NULL',
        ),
        nullable=True,
        index=True,
    ),
    Column('properties', JSONB, nullable=False),
    UniqueConstraint('name', name='name'),
    schema=settings.NIAMOTO_SCHEMA,
)


# ---------------------- #
#  Raster registry table #
# ---------------------- #

raster_registry = Table(
    'raster_registry',
    metadata,
    Column('name', String(100), primary_key=True),
    Column('srid', Integer, nullable=False),
    Column('date_create', DateTime, nullable=False),
    Column('date_update', DateTime, nullable=False),
    schema=settings.NIAMOTO_RASTER_SCHEMA,
)
