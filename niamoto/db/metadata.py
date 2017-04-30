# coding: utf-8

"""
Module describing the metadata of a Niamoto database.
"""

import enum

from sqlalchemy import *
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import *

from niamoto import settings


metadata = MetaData(
    schema=settings.NIAMOTO_SCHEMA,
    naming_convention={
        "ck": "ck_%(table_name)s_%(constraint_name)s"
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
        ForeignKey('data_provider.id'),
        nullable=False
    ),
    Column('provider_pk', Integer, nullable=False),
    Column('location', Geometry('POINT', srid=4326), nullable=False),
    Column('taxon_id', ForeignKey('taxon.id'), nullable=True),
    Column('properties', JSONB),
    UniqueConstraint('provider_id', 'provider_pk'),
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
    Column('full_name', Text, nullable=False, unique=True),
    Column('rank_name', Text, nullable=False),
    Column('rank', Enum(TaxonRankEnum), nullable=False),
    Column('parent_id', ForeignKey('taxon.id'), nullable=True),
    Column('synonyms', JSONB),
    #  MPTT (Modified Pre-order Tree Traversal) columns
    Column('mptt_left', Integer, nullable=False),
    Column('mptt_right', Integer, nullable=False),
    Column('mptt_tree_id', Integer, nullable=False),
    Column('mptt_depth', Integer, nullable=False),
    CheckConstraint('mptt_depth >= 0', name='mptt_depth_gt_0'),
    CheckConstraint('mptt_left >= 0', name='mptt_left_gt_0'),
    CheckConstraint('mptt_right >= 0', name='mptt_right_gt_0'),
    CheckConstraint('mptt_tree_id >= 0', name='mptt_tree_id_gt_0'),
)

# ---------- #
# Plot table #
# ---------- #

plot = Table(
    'plot',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('provider_id', ForeignKey('data_provider.id'), nullable=False),
    Column('provider_pk', Integer, nullable=False),
    Column('name', String(100), nullable=False, unique=True),
    Column('location', Geometry('POINT', srid=4326), nullable=False),
    Column('properties', JSONB),
    UniqueConstraint('provider_id', 'provider_pk'),
)

# --------------------------- #
#  Plot <-> Occurrence table  #
# --------------------------- #

plot_occurrence = Table(
    'plot_occurrence',
    metadata,
    Column('plot_id', ForeignKey('plot.id'), primary_key=True),
    Column('occurrence_id', ForeignKey('occurrence.id'), primary_key=True),
    Column('occurrence_identifier', String(50)),
    UniqueConstraint('plot_id', 'occurrence_identifier'),
)

# ---------------------- #
#  Data provider tables  #
# ---------------------- #

data_provider_type = Table(
    'data_provider_type',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(100), nullable=False, unique=True),
)

data_provider = Table(
    'data_provider',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(100), nullable=False, unique=True),
    Column(
        'provider_type_id',
        ForeignKey('data_provider_type.id'),
        nullable=False
    ),
    Column('properties', JSONB),
)
