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
        ForeignKey('occurrence_provider.id'),
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
    FAMILY = "FAMILY"
    GENUS = "GENUS"
    SPECIE = "SPECIE"
    INFRA = "INFRA"


taxon = Table(
    'taxon',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('full_name', Text, nullable=False, unique=True),
    Column('rank_name', Text, nullable=False),
    Column('rank', Enum(TaxonRankEnum), nullable=False),
    Column('parent_id', ForeignKey('taxon.id'), nullable=True),
    Column('lft', Integer, nullable=False),
    Column('rght', Integer, nullable=False),
    Column('tree_id', Integer, nullable=False),
    Column('level', Integer, nullable=False),
    CheckConstraint('level >= 0', name='level_gt_0'),
    CheckConstraint('lft >= 0', name='lft_gt_0'),
    CheckConstraint('rght >= 0', name='rght_gt_0'),
    CheckConstraint('tree_id >= 0', name='tree_id_gt_0'),
)

# ---------- #
# Plot table #
# ---------- #

plot = Table(
    'plot',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('provider_id', ForeignKey('plot_provider.id'), nullable=False),
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

# ---------------------------- #
#  Occurrence provider tables  #
# ---------------------------- #

occurrence_provider_type = Table(
    'occurrence_provider_type',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(50), nullable=False, unique=True),
)

occurrence_provider = Table(
    'occurrence_provider',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(100), nullable=False, unique=True),
    Column(
        'provider_type_id',
        ForeignKey('occurrence_provider_type.id'),
        nullable=False
    ),
)


# ---------------------- #
#  Plot provider tables  #
# ---------------------- #

plot_provider_type = Table(
    'plot_provider_type',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(50), nullable=False, unique=True),
)

plot_provider = Table(
    'plot_provider',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(100), nullable=False, unique=True),
    Column(
        'provider_type_id',
        ForeignKey('plot_provider_type.id'),
        nullable=False
    ),
)
