# coding: utf-8

from niamoto.taxonomy.taxonomy_manager import TaxonomyManager
from niamoto.db import metadata as niamoto_db_meta
from niamoto.raster.raster_manager import RasterManager
from niamoto.log import get_logger


LOGGER = get_logger(__name__)


class SSDMManager(RasterManager):
    """
    Class managing the storage of Species Distribution Models (SDMs) and
    Stacked Species Distribution Models (SSDMs).
    """

    registry_table_meta = niamoto_db_meta.sdm_registry
    TAXON_ID_PREFIX = "species"

    @classmethod
    def get_sdm_list(cls):
        """
        :return: A pandas Dataframe containing all the stored SDMs.
        """
        return super(SSDMManager, cls).get_raster_list()

    @classmethod
    def add_sdm(cls, raster_file_path, taxon_id, tile_dimension=None,
                register=False, properties={}):
        """
        Add a sdm raster in database and register it in the sdm_registry.
        :param raster_file_path: The path to the SDM raster file.
        :param taxon_id: The id of the corresponding taxon
                (must exist in database)
        :param tile_dimension: The tile dimension (width, height), if None,
            tile dimension will be chosen automatically by PostGIS.
        :param register: Register the raster as a filesystem (out-db) raster.
            (-R option of raster2pgsql).
        :param properties: A dict of arbitrary properties.
        """
        TaxonomyManager.assert_taxon_exists_in_database(taxon_id)
        super(SSDMManager, cls).add_raster(
            raster_file_path,
            "{}_{}".format(cls.TAXON_ID_PREFIX, taxon_id),
            tile_dimension=tile_dimension,
            register=register,
            properties=properties,
            taxon_id=taxon_id,
        )

    @classmethod
    def update_sdm(cls, raster_file_path, taxon_id,
                   tile_dimension=None, register=False, properties={}):
        """
        Update an existing SDM raster.
        :param raster_file_path: The new SDM raster file path.
        :param taxon_id: The id of the taxon corresponding to the SDM.
        :param tile_dimension: The tile dimension (width, height), if None,
            tile dimension will be chosen automatically by PostGIS.
        :param register: Register the raster as a filesystem (out-db) raster.
            (-R option of raster2pgsql).
        :param properties: A dict of arbitrary properties.
        """
        super(SSDMManager, cls).update_raster(
            raster_file_path,
            "{}_{}".format(cls.TAXON_ID_PREFIX, taxon_id),
            tile_dimension=tile_dimension,
            register=register,
            properties=properties
        )

    @classmethod
    def delete_sdm(cls, taxon_id, connection=None):
        """
        Delete an existing SDM raster.
        :param taxon_id: The id of the taxon corresponding to the SDM.
        :param connection: If provided, use an existing connection.
        """
        super(SSDMManager, cls).delete_raster(
            "{}_{}".format(cls.TAXON_ID_PREFIX, taxon_id),
            connection=connection
        )