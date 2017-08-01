# coding: utf-8

import unittest
import os

from niamoto.testing import set_test_path
set_test_path()

from niamoto.conf import settings, NIAMOTO_HOME
from niamoto.data_providers.sql_provider.sql_data_provider import \
    SQLDataProvider
from niamoto.testing.test_database_manager import TestDatabaseManager
from niamoto.testing.base_tests import BaseTestNiamotoSchemaCreated


TEST_DB_PATH = os.path.join(
    NIAMOTO_HOME,
    'data',
    'plantnote',
    'ncpippn_test.sqlite',
)


class SQLTestProvider(SQLDataProvider):

    OCCURRENCE_SQL = \
        """
        SELECT "Individus"."ID Individus" AS id,
            coalesce("Déterminations"."ID Taxons", NULL) AS taxon_id, 
            "Localités"."LongDD" AS x,
             "Localités"."LatDD" AS y,
            "Individus"."Dominance" AS strata,
            "Individus".wood_density AS wood_density,
            "Individus".leaves_sla AS leaves_sla,
            "Individus".bark_thickness AS bark_thickness,
            coalesce("Observations"."DBH", NULL) AS dbh,
            coalesce("Observations".hauteur, NULL) AS height,
            coalesce("Observations".nb_tiges, NULL) AS stem_nb,
            coalesce("Observations".statut, NULL) AS status,
            "Observations".date_observation 
        FROM "Individus" 
        LEFT OUTER JOIN "Observations" 
            ON "Individus"."ID Observations" = "Observations"."ID Observations" 
        LEFT OUTER JOIN "Déterminations"
            ON "Individus"."ID Déterminations" = "Déterminations"."ID Déterminations"
        LEFT OUTER JOIN "Inventaires" 
            ON "Individus"."ID Inventaires" = "Inventaires"."ID Inventaires" 
        JOIN "Localités" 
            ON "Inventaires"."ID Parcelle" = "Localités"."ID Localités" 
        GROUP BY "Individus"."ID Individus" 
        ORDER BY "Observations".date_observation DESC, 
            "Déterminations"."Date Détermination" DESC;
        """

    PLOT_SQL = \
        """
        SELECT "Localités"."ID Localités" AS id,
            "Localités"."Nom Entier" AS name,
            "Localités"."LongDD" AS x,
            "Localités"."LatDD" AS y,
            "Localités"."Largeur" AS width,
            "Localités"."Longueur" AS height 
        FROM "Localités";
        """

    PLOT_OCCURRENCE_SQL = \
        """
        SELECT "Inventaires"."ID Parcelle" AS plot_id,
            "Inventaires"."ID Individus" AS occurrence_id,
            "Inventaires"."Identifiant" AS occurrence_identifier 
        FROM "Inventaires";
        """

    def __init__(self, name, db_url):
        super(SQLTestProvider, self).__init__(
            name, db_url,
            self.OCCURRENCE_SQL,
            self.PLOT_SQL,
            self.PLOT_OCCURRENCE_SQL
        )

    @classmethod
    def get_type_name(cls):
        return "TEST_SQL"


class TestSQLDataProvider(BaseTestNiamotoSchemaCreated):
    """
    Test case for sql data provider.
    """

    def test_plantnote_data_provider(self):
        db_url = "sqlite:///{}".format(TEST_DB_PATH)
        SQLTestProvider.register_data_provider(
            'sql_provider',
            db_url,
        )
        test_data_provider = SQLTestProvider(
            'sql_provider',
            db_url,
        )
        self.assertIsNotNone(test_data_provider)
        self.assertIsNotNone(test_data_provider.db_id)
        # Test sync
        test_data_provider.sync()


if __name__ == '__main__':
    TestDatabaseManager.setup_test_database()
    TestDatabaseManager.create_schema(settings.NIAMOTO_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_RASTER_SCHEMA)
    TestDatabaseManager.create_schema(settings.NIAMOTO_VECTOR_SCHEMA)
    unittest.main(exit=False)
    TestDatabaseManager.teardown_test_database()

