# coding: utf-8

from sqlalchemy import *
import pandas as pd

from niamoto.data_providers.base_plot_provider import BasePlotProvider


class PlantnotePlotProvider(BasePlotProvider):
    """
    Pl@ntnote Plot Provider.
    Provide plots from a Pl@ntnote database. The Pl@ntnote database
    must have previously been converted to a SQLite3 database.
    """

    def __init__(self, data_provider, plantnote_db_path):
        super(PlantnotePlotProvider, self).__init__(data_provider)
        self.plantnote_db_path = plantnote_db_path

    def get_provider_plot_dataframe(self):
        db_str = 'sqlite:///{}'.format(self.plantnote_db_path)
        eng = create_engine(db_str)
        connection = eng.connect()
        try:
            metadata = MetaData()
            metadata.reflect(eng)
            #  Needed tables
            loc_table = metadata.tables['Localités']
            loc_col = "SRID=4326;POINT(" + \
                type_coerce(loc_table.c["LongDD"], String) + \
                ' ' + \
                type_coerce(loc_table.c["LatDD"], String) + \
                ')'
            sel = select([
                loc_table.c["ID Localités"].label("id"),
                loc_table.c["Nom Entier"].label("name"),
                loc_col.label("location"),
                loc_table.c["Largeur"].label("width"),
                loc_table.c["Longueur"].label("height"),
            ])
            df = pd.read_sql(sel, connection, index_col="id")
            property_cols = [
                "width",
                "height",
            ]
            properties = df[property_cols].apply(
                lambda x: x.to_json(),
                axis=1
            )
            df.drop(property_cols, axis=1, inplace=True)
            df['properties'] = properties
            return df
        except:
            raise
        finally:
            if connection:
                connection.close()
                eng.dispose()
