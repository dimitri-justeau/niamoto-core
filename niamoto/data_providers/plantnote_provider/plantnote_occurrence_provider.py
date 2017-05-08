# coding: utf-8

from sqlalchemy import *
import pandas as pd

from niamoto.data_providers import BaseOccurrenceProvider


class PlantnoteOccurrenceProvider(BaseOccurrenceProvider):
    """
    Pl@ntnote Occurrence Provider.
    Provide occurrences from a Pl@ntnote database. The Pl@ntnote database
    must have previously been converted to a SQLite3 database.
    """

    def __init__(self, data_provider, plantnote_db_path):
        super(PlantnoteOccurrenceProvider, self).__init__(data_provider)
        self.plantnote_db_path = plantnote_db_path

    def get_provider_occurrence_dataframe(self):
        db_str = 'sqlite:///{}'.format(self.plantnote_db_path)
        engine = create_engine(db_str)
        connection = engine.connect()
        try:
            metadata = MetaData()
            metadata.reflect(engine)
            #  Needed tables
            occ_table = metadata.tables['Individus']
            obs_table = metadata.tables['Observations']
            det_table = metadata.tables['Déterminations']
            inv_table = metadata.tables['Inventaires']
            loc_table = metadata.tables['Localités']
            #  Id columns for joining
            id_occ_obs = occ_table.c["ID Observations"]
            id_obs = obs_table.c["ID Observations"]
            id_occ_det = occ_table.c["ID Déterminations"]
            id_det = det_table.c["ID Déterminations"]
            id_occ_inv = occ_table.c["ID Inventaires"]
            id_inv = inv_table.c["ID Inventaires"]
            id_inv_loc = inv_table.c["ID Parcelle"]
            id_loc = loc_table.c["ID Localités"]
            loc_col = type_coerce("POINT(", String) + \
                type_coerce(loc_table.c["LongDD"], String) + \
                type_coerce(' ', String) + \
                type_coerce(loc_table.c["LatDD"], String) + \
                type_coerce(')', String)
            sel = select([
                occ_table.c["ID Individus"].label('id'),
                func.coalesce(
                    det_table.c["ID Taxons"],
                    None
                ).label('taxon_id'),
                loc_col.label("location"),
                occ_table.c["Dominance"].label("strata"),
                occ_table.c["wood_density"].label("wood_density"),
                occ_table.c["leaves_sla"].label("leaves_sla"),
                occ_table.c["bark_thickness"].label("bark_thickness"),
                func.coalesce(obs_table.c["DBH"], None).label("dbh"),
                func.coalesce(obs_table.c["hauteur"], None).label("height"),
                func.coalesce(obs_table.c["nb_tiges"], None).label("stem_nb"),
                func.coalesce(obs_table.c["statut"], None).label("status"),
                obs_table.c["date_observation"],
            ]).select_from(
                occ_table.join(
                    obs_table,
                    id_occ_obs == id_obs
                ).join(
                    det_table,
                    id_occ_det == id_det
                ).join(
                    inv_table,
                    id_occ_inv == id_inv
                ).join(
                    loc_table,
                    id_inv_loc == id_loc
                )
            ).order_by(
                obs_table.c["date_observation"].desc(),
                det_table.c["Date Détermination"].desc(),
            ).group_by(
                occ_table.c["ID Individus"],
            )

            return pd.read_sql(sel, connection, index_col="id")
        except:
            raise
        finally:
            if connection:
                connection.close()
                engine.dispose()

