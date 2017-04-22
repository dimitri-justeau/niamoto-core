# coding: utf-8

from niamoto.db.metadata import metadata


class Creator:
    """
    Class managing creation of niamoto databases.
    """

    @classmethod
    def create_niamoto_schema(cls, engine):
        metadata.create_all(engine)
