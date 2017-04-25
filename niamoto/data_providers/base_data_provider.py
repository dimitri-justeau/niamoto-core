# coding: utf-8


class BaseDataProvider:
    """
    Abstract base class for plot and occurrence data providers.
    """

    def __init__(self, db_id):
        """
        :param db_id: The database id of the corresponding data
        provider record.
        """
        self.db_id = db_id

    @property
    def plot_provider(self):
        raise NotImplementedError()

    @property
    def occurrence_provider(self):
        raise NotImplementedError()
