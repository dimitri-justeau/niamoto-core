# coding: utf-8


class BaseOccurrenceProvider:
    """
    Abstract base class for occurrence provider.
    """

    def __init__(self, db_id):
        """
        :param db_id: The database id of the corresponding data
        provider record.
        """
        self.db_id = db_id

    def get_current_occurrence_data(self):
        """
        :return: A DataFrame containing the current database occurrence data
        for this provider.
        """
        pass
