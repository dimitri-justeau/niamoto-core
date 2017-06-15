# coding: utf-8

import os

from niamoto.conf import NIAMOTO_HOME
from niamoto.data_publishers.occurrence_data_publisher import \
    OccurrenceDataPublisher
from niamoto.data_publishers.r_data_publisher import RDataPublisher

R_SCRIPTS_HOME = os.path.join(NIAMOTO_HOME, 'R')

if not os.path.exists(R_SCRIPTS_HOME):
    os.mkdir(R_SCRIPTS_HOME)


def create_r_publisher(file_path):
    key = "R_" + file_path[:-2]

    class RPublisher(RDataPublisher):

        def __init__(self):
            super(RPublisher, self).__init__(
                os.path.join(R_SCRIPTS_HOME, file_path)
            )

        @classmethod
        def get_key(cls):
            return key


for file in os.listdir(R_SCRIPTS_HOME):
    if file.endswith(".R"):
        create_r_publisher(file)
