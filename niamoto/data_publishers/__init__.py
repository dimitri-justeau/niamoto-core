# coding: utf-8

import os

from niamoto.conf import NIAMOTO_HOME
from niamoto.data_publishers.occurrence_data_publisher import \
    OccurrenceDataPublisher


R_SCRIPTS_HOME = os.path.join(NIAMOTO_HOME, 'R')

if not os.path.exists(R_SCRIPTS_HOME):
    os.mkdir(R_SCRIPTS_HOME)


for file in os.listdir(R_SCRIPTS_HOME):
    if file.endswith(".R"):
        print(file[:-2])
