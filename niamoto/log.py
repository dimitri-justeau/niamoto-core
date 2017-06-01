# coding: utf-8

import logging
from logging.handlers import RotatingFileHandler
import os

from niamoto.conf import NIAMOTO_HOME


LOGS_DIR = os.path.join(NIAMOTO_HOME, 'logs')
LOG_FILE = os.path.join(LOGS_DIR, 'niamoto.log')


if not os.path.exists(LOGS_DIR) or not os.path.isdir(LOGS_DIR):
    os.mkdir(LOGS_DIR)


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
DATE_FORMAT = '%Y/%m/%d %H:%M:%S'


def get_logger(name, log_format=LOG_FORMAT, date_format=DATE_FORMAT,
               log_file=LOG_FILE, file_logging_level=logging.DEBUG,
               steam_logging_level=logging.INFO):
    # Formatter
    formatter = logging.Formatter(fmt=log_format, datefmt=date_format)
    # File handler
    file_handler = RotatingFileHandler(log_file, 'a', 1000000, 1)
    file_handler.setFormatter(formatter)
    # Steam handler
    steam_handler = logging.StreamHandler()
    steam_handler.setFormatter(formatter)
    # Set logging levels
    file_handler.setLevel(file_logging_level)
    steam_handler.setLevel(steam_logging_level)
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(steam_handler)
    return logger
