# coding: utf-8

import logging
from logging.handlers import RotatingFileHandler
import os

from niamoto.conf import NIAMOTO_HOME


LOGS_DIR = os.path.join(NIAMOTO_HOME, 'logs')
LOG_FILE = os.path.join(LOGS_DIR, 'niamoto.log')


if not os.path.exists(LOGS_DIR) or not os.path.isdir(LOGS_DIR):
    os.mkdir(LOGS_DIR)


LOG_FORMAT = "%(asctime)s [%(levelname)s] <%(name)s> %(message)s"
INFO_LOG_FORMAT = "[%(levelname)s] %(message)s"
DATE_FORMAT = '%Y/%m/%d %H:%M:%S'


class NiamotoLogFormatter(logging.Formatter):

    def __init__(self, fmt, date_fmt, info_fmt):
        super(NiamotoLogFormatter, self).__init__(fmt=fmt, datefmt=date_fmt)
        self.default_fmt = fmt
        self.info_fmt = info_fmt

    def format(self, record):
        self._style._fmt = self.default_fmt
        if record.levelno >= logging.INFO:
            self._style._fmt = self.info_fmt
        return super(NiamotoLogFormatter, self).format(record)


def get_logger(name, log_format=LOG_FORMAT, info_format=INFO_LOG_FORMAT,
               date_format=DATE_FORMAT, log_file=LOG_FILE,
               file_logging_level=logging.DEBUG,
               steam_logging_level=logging.INFO):
    # Formatter
    formatter = NiamotoLogFormatter(
        fmt=log_format,
        info_fmt=info_format,
        date_fmt=date_format
    )
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
