# coding: utf-8

import logging
from logging.handlers import RotatingFileHandler
import os

import colorlog

from niamoto.conf import NIAMOTO_HOME


LOGS_DIR = os.path.join(NIAMOTO_HOME, 'logs')
LOG_FILE = os.path.join(LOGS_DIR, 'niamoto.log')


if os.path.exists(NIAMOTO_HOME):
    if not os.path.exists(LOGS_DIR) or not os.path.isdir(LOGS_DIR):
        os.mkdir(LOGS_DIR)


def update_log_path():
    global LOGS_DIR, LOG_FILE
    from niamoto.conf import NIAMOTO_HOME
    LOGS_DIR = os.path.join(NIAMOTO_HOME, 'logs')
    LOG_FILE = os.path.join(LOGS_DIR, 'niamoto.log')
    if os.path.exists(NIAMOTO_HOME):
        if not os.path.exists(LOGS_DIR) or not os.path.isdir(LOGS_DIR):
            os.mkdir(LOGS_DIR)


LOG_FORMAT = "%(asctime)s [%(levelname)s] <%(name)s> %(message)s"
COLOR_LOG_FORMAT = "%(log_color)s%(asctime)s [%(levelname)s] " \
                   "<%(name)s> %(message)s"
INFO_LOG_FORMAT = "%(log_color)s[%(levelname)s] %(message)s"
DATE_FORMAT = '%Y/%m/%d %H:%M:%S'
STREAM_LOGGING_LEVEL = logging.INFO
FILE_LOGGING_LEVEL = logging.DEBUG


class NiamotoLogFormatter(colorlog.ColoredFormatter):

    def __init__(self, fmt, date_fmt, info_fmt):
        super(NiamotoLogFormatter, self).__init__(
            fmt=fmt,
            datefmt=date_fmt,
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            },
        )
        self.default_fmt = fmt
        self.info_fmt = info_fmt

    def format(self, record):
        self._style._fmt = self.default_fmt
        if record.levelno >= logging.INFO:
            self._style._fmt = self.info_fmt
        return super(NiamotoLogFormatter, self).format(record)


def get_logger(name, colorlog_format=COLOR_LOG_FORMAT,
               info_format=INFO_LOG_FORMAT, date_format=DATE_FORMAT):
    log_file = LOG_FILE
    file_logging_level = FILE_LOGGING_LEVEL
    stream_logging_level = STREAM_LOGGING_LEVEL
    # Formatter
    formatter_stream = NiamotoLogFormatter(
        fmt=colorlog_format,
        info_fmt=info_format,
        date_fmt=date_format,
    )
    # Steam handler
    stream_handler = colorlog.StreamHandler()
    stream_handler.setFormatter(formatter_stream)
    # Set logging levels
    stream_handler.setLevel(stream_logging_level)
    # Create logger
    logger = colorlog.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if len(logger.handlers) > 0:
        return logger
    if os.path.exists(NIAMOTO_HOME):
        # File handler
        file_handler = RotatingFileHandler(log_file, 'a', 1000000, 1)
        file_handler.setFormatter(formatter_stream)
        file_handler.setLevel(file_logging_level)
        logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger
