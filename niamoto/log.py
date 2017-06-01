# coding: utf-8

import logging


def get_logger(name):
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(
        format=log_format,
        level=logging.DEBUG,
        datefmt='%Y/%m/%d %H:%M:%S'
    )
    logger = logging.getLogger(name)
    return logger
