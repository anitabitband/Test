# -*- coding: utf-8 -*-

import logging
from yoink._version import ___version___ as version

logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = ['get_yoink_logger']


def get_yoink_logger(name):
    result = logging.getLogger(name)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    result.addHandler(handler)
    result.setLevel(logging.DEBUG)
    return result
