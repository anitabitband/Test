# -*- coding: utf-8 -*-

# Implementations of assorted file retrievers.

import logging

_DIRECT_COPY_PLUGIN = 'ngamsDirectCopyDppi'


class NGASFileRetriever:
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)
