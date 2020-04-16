# -*- coding: utf-8 -*-

import logging
from yoink._version import ___version___ as version

logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    level=logging.DEBUG)
LOG = logging.getLogger(__name__)

