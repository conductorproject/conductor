"""
Collection classes for conductor
"""

import logging

logger = logging.getLogger(__name__)


class Collection(object):

    def __init__(self, short_name, name=None):
        self.short_name = short_name
        self.name = name if name is not None else short_name

    def __repr__(self):
        return ("{0}.{1.__class__.__name__}({1.short_name!r}, "
                "name={1.name!r})".format(__name__, self))


