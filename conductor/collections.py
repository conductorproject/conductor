"""
Collection classes for conductor
"""

import logging

from . import errors
from .settings import settings

logger = logging.getLogger(__name__)


class CollectionFactory(object):

    @staticmethod
    def get_collection(short_name):
        try:
            s = [i for i in settings.collections if
                 i["short_name"] == short_name][0]
        except IndexError:
            raise errors.CollectionNotDefinedError(
                "collection {!r} is not defined in the "
                "settings".format(short_name)
            )
        return Collection(short_name, name=s.get("name"))


collection_factory = CollectionFactory()


class Collection(object):
    short_name = ""
    name = ""

    def __init__(self, short_name, name=None):
        self.short_name = short_name
        self.name = name if name is not None else short_name

    def __repr__(self):
        return ("{0}.{1.__class__.__name__}({1.short_name!r}, "
                "name={1.name!r})".format(__name__, self))


