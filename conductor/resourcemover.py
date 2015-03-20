"""
Mover classes for conductor
"""

import os
import re
import logging
from socket import gethostname

logger = logging.getLogger("conductor.{}".format(__name__))

class ResourceMover(object):
    _data_dir = ""

    @property
    def data_dir(self):
        return self._data_dir.format(self)

    @data_dir.setter
    def data_dir(self, pattern):
        self._data_dir = pattern

    def __init__(self, name="", data_dir=None):
        if name == "":
            name = gethostname()
        if data_dir is None:
            data_dir = os.path.expanduser("~")
        self.name = name
        self.data_dir = data_dir

    def find(self, file_pattern):
        raise NotImplementedError

    def fetch(self, *paths):
        raise NotImplementedError

    def copy(self, origin_paths, destination_dir):
        raise NotImplementedError

    def delete(self, paths):
        raise NotImplementedError


class LocalMover(ResourceMover):

    def __init__(self, *args, **kwargs):
        super(LocalMover, self).__init__(*args, **kwargs)
        LocalMover._cached_mover = self

    def find(self, file_pattern):
        """
        Search for the input file_pattern in the local filesystem.

        :param file_pattern:
        :return:
        :rtype: [str]
        """
        found = []
        if file_pattern.startswith("/"):
            full_pattern = file_pattern
        else:
            full_pattern = os.path.join(self.data_dir, file_pattern)
        dirname, name_pattern = os.path.split(full_pattern)
        try:
            for item in os.listdir(dirname):
                re_obj = re.search(name_pattern, item)
                if re_obj is not None:
                    found.append(os.path.join(dirname, item))
        except OSError as e:
            logger.warning(e)
        return found

    def fetch(self, *paths):
        raise NotImplementedError

    def copy(self, origin_paths, destination_dir):
        raise NotImplementedError

    def delete(self, paths):
        raise NotImplementedError


class RemoteMover(ResourceMover):
    pass
