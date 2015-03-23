"""
Mover classes for conductor
"""

import os
import re
import logging
from socket import gethostname

import ftputil
import ftputil.error

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

    def _prepare_find(self, file_pattern):
        if file_pattern.startswith("/"):
            full_pattern = file_pattern
        else:
            full_pattern = os.path.join(self.data_dir, file_pattern)
        return os.path.split(full_pattern)


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
        dirname, name_pattern = self._prepare_find(file_pattern)
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


class FtpMover(RemoteMover):

    def __init__(self, name, server, username, password, *args, **kwargs):
        super(FtpMover, self).__init__(name=name, **kwargs)
        self.server = server
        self.username = username
        self.password = password
        self._connect()

    def find(self, file_pattern):
        found = []
        try:
            dirname, name_pattern = self._prepare_find(file_pattern)
            for item in self.ftp_host.listdir(dirname):
                re_obj = re.search(name_pattern, item)
                if re_obj is not None:
                    found.append(os.path.join(dirname, item))
        except ftputil.error.TemporaryError as e:
            logger.warning("the previous connection timed out, "
                           "reconnecting...")
            self.ftp_host.close()
            self._connect()
            self.find(file_pattern)
        except ftputil.error.PermanentError as e:
            if e.errno == 550:
                logger.warning(e)
            else:
                raise
        return found

    def fetch(self, *paths):
        raise NotImplementedError

    def copy(self, origin_paths, destination_dir):
        raise NotImplementedError

    def delete(self, paths):
        raise NotImplementedError

    def ensure_connected(self):
        raise NotImplementedError

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.name)

    def _connect(self):
        self.ftp_host = ftputil.FTPHost(self.server, self.username,
                                        self.password)


class RemoteMoverFactory(object):
    """
    This class should be used when there is a need to create remote movers.

    Some remote movers use stateful connections that we can cache in order
    to save on network resources and processing time. This class has a
    caching mechanism.
    """

    FTP = "FTP"
    SFTP = "SFTP"
    CSW = "CSW"

    _protocol_map = {
        FTP: FtpMover,
        }

    _cache = dict()

    def get_mover(self, name, protocol, **protocol_parameters):
        protocol_class = self._protocol_map.get(protocol)
        if protocol_class is not None:
            cached_protocol_instances = self._cache.get(protocol)
            if cached_protocol_instances is not None:
                cached_instance = cached_protocol_instances.get(name)
                if cached_instance is None:
                    self._cache[protocol][name] = protocol_class(
                        name, **protocol_parameters)
            else:
                self._cache[protocol] = dict()
                self._cache[protocol][name] = protocol_class(
                    name, **protocol_parameters)
        else:
            raise ValueError("Invalid protocol")
        return self._cache[protocol][name]


