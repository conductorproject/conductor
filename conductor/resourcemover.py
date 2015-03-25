"""
Mover classes for conductor
"""

import os
import re
import shutil
import logging
from socket import gethostname

import ftputil
import ftputil.error

logger = logging.getLogger("conductor.{}".format(__name__))

class ResourceMover(object):
    _data_dirs = ""

    @property
    def data_dirs(self):
        return [p.format(self) for p in self._data_dirs]

    @data_dirs.setter
    def data_dirs(self, path_patterns):
        self._data_dirs = path_patterns

    def __init__(self, name="", data_dirs=None):
        self.name = name if name != "" else gethostname()
        self.data_dirs = data_dirs if data_dirs is not None \
            else [os.path.expanduser("~")]

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.name)

    def find(self, file_pattern):
        raise NotImplementedError

    def fetch(self, destination_dir, *paths):
        raise NotImplementedError

    def copy(self, origin_paths, destination_dir):
        raise NotImplementedError

    def delete(self, paths):
        raise NotImplementedError

    def _prepare_find(self, file_pattern):
        if file_pattern.startswith("/"):
            patterns = [file_pattern]
        else:
            patterns = [os.path.join(d, file_pattern) for d in self.data_dirs]
        return [os.path.split(p) for p in patterns]

    def _create_local_directory(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)


class LocalMover(ResourceMover):

    def __init__(self, *args, **kwargs):
        super(LocalMover, self).__init__(*args, **kwargs)
        LocalMover._cached_mover = self

    def find(self, *file_patterns):
        """
        Search for the input file_patterns in the local filesystem.

        :param file_patterns: A sequence of path patterns to search for
        :type: [basestring]
        :return:
        :rtype: [basestring]
        """
        found = []
        for p in file_patterns:
            for dirname, name_pattern in self._prepare_find(p):
                try:
                    for item in os.listdir(dirname):
                        re_obj = re.search(name_pattern, item)
                        if re_obj is not None:
                            found.append(os.path.join(dirname, item))
                except OSError as e:
                    logger.warning(e)
        return found

    def fetch(self, destination_dir, *paths):
        self._create_local_directory(destination_dir)
        copied = []
        for p in paths:
            shutil.copy(p, destination_dir)
            copied.append(os.path.join(destination_dir, os.path.basename(p)))
        return copied

    def copy(self, origin_paths, destination_dir):
        raise NotImplementedError

    def delete(self, *paths):
        for p in paths:
            try:
                os.remove(p)
                dirname = os.path.dirname(p)
                os.removedirs(dirname)
            except OSError as e:
                logger.error(e)


class RemoteMover(ResourceMover):
    pass


class FtpMover(RemoteMover):

    def __init__(self, name, server, username, password, *args, **kwargs):
        super(FtpMover, self).__init__(name=name, **kwargs)
        self.server = server
        self.username = username
        self.password = password
        self.ftp_host = None

    def find(self, *file_patterns):
        found = []
        try:
            for p in file_patterns:
                for dirname, name_pattern in self._prepare_find(p):
                    logger.debug("Looking for {} {}...".format(dirname,
                                                               name_pattern))
                    for item in self.ftp_host.listdir(dirname):
                        re_obj = re.search(name_pattern, item)
                        if re_obj is not None:
                            found.append(os.path.join(dirname, item))
        except AttributeError:
            logger.info("Not connected to FTP server yet. Establishing "
                        "the first FTP connection with "
                        "{}...".format(self.server))
            self._connect()
            found = self.find(*file_patterns)
        except ftputil.error.TemporaryError as e:
            logger.info("Previous connection timed out, "
                           "Reconnecting...")
            self.ftp_host.close()
            self._connect()
            found = self.find(*file_patterns)
        except ftputil.error.PermanentError as e:
            if e.errno == 550:
                logger.warning(e)
            else:
                raise
        return found

    # TODO - Check for connection errors in a similar fashion to the find() method
    def fetch(self, destination_dir, *paths):
        self._create_local_directory(destination_dir)
        copied = []
        for p in paths:
            file_name = os.path.basename(p)
            target_path = os.path.join(destination_dir, file_name)
            self.ftp_host.download(p, target_path)
            copied.append(target_path)
        return copied

    def copy(self, origin_paths, destination_dir):
        raise NotImplementedError

    # TODO - Check for connection errors in a similar fashion to the find() method
    # TODO - Remove empty directories recursively, similarly to the LocalMover's delete() method
    # TODO - Improve error handling
    def delete(self, *paths):
        for p in paths:
            try:
                self.ftp_host.remove(p)
            except Exception as e:
                logger.error(e)

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

    def get_mover(self, name, protocol, **mover_parameters):
        protocol_class = self._protocol_map.get(protocol)
        if protocol_class is not None:
            cached_protocol_instances = self._cache.get(protocol)
            if cached_protocol_instances is not None:
                cached_instance = cached_protocol_instances.get(name)
                if cached_instance is None:
                    self._cache[protocol][name] = protocol_class(
                        name, **mover_parameters)
            else:
                self._cache[protocol] = dict()
                self._cache[protocol][name] = protocol_class(
                    name, **mover_parameters)
        else:
            raise ValueError("Invalid protocol")
        return self._cache[protocol][name]
