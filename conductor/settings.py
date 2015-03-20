"""
A settings module for conductor
"""

from urlparse import urlsplit
import logging
import json
from socket import gethostname

import fileresource
import resourcemover

logger = logging.getLogger(__name__)

class Settings(object):

    _package_settings = dict()
    _file_resource_settings = dict()
    _mover_settings = dict()
    _cached_local_mover = None

    @classmethod
    def get_settings(cls, uri):
        parsed_uri = urlsplit(uri)
        if parsed_uri.scheme == 'http':
            pass
        elif parsed_uri.scheme == 'file':
            cls.get_settings_from_file(parsed_uri.path)

    @classmethod
    def get_settings_from_file(cls, path):
        try:
            with open(path) as fh:
                all_settings = json.load(fh)
                cls._package_settings = all_settings.get("packages", {})
                cls._file_resource_settings = all_settings.get(
                    "file_resources", {})
                cls._mover_settings = all_settings.get("movers", {})
        except IOError as e:
            logger.error(e)

    @classmethod
    def get_file_resource(cls, name, timeslot):
        file_resource_settings = cls._file_resource_settings.get(name, dict())
        local_mover = cls.get_mover()
        file_resource = fileresource.FileResource(name=name, timeslot=timeslot,
                                                  local_mover=local_mover,
                                                  **file_resource_settings)
        return file_resource

    @classmethod
    def get_mover(cls, name=None, force_local_mover_creation=False):
        force = (name is not None) and force_local_mover_creation
        local_host_name = gethostname()
        if name is None or name == local_host_name or force:
            if cls._cached_local_mover is None or force_local_mover_creation:
                # lets create the first LocalMover
                mover_settings = cls._mover_settings.get(local_host_name,
                                                         dict())
                if len(mover_settings) > 0:
                    mover = resourcemover.LocalMover(name=local_host_name,
                                                     **mover_settings)
                    cls._cached_local_mover = mover
                else:
                    logger.warning("Creating a LocalMover that is not "
                                   "defined in the settings.")
                    mover = resourcemover.LocalMover(name=local_host_name)
            else:
                mover = cls._cached_local_mover
        else:
            mover_settings = cls._mover_settings.get(name, dict())
            if len(mover_settings) > 0:
                mover = resourcemover.RemoteMover(name=name,
                                                  **mover_settings)
            else:
                logger.warning("Creating a RemoteMover that is not "
                               "defined in the settings.")
                mover = resourcemover.RemoteMover(name=name)
        return mover
