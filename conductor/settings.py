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
    _processing_task_settings = dict()
    _mover_settings = dict()
    _cached_local_mover = None
    _remote_mover_factory = resourcemover.RemoteMoverFactory()

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
        the_settings = file_resource_settings.copy()
        local_mover = cls.get_mover()
        remote_movers_settings = the_settings.get("remote_movers",
                                                            [])
        try:
            del the_settings["remote_movers"]
        except KeyError:
            pass
        remote_movers = []
        for mover_settings in remote_movers_settings:
            mover = cls.get_mover(name=mover_settings["name"],
                                  protocol=mover_settings["protocol"])
            remote_movers.append(mover)
        file_resource = fileresource.FileResource(name=name, timeslot=timeslot,
                                                  local_mover=local_mover,
                                                  remote_movers=remote_movers,
                                                  **the_settings)
        return file_resource

    @classmethod
    def get_mover(cls, name=None, protocol="LOCAL"):
        local_host_name = gethostname()
        name = local_host_name if name is None else name
        mover_settings = cls._mover_settings.get(name, dict())
        if protocol.upper() == "LOCAL":
            mover = resourcemover.LocalMover(name=local_host_name,
                                             **mover_settings)
        else:
            mover = cls._remote_mover_factory.get_mover(name, protocol,
                                                        **mover_settings)
        return mover

    @classmethod
    def get_processing_task(cls, name, timeslot):
        processing_task_settings = cls._processing_task_settings.get(name,
                                                                     dict())
