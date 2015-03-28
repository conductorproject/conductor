"""
A settings module for conductor
"""

from urlparse import urlsplit
import logging
import json
from socket import gethostname

import fileresource
import resourcemover
import processingtask
import errors
from timeslotdisplacement import STRATEGY

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
                cls._processing_task_settings = all_settings.get(
                    "processing_tasks", {})
        except IOError as e:
            logger.error(e)

    @classmethod
    def get_file_resource(cls, name, timeslot):
        try:
            file_resource_settings = cls._file_resource_settings[name]
        except KeyError:
            raise errors.InvalidSettingsError("file_resource '{}' is "
                                              "not defined in the "
                                              "settings".format(name))
        local_mover = cls.get_mover()
        the_settings = file_resource_settings.copy()
        del the_settings["search_paths"]
        file_resource = fileresource.FileResource(name=name, timeslot=timeslot,
                                                  local_mover=local_mover,
                                                  **the_settings)
        for sps in file_resource_settings["search_paths"]:
            path_pattern = sps["path"]
            remote_movers = []
            for remote_mover_settings in sps.get("remote_movers", []):
                mover_name = remote_mover_settings["name"]
                protocol = remote_mover_settings.get("protocol", "FTP")
                remote_movers.append(cls.get_mover(mover_name, protocol))
            file_resource.add_search_path(path_pattern, remote_movers)
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
        try:
            processing_task_settings = cls._processing_task_settings[name]
        except KeyError:
            raise errors.InvalidSettingsError("processing_task '{}' is "
                                              "not defined in the "
                                              "settings".format(name))
        the_settings = processing_task_settings.copy()
        for k in ("inputs", "outputs"):
            try:
                del the_settings[k]
            except KeyError:
                pass
        task = processingtask.ProcessingTask(name, timeslot,
                                             **the_settings)
        inputs_settings = processing_task_settings.get("inputs", [])
        outputs_settings = processing_task_settings.get("outputs", [])
        cls._add_task_resources(timeslot, inputs_settings, task.add_inputs)
        cls._add_task_resources(timeslot, outputs_settings, task.add_outputs)
        return task

    @classmethod
    def _add_task_resources(cls, timeslot, resources_settings,
                            task_method_callback):
        for resource in resources_settings:
            file_resource = cls.get_file_resource(resource["name"], timeslot)
            strategy_settings = resource.get("strategy")
            try:
                strategy = strategy_settings.get("name",
                                                 STRATEGY["SINGLE_ABSOLUTE"])
                strategy_params = strategy_settings.copy()
                del strategy_params["name"]
            except AttributeError:
                strategy = None
                strategy_params = None
            task_method_callback(
                file_resource, strategy=strategy,
                strategy_params=strategy_params,
                except_when=resource.get("except_when"),
                optional_when=resource.get("optional_when"),
                filtering_rules=resource.get("filtering_rules")
            )
