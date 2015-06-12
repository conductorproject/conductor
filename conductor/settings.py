"""
A settings module for conductor
"""

from urlparse import urlsplit
import logging
import json
from socket import gethostname

import fileresource
import resourcemover
import tasks
from timeslotdisplacement import STRATEGY
from taskrunmode import get_run_mode, RUN_MODE

from . import errors

logger = logging.getLogger(__name__)


class Settings(object):

    settings_source = None
    servers = []
    collections = []
    resources = []
    tasks = []

    def __init__(self):
        self.settings_source = None
        self.servers = []
        self.collections = []
        self.resources = []
        self.tasks = []

    def __repr__(self):
        return "{0}.{1.__class__.__name__}({1.settings_source!r})".format(
            __name__, self)

    def available_resources(self):
        return [i["name"] for i in self.resources]

    def available_collections(self):
        return [i["short_name"] for i in self.collections]

    def available_servers(self):
        return [i["name"] for i in self.servers]

    def available_tasks(self):
        return[i["name"] for i in self.tasks]

    def get_settings(self, url):
        parsed_url = urlsplit(url)
        if parsed_url.scheme == "file":
            self.get_settings_from_file(parsed_url.path)
            self.settings_source = url
        else:
            logger.error("unsupported url scheme: "
                         "{}".format(parsed_url.scheme))

    def get_settings_from_file(self, path):
        try:
            with open(path) as fh:
                all_settings = json.load(fh)
                self.servers = all_settings.get("servers", [])
                self.collections = all_settings.get("collections", [])
                self.resources = all_settings.get("resources", [])
                self.tasks = all_settings.get("tasks", [])
        except IOError as e:
            logger.error(e)


settings = Settings()


class OldSettings(object):

    _package_settings = dict()
    _file_resource_settings = dict()
    _processing_task_settings = dict()
    _mover_settings = dict()
    _cached_local_mover = None
    _remote_mover_factory = resourcemover.RemoteMoverFactory()

    def __init__(self):
        self._package_settings = dict()
        self._file_resource_settings = dict()
        self._processing_task_settings = dict()
        self._mover_settings = dict()
        self._scheme_settings = dict()

    def get_settings(self, uri):
        parsed_uri = urlsplit(uri)
        if parsed_uri.scheme == 'http':
            pass
        elif parsed_uri.scheme == 'file':
            self.get_settings_from_file(parsed_uri.path)

    def get_settings_from_file(self, path):
        try:
            with open(path) as fh:
                all_settings = json.load(fh)
                self._package_settings = all_settings.get("packages", {})
                self._file_resource_settings = all_settings.get(
                    "file_resources", {})
                self._mover_settings = all_settings.get("movers", {})
                self._processing_task_settings = all_settings.get(
                    "processing_tasks", {})
        except IOError as e:
            logger.error(e)

    def get_file_resource(self, name, timeslot):
        try:
            file_resource_settings = self._file_resource_settings[name]
        except KeyError:
            raise errors.InvalidSettingsError("file_resource '{}' is "
                                              "not defined in the "
                                              "settings".format(name))
        local_mover = self.get_mover()
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
                remote_movers.append(self.get_mover(mover_name, protocol))
            file_resource.add_search_path(path_pattern, remote_movers)
        return file_resource

    def get_mover(self, name=None, protocol="LOCAL"):
        local_host_name = gethostname()
        name = local_host_name if name is None else name
        mover_settings = self._mover_settings.get(name, dict())
        if protocol.upper() == "LOCAL":
            mover = resourcemover.LocalMover(name=local_host_name,
                                             **mover_settings)
        else:
            mover = self._remote_mover_factory.get_mover(name, protocol,
                                                         **mover_settings)
        return mover

    def get_processing_task(self, name, timeslot, run_mode=None):
        try:
            processing_task_settings = self._processing_task_settings[name]
        except KeyError:
            raise errors.InvalidSettingsError("processing_task '{}' is "
                                              "not defined in the "
                                              "settings".format(name))
        the_settings = processing_task_settings.copy()
        for k in ("inputs", "outputs", "run_modes"):
            try:
                del the_settings[k]
            except KeyError:
                pass
        run_modes = self._get_task_run_modes(
            processing_task_settings.get("run_modes", dict()))
        task = tasks.Task(name, timeslot,
                                             creation_mode=run_modes[0],
                                             deletion_mode=run_modes[1],
                                             archiving_mode=run_modes[2],
                                             active_mode=run_mode,
                                             **the_settings)
        inputs_settings = processing_task_settings.get("inputs", [])
        outputs_settings = processing_task_settings.get("outputs", [])

        self._add_task_resources(timeslot, inputs_settings, task.add_inputs)
        self._add_task_resources(timeslot, outputs_settings, task.add_outputs)
        return task

    def _add_task_resources(self, timeslot, resources_settings,
                            task_method_callback):
        for resource in resources_settings:
            file_resource = self.get_file_resource(resource["name"], timeslot)
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
                filtering_rules=resource.get("filtering_rules"),
                copy_to_working_dir=resource.get("copy_to_working_dir", True)
            )

    def _get_task_run_modes(self, run_modes_settings):
        creation_settings = run_modes_settings.get(
            RUN_MODE.CREATION_MODE.name, dict())
        creation_execution_code = creation_settings.get("execution_code")
        creation_params = creation_settings.get("parameters", dict())
        creation_mode = get_run_mode(RUN_MODE.CREATION_MODE.name,
                                     creation_execution_code,
                                     **creation_params)
        deletion_settings = run_modes_settings.get(
            RUN_MODE.DELETION_MODE.name, dict())
        deletion_execution_code = deletion_settings.get("execution_code")
        deletion_params = deletion_settings.get("parameters", dict())
        deletion_mode = get_run_mode(RUN_MODE.DELETION_MODE.name,
                                     deletion_execution_code,
                                     **deletion_params)

        archiving_settings = run_modes_settings.get(
            RUN_MODE.MOVING_MODE.name, dict())
        archiving_execution_code = archiving_settings.get("execution_code")
        archiving_params = archiving_settings.get("parameters", dict())
        archiving_mode = get_run_mode(RUN_MODE.MOVING_MODE.name,
                                      archiving_execution_code,
                                      **archiving_params)
        return creation_mode, deletion_mode, archiving_mode


old_settings = OldSettings()

#    @classmethod
#    def get_settings(cls, uri):
#        parsed_uri = urlsplit(uri)
#        if parsed_uri.scheme == 'http':
#            pass
#        elif parsed_uri.scheme == 'file':
#            cls.get_settings_from_file(parsed_uri.path)
#
#    @classmethod
#    def get_settings_from_file(cls, path):
#        try:
#            with open(path) as fh:
#                all_settings = json.load(fh)
#                cls._package_settings = all_settings.get("packages", {})
#                cls._file_resource_settings = all_settings.get(
#                    "file_resources", {})
#                cls._mover_settings = all_settings.get("movers", {})
#                cls._processing_task_settings = all_settings.get(
#                    "processing_tasks", {})
#        except IOError as e:
#            logger.error(e)
#
#    @classmethod
#    def get_file_resource(cls, name, timeslot):
#        try:
#            file_resource_settings = cls._file_resource_settings[name]
#        except KeyError:
#            raise errors.InvalidSettingsError("file_resource '{}' is "
#                                              "not defined in the "
#                                              "settings".format(name))
#        local_mover = cls.get_mover()
#        the_settings = file_resource_settings.copy()
#        del the_settings["search_paths"]
#        file_resource = fileresource.FileResource(name=name, timeslot=timeslot,
#                                                  local_mover=local_mover,
#                                                  **the_settings)
#        for sps in file_resource_settings["search_paths"]:
#            path_pattern = sps["path"]
#            remote_movers = []
#            for remote_mover_settings in sps.get("remote_movers", []):
#                mover_name = remote_mover_settings["name"]
#                protocol = remote_mover_settings.get("protocol", "FTP")
#                remote_movers.append(cls.get_mover(mover_name, protocol))
#            file_resource.add_search_path(path_pattern, remote_movers)
#        return file_resource
#
#    @classmethod
#    def get_mover(cls, name=None, protocol="LOCAL"):
#        local_host_name = gethostname()
#        name = local_host_name if name is None else name
#        mover_settings = cls._mover_settings.get(name, dict())
#        if protocol.upper() == "LOCAL":
#            mover = resourcemover.LocalMover(name=local_host_name,
#                                             **mover_settings)
#        else:
#            mover = cls._remote_mover_factory.get_mover(name, protocol,
#                                                        **mover_settings)
#        return mover
#
#    @classmethod
#    def get_processing_task(cls, name, timeslot, run_mode=None):
#        try:
#            processing_task_settings = cls._processing_task_settings[name]
#        except KeyError:
#            raise errors.InvalidSettingsError("processing_task '{}' is "
#                                              "not defined in the "
#                                              "settings".format(name))
#        the_settings = processing_task_settings.copy()
#        for k in ("inputs", "outputs", "run_modes"):
#            try:
#                del the_settings[k]
#            except KeyError:
#                pass
#        run_modes = cls._get_task_run_modes(
#            processing_task_settings.get("run_modes", dict()))
#        task = processingtask.Task(name, timeslot,
#                                             creation_mode=run_modes[0],
#                                             deletion_mode=run_modes[1],
#                                             archiving_mode=run_modes[2],
#                                             active_mode=run_mode,
#                                             **the_settings)
#        inputs_settings = processing_task_settings.get("inputs", [])
#        outputs_settings = processing_task_settings.get("outputs", [])
#
#        cls._add_task_resources(timeslot, inputs_settings, task.add_inputs)
#        cls._add_task_resources(timeslot, outputs_settings, task.add_outputs)
#        return task
#
#    @classmethod
#    def _add_task_resources(cls, timeslot, resources_settings,
#                            task_method_callback):
#        for resource in resources_settings:
#            file_resource = cls.get_file_resource(resource["name"], timeslot)
#            strategy_settings = resource.get("strategy")
#            try:
#                strategy = strategy_settings.get("name",
#                                                 STRATEGY["SINGLE_ABSOLUTE"])
#                strategy_params = strategy_settings.copy()
#                del strategy_params["name"]
#            except AttributeError:
#                strategy = None
#                strategy_params = None
#            task_method_callback(
#                file_resource, strategy=strategy,
#                strategy_params=strategy_params,
#                except_when=resource.get("except_when"),
#                optional_when=resource.get("optional_when"),
#                filtering_rules=resource.get("filtering_rules")
#            )
#
#    @classmethod
#    def _get_task_run_modes(cls, run_modes_settings):
#        creation_settings = run_modes_settings.get(
#            RUN_MODES.CREATION_MODE.name, dict())
#        creation_execution_code = creation_settings.get("execution_code")
#        creation_params = creation_settings.get("parameters", dict())
#        creation_mode = get_run_mode(RUN_MODES.CREATION_MODE.name,
#                                     creation_execution_code,
#                                     **creation_params)
#        deletion_settings = run_modes_settings.get(
#            RUN_MODES.DELETION_MODE.name, dict())
#        deletion_execution_code = deletion_settings.get("execution_code")
#        deletion_params = deletion_settings.get("parameters", dict())
#        deletion_mode = get_run_mode(RUN_MODES.DELETION_MODE.name,
#                                     deletion_execution_code,
#                                     **deletion_params)
#
#        archiving_settings = run_modes_settings.get(
#            RUN_MODES.ARCHIVING_MODE.name, dict())
#        archiving_execution_code = archiving_settings.get("execution_code")
#        archiving_params = archiving_settings.get("parameters", dict())
#        archiving_mode = get_run_mode(RUN_MODES.ARCHIVING_MODE.name,
#                                      archiving_execution_code,
#                                      **archiving_params)
#        return creation_mode, deletion_mode, archiving_mode
