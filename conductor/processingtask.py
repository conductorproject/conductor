"""
Processing tasks for conductor.
"""

import os
import shutil
import logging
from datetime import datetime
from tempfile import mkdtemp

from enum import Enum

import taskresource
import errors
from taskrunmode import RUN_MODE
import taskobserver

logger = logging.getLogger(__name__)


class ProcessingTask(object):
    description = ""
    working_dir = None
    _timeslot = None
    _inputs = []
    _outputs = []
    _run_observers = []
    _run_progress = 0
    _run_details = ""
    _run_state = ""

    @property
    def active_inputs(self):
        return [r for r in self._inputs if r.active]

    @property
    def active_outputs(self):
        return [r for r in self._outputs if r.active]

    @property
    def mandatory_inputs(self):
        return [r for r in self._inputs if not r.optional]

    @property
    def mandatory_outputs(self):
        return [r for r in self._outputs if not r.optional]

    @property
    def timeslot(self):
        return self._timeslot

    @timeslot.setter
    def timeslot(self, timeslot):
        if isinstance(timeslot, basestring):
            timeslot = datetime.strptime(timeslot, "%Y%m%d%H%M")
        old_timeslot = self._timeslot
        self._timeslot = timeslot
        self._reconfigure_resources(old_timeslot)

    @property
    def run_progress(self):
        return self._run_progress

    @run_progress.setter
    def run_progress(self, value):
        self._run_progress = value
        self.update_observers()

    @property
    def run_state(self):
        return self._run_state

    @run_state.setter
    def run_state(self, value):
        self._run_state = value
        self.update_observers()

    @property
    def run_details(self):
        return self._run_details

    @run_details.setter
    def run_details(self, value):
        self._run_details = value
        self.update_observers()

    def __init__(self, name, timeslot, description="",
                 creation_mode=None, deletion_mode=None,
                 archiving_mode=None, active_mode=None,
                 remove_working_dir=True, decompress_inputs=True):
        self.creation_mode = creation_mode
        self.deletion_mode = deletion_mode
        self.archiving_mode = archiving_mode
        self.name = name
        self.timeslot = timeslot
        self.description = description
        self._inputs = []
        self._outputs = []
        self.remove_working_dir = remove_working_dir
        self.decompress_inputs = decompress_inputs
        self.working_dir = mkdtemp()
        self._run_observers = [taskobserver.ConsoleObserver(self)]
        self.run_details = ""
        self.run_progress = 0
        self.run_state = "Not running"
        logger.debug("working_dir: {}".format(self.working_dir))

    def update_observers(self):
        [obs() for obs in self._run_observers]

    def _reconfigure_resources(self, old_timeslot):
        """
        Update the timeslots of all inputs and outputs

        This method is called whenever the timeslot changes.

        :return:
        """

        logger.info("Reconfiguring input and output timeslots...")
        for resource in self._inputs + self._outputs:
            delta = resource.file_resource.timeslot - old_timeslot
            resource.file_resource.timeslot = self.timeslot + delta

    def add_inputs(self, file_resource, strategy=None, strategy_params=None,
                   except_when=None, optional_when=None, filtering_rules=None):
        task_resources = taskresource.factory.get_resources(
            file_resource, base_timeslot=self.timeslot,
            strategy=strategy, strategy_params=strategy_params,
            except_when=except_when, optional_when=optional_when,
            filtering_rules=filtering_rules
        )
        self._inputs.extend(task_resources)

    def add_outputs(self, file_resource, strategy=None, strategy_params=None,
                    except_when=None, optional_when=None,
                    filtering_rules=None):
        task_resources = taskresource.factory.get_resources(
            file_resource, base_timeslot=self.timeslot,
            strategy=strategy, strategy_params=strategy_params,
            except_when=except_when, optional_when=optional_when,
            filtering_rules=filtering_rules
        )
        self._outputs.extend(task_resources)

    def find_inputs(self):
        found = dict()
        for inp in self.active_inputs:
            found[inp] = inp.find()
        return found

    def find_outputs(self):
        found = dict()
        for outp in self.active_outputs:
            found[outp] = outp.find()
        return found

    def fetch_inputs(self):
        fetched = dict()
        for inp in self.active_inputs:
            logger.info("fetching '{}'...".format(inp.file_resource.name))
            fetched_path = inp.fetch(self.working_dir)
            if fetched_path is not None and self.decompress_inputs:
                fetched_path, = inp.file_resource.decompress(fetched_path)
            fetched[inp] = fetched_path
        return fetched

    def clean_temporary_resources(self):
        parent = os.path.dirname(self.working_dir)
        shutil.rmtree(self.working_dir)
        try:
            os.removedirs(parent)
        except OSError:
            pass

    def run(self, mode):
        """
        Execute the sequence of operations defined by the active_mode.

        :return:
        """

        mode_map = {
            RUN_MODE.CREATION_MODE: self.run_creation_mode,
            RUN_MODE.DELETION_MODE: self.run_deletion_mode,
            RUN_MODE.MOVING_MODE: self.run_moving_mode,
        }
        try:
            result = mode_map[mode](mode)
        except KeyError:
            raise errors.RunModeError("invalid run_mode {}".format(mode))
        return result

    def run_creation_mode(self, mode):
        logger.info("Running '{0.name}' in creation mode...".format(self))
        result = True
        self.initialize_mode(mode)
        fetched = self.fetch_inputs()
        able, able_details = self.able_to_execute(fetched)
        if able:
            execution_result = self.execute(fetched)
        else:
            raise errors.ExecutionCannotStartError(able_details)
        self.move_outputs(execution_result)
        self.finalize_mode(mode)
        return result

    def run_deletion_mode(self):
        logger.info("Running '{0.name}' in deletion mode...".format(self))
        result = True
        return result

    def run_moving_mode(self):
        logger.info("Running '{0.name}' in moving mode...".format(self))
        result = True
        return result

    def initialize_mode(self, mode):
        """
        Perform some task specific initialization.

        This method can be reimplemented in child classes in order to
        perform some pre-processing steps. The default implementation
        does nothing.

        :param mode:
        :type mode: taskrunmode.RUN_MODE
        :return: None
        """
        pass

    def finalize_mode(self, mode):
        """
        Perform some task specific finalization.

        This method can be reimplemented in child classes in order to
        perform some post-processing steps. The default implementation
        does nothing.

        :param mode:
        :type mode: taskrunmode.RUN_MODE
        :return: None
        """
        pass

    def able_to_execute(self, fetched_inputs):
        """
        Determine if the task has all of the necessary conditions to execute.

        :param fetched_inputs:
        :return:
        """

        all_ok = []
        details = []
        for inp, path in fetched_inputs.iteritems():
            if inp in self.mandatory_inputs:
                if path is not None:
                    this_ok = True
                else:
                    this_ok = False
                    details.append("Mandatory input '{}' is not "
                                   "available".format(inp.file_resource.name))
            else:
                this_ok = True
            all_ok.append(this_ok)
        result = True if all(all_ok) else False
        return result, ", ".join(details)

    def execute(self, fetched):
        """
        Perform task specific calculations.

        This method must be reimplemented in derived classes. The default
        implementation does nothing.

        :param fetched:
        :return:
        """
        return True

    def move_outputs(self, execution_result):
        raise NotImplementedError

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.name)


class TaskContextManagerSettings(object):

    def __init__(self, settings_manager, task_name, timeslot):
        self.task = settings_manager.get_processing_task(task_name, timeslot)

    def __enter__(self):
        return self.task

    def __exit__(self, exc_type=None, exc_value=None, exc_traceback=None):
        self.task.clean_temporary_resources()
