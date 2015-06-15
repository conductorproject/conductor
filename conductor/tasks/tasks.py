"""
Processing tasks for conductor.
"""

import os
import shutil
import logging
from datetime import datetime
from tempfile import mkdtemp

from .. import TaskResourceRole
from .. import errors
from ..resources import resource_factory
from ..settings import settings
from .taskresources import task_resource_factory
from . import taskobserver

logger = logging.getLogger(__name__)

class TaskFactory(object):

    def get_task(self, name, timeslot=None):
        try:
            s = [i for i in settings.tasks if i["name"] == name][0]
        except IndexError:
            raise errors.TaskNotDefinedError(
                "Task {!r} is not defined in the settings".format(name))
        t = Task(name, s["urn"], timeslot,
                 description=s.get("description", u""),
                 remove_working_dir=s.get("remove_working_directory", True),
                 decompress_inputs=s.get("decompress_inputs", True))
        for inp in s.get("inputs", []):
            resource = resource_factory.get_resource(inp["name"], t.timeslot)
            task_resources = task_resource_factory.get_task_resources(
                resource, except_when=inp.get("except_when", {}),
                optional_when=inp.get("optional_when", {}),
                can_get_representation=inp.get("can_get_representation", True),
                displace_timeslot=inp.get("displace_timeslot", {}),
                multiple_timeslots=inp.get("generate_multiple_timeslots", {}),
                multiple_parameters=inp.get("generate_multiple_parameters", [])
            )
            for tr in task_resources:
                t.add_task_resource(tr, TaskResourceRole.INPUT)
        for out in s.get("outputs", []):
            pass
        return t


task_factory = TaskFactory()


class Task(object):
    description = u""
    working_dir = None
    name = u""
    _urn = u""
    _timeslot = None
    _inputs = []
    _outputs = []
    _run_observers = []
    _run_progress = 0
    _run_details = u""
    _run_state = u""

    @property
    def safe_name(self):
        return self.name.replace(" ", "_")

    @property
    def urn(self):
        return self._urn.format(self)

    @urn.setter
    def urn(self, urn):
        self._urn = urn

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
    def timeslot_string(self):
        if self.timeslot is not None:
            result = self.timeslot.strftime("%Y%m%d%H%M")
        else:
            result = ""
        return result

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

    @property
    def working_dir_inputs(self):
        return os.path.join(self.working_dir, "inputs")

    @property
    def working_dir_outputs(self):
        return os.path.join(self.working_dir, "outputs")

    def __init__(self, name, urn, timeslot, description="",
                 remove_working_dir=True, decompress_inputs=True):
        self.name = name
        self._urn = urn
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

    def add_task_resource(self, task_resource, role):
        group = {
            TaskResourceRole.INPUT: self._inputs,
            TaskResourceRole.OUTPUT: self._outputs,
        }[role]
        group.append(task_resource)

    def fetch_inputs(self):
        fetched = dict()
        for inp in self.active_inputs:
            logger.info("fetching '{}'...".format(inp.resource.name))
            fetched_path = inp.fetch(self.working_dir_inputs)
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
        result = True
        fetched = self.fetch_inputs()
        able, able_details = self.able_to_execute(fetched)
        if able:
            execution_result = self.execute(fetched)
            generated_outputs, generated_details = self.check_for_outputs()
            if not generated_outputs:
                raise errors.InvalidExecutionError(
                    "The task executed correctly but not all of the expected "
                    "outputs were found: {}".format(generated_details)
                )
            self.move_outputs(execution_result)
        else:
            raise errors.ExecutionCannotStartError(able_details)
        return result

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

    def check_for_outputs(self):
        """
        Confirm that the expected outputs have been generated.

        :return:
        """
        details = []
        all_found = True
        for outp, temp_path in self.find_temporary_outputs():
            all_found = all_found and temp_path is not None
            if temp_path is None:
                details.append("Could not find output: {}".format(outp))
        return all_found, details

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
