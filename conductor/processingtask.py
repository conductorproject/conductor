"""
Processing tasks for conductor.
"""

import os
import logging
from datetime import datetime
from tempfile import mkdtemp

import taskresource

logger = logging.getLogger(__name__)


class ProcessingTask(object):
    description = ""
    working_dir = None
    _timeslot = None
    _inputs = []
    _outputs = []

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

    def __init__(self, name, timeslot, description=""):
        self.name = name
        self.timeslot = timeslot
        self.description = description
        self._inputs = []
        self._outputs = []
        self.working_dir = mkdtemp()
        logger.debug("working_dir: {}".format(self.working_dir))

    def _reconfigure_resources(self, old_timeslot):
        """
        Update the timeslots of all inputs and outputs

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
        raise NotImplementedError


class TaskContextManagerSettings(object):

    def __init__(self, settings_manager, task_name, timeslot):
        self.task = settings_manager.get_processing_task(task_name, timeslot)

    def __enter__(self):
        return self.task

    def __exit__(self, exc_type=None, exc_value=None, exc_traceback=None):
        os.removedirs(self.task.working_dir)
