"""
Processing tasks for conductor.
"""

import logging
from datetime import datetime

import taskresource

logger = logging.getLogger("conductor.{}".format(__name__))


class ProcessingTask(object):
    description = ""
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
        self._timeslot = timeslot
        self._reconfigure_resources()

    def __init__(self, name, timeslot, description=""):
        self.name = name
        self.timeslot = timeslot
        self.description = description
        self._inputs = []
        self._outputs = []

    def _reconfigure_resources(self):
        """
        Update the timeslots of all inputs and outputs

        :return:
        """

        logger.info("Reconfiguring input and output timeslots...")
        for resource in self._inputs + self._outputs:
            delta = resource.timeslot - self.timeslot
            resource.timeslot = self.timeslot + delta

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
