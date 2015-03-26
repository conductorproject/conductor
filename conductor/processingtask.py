"""
Processing tasks for conductor.
"""

from processingresource import factory

class ProcessingTask(object):
    _timeslot = None
    _inputs = []
    _outputs = []
    _processing_step_factory = factory

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
    def timeslot(self, ts):
        self._timeslot = ts
        self._inputs = []
        self._outputs = []

    def __init__(self, name, timeslot, inputs=None, outputs=None):
        self.name = name
        self._inputs = inputs
        self._outputs = outputs

    def add_input(self, processing_step_resource):
        self._inputs.append(processing_step_resource)

    def add_output(self, processing_step_resource):
        self._outputs.append(processing_step_resource)
