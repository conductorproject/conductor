"""
Classes for managing conductor.task resources
"""

import copy
import logging

from . import timeslotdisplacement as tsd

logger = logging.getLogger(__name__)


class TaskResourceFactory(object):

    def get_task_resources(self, base_resource, except_when=None,
                           optional_when=None, can_get_representation=True,
                           displace_timeslot=None, multiple_timeslots=None,
                           multiple_parameters=None):
        """
        Create new TaskResource instances.

        :param base_resource: The resource that serves as a base for the
            new TaskResource instances to be created
        :type base_resource: conductor.resource.Resource
        :param except_when: A mapping specifying when the TaskResources
            should not be active. It accepts the following keys:

            * year
            * month
            * day
            * hour
            * minute
            * dekade

        :type except_when: dict
        :param optional_when: A mapping specifying when the TaskResources
            should be optional. It accepts the same keys as the `except_when`
            parameter
        :type optional_when: dict
        :param can_get_representation: Should the client code be allowed to
            retrieve a representation of the resource into some local
            directory?
        :type can_get_representation: bool
        :param displace_timeslot: A mapping specifying how the TaskResource's
            timeslot should be displaced from the timeslot of the
            `base_resource` parameter. It accepts the same keys as the
            `except_when` parameter.
        :type displace_timeslot: dict
        :param multiple_timeslots: A mapping specifying how to create multiple
            TaskResource instances based on further displacements of the
            timeslot of the input `base_resource`. These displacements are
            applied after the `displace_timeslot` parameter
        :type multiple_timeslots: dict
        :param multiple_parameters: A sequence of parameters that can be used
            to generate multiple instances of the TaskResource.
        :type multiple_parameters: list
        :return:
        """

        displace_timeslot = displace_timeslot or {}
        multiple_parameters = multiple_parameters or []
        base_timeslot = tsd.TimeslotDisplacement.offset_timeslot(
            base_resource.timeslot, **displace_timeslot)
        slots = []
        if multiple_timeslots is not None:
            for i in xrange(multiple_timeslots.get("number_of_timeslots", 1)):
                v = multiple_timeslots.get("frequency", 1) * i
                param = {multiple_timeslots.get("frequency_unit", "hour"): v}
                slots.append(tsd.TimeslotDisplacement.offset_timeslot(
                    base_timeslot, **param))
        else:
            slots.append(base_timeslot)
        new_resources = []
        for s in slots:
            if len(multiple_parameters) > 0:
                for p in multiple_parameters:
                    for v in p["values"]:
                        r = copy.deepcopy(base_resource)
                        r.timeslot = s
                        r.parameters[p["parameter"]] = v
                        new_resources.append(r)
            else:
                r = copy.copy(base_resource)
                r.timeslot = s
                new_resources.append(r)
        task_resources = []
        for r in new_resources:
            tr = TaskResource(r, optional_when=optional_when,
                              except_when=except_when,
                              can_get_representation=can_get_representation)
            task_resources.append(tr)
        return task_resources


task_resource_factory = TaskResourceFactory()


class TaskResource(object):

    resource = None
    optional_when = dict()
    except_when = dict()

    @property
    def active(self):
        result = True
        if self.resource.timeslot is not None:
            f = self.resource
            y = f.timeslot.year in self.except_when["year"]
            m = f.timeslot.month in self.except_when["month"]
            d = f.timeslot.day in self.except_when["day"]
            H = f.timeslot.hour in self.except_when["hour"]
            M = f.timeslot.minute in self.except_when["minute"]
            D = f.dekade in self.except_when["dekade"]
            if any((y, m, d, H, M, D)):
                result = False
        return result

    @property
    def optional(self):
        result = False
        if self.resource.timeslot is not None:
            f = self.resource
            y = f.timeslot.year in self.optional_when["year"]
            m = f.timeslot.month in self.optional_when["month"]
            d = f.timeslot.day in self.optional_when["day"]
            H = f.timeslot.hour in self.optional_when["hour"]
            M = f.timeslot.minute in self.optional_when["minute"]
            D = f.dekade in self.optional_when["dekade"]
            if any((y, m, d, H, M, D)):
                result = True
        return result

    def __init__(self, resource, optional_when=None,
                 except_when=None, can_get_representation=True):
        self.resource = resource
        self.optional_when = dict(year=[], month=[], day=[], hour=[],
                                  minute=[], dekade=[])
        self.optional_when.update(optional_when or {})
        self.except_when = dict(year=[], month=[], day=[], hour=[],
                                minute=[], dekade=[])
        self.except_when.update(except_when or {})
        self.can_get_representation = can_get_representation

    def __repr__(self):
        return "{0}.{1.__class__.__name__}({1.resource!r})".format(
            __name__, self)
