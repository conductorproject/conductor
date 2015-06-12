"""
Classes for managing conductor.task resources
"""

import copy
import logging

from . import errors
from . import timeslotdisplacement as tsd
from . import TimeslotStrategy

logger = logging.getLogger(__name__)


class TaskResourceFactory(object):

    def get_resources(self, resource, base_timeslot=None,
                      strategy=TimeslotStrategy.SINGLE,
                      strategy_params=None, except_when=None,
                      optional_when=None,
                      can_get_representation=True):
        """
        Create new TaskResources

        This is a factory for abstracting away the complexities of creating
        multiple resources

        :return:
        """

        except_when = except_when or dict()
        optional_when = optional_when or dict()
        params = strategy_params or dict()
        ts = base_timeslot or resource.timeslot
        time_units = ["years", "months", "days", "hours", "minutes", "dekades"]
        offset_creation_params = dict()
        for u in time_units:
            offset_creation_params[u] = params.get(u, 0)
        s = tsd.TimeslotDisplacement(ts, **offset_creation_params)
        timeslots = []
        p = dict()
        for u in time_units:
            p["start_{}".format(u)] = params.get("start_{}".format(u), 0)
            p["frequency_{}".format(u)] = params.get(
                "frequency_{}".format(u), 0)
        p["number_of_timeslots"] = params.get("number_of_timeslots", 1)
        result = []
        for slot in s.get_timeslots(**p):
            new_resource = copy.copy(resource)
            new_resource.timeslot = slot
            tr = TaskResource(new_resource, optional_when=optional_when,
                              except_when=except_when,
                              can_get_representation=can_get_representation)
            result.append(tr)
        return result


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
            y = f.year in self.except_when["years"]
            m = f.month in self.except_when["months"]
            d = f.day in self.except_when["days"]
            H = f.hour in self.except_when["hours"]
            M = f.minute in self.except_when["minutes"]
            D = f.dekade in self.except_when["dekades"]
            if any((y, m, d, H, M, D)):
                result = False
        return result

    @property
    def optional(self):
        result = False
        if self.resource.timeslot is not None:
            f = self.resource
            y = f.year in self.optional_when["years"]
            m = f.month in self.optional_when["months"]
            d = f.day in self.optional_when["days"]
            H = f.hour in self.optional_when["hours"]
            M = f.minute in self.optional_when["minutes"]
            D = f.dekade in self.optional_when["dekades"]
            if any((y, m, d, H, M, D)):
                result = True
        return result

    def __init__(self, resource, optional_when=None,
                 except_when=None, can_get_representation=True):
        self.resource = resource
        self.optional_when = dict(years=[], months=[], days=[], hours=[],
                                  minutes=[], dekades=[])
        self.optional_when.update(optional_when)
        self.except_when = dict(years=[], months=[], days=[], hours=[],
                                minutes=[], dekades=[])
        self.except_when.update(except_when)
        self.can_get_representation = can_get_representation

    def __repr__(self):
        return "{0}.{1.__class__.__name__}({1.resource!r})".format(
            __name__, self)
