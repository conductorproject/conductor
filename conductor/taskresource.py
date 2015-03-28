"""
Classes for managing conductor.ProcessingTask resources
"""

import copy
import logging

import timeslotdisplacement as tsd
import errors

logger = logging.getLogger(__name__)


class TaskResource(object):

    @property
    def active(self):
        result = True
        if self.file_resource.timeslot is not None:
            f = self.file_resource
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
        if self.file_resource.timeslot is not None:
            f = self.file_resource
            y = f.year in self.optional_when["years"]
            m = f.month in self.optional_when["months"]
            d = f.day in self.optional_when["days"]
            H = f.hour in self.optional_when["hours"]
            M = f.minute in self.optional_when["minutes"]
            D = f.dekade in self.optional_when["dekades"]
            if any((y, m, d, H, M, D)):
                result = True
        return result

    def __init__(self, file_resource, optional_when_years=None,
                 optional_when_months=None, optional_when_days=None,
                 optional_when_hours=None, optional_when_minutes=None,
                 optional_when_dekades=None, except_when_years=None,
                 except_when_months=None, except_when_days=None,
                 except_when_hours=None, except_when_minutes=None,
                 except_when_dekades=None, timeslot_choosing_rules=None):
        self.file_resource = file_resource
        self.optional_when = {
            "years": optional_when_years or [],
            "months": optional_when_months or [],
            "days": optional_when_days or [],
            "hours": optional_when_hours or [],
            "minutes": optional_when_minutes or [],
            "dekades": optional_when_dekades or [],
        }
        self.except_when = {
            "years": except_when_years or [],
            "months": except_when_months or [],
            "days": except_when_days or [],
            "hours": except_when_hours or [],
            "minutes": except_when_minutes or [],
            "dekades": except_when_dekades or [],
        }
        self.timeslot_choosing_rules = timeslot_choosing_rules

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.file_resource)

    def find(self):
        found_mover, found_paths = self.file_resource.find()
        chosen_path = None
        if any(found_paths):
            chosen_path = self.file_resource.choose(
                found_paths,
                filtering_rules=self.timeslot_choosing_rules
            )
        return found_mover, chosen_path


class TaskResourceFactory(object):

    def get_resources(self, resource, base_timeslot=None, strategy=None,
                      strategy_params=None, except_when=None,
                      optional_when=None, filtering_rules=None):
        """
        Create new TaskResources

        This is a factory for abstracting away the complexities of creating
        file resources

        :return:
        """

        if strategy is None:
            strategy = tsd.STRATEGY.SINGLE_ABSOLUTE
        else:
            try:
                strategy = tsd.STRATEGY[strategy]
            except KeyError:
                raise errors.InvalidSettingsError("Invalid Strategy "
                                                  "'{}'".format(strategy))
        except_when = except_when or dict()
        optional_when = optional_when or dict()
        params = strategy_params if strategy_params is not None else dict()
        ts = base_timeslot if base_timeslot is not None else resource.timeslot
        offset_creation_params = {
            "offset_years": params.get("years", 0),
            "offset_months": params.get("months", 0),
            "offset_days": params.get("days", 0),
            "offset_hours": params.get("hours", 0),
            "offset_minutes": params.get("minutes", 0),
            "offset_dekades": params.get("dekades", 0),
            }
        timeslots = []
        filtering_rules = filtering_rules or []
        if strategy == tsd.STRATEGY.SINGLE_ABSOLUTE:
            offsets = {
                "offset_years": params.get("relative_years", 0),
                "offset_months": params.get("relative_months", 0),
                "offset_days": params.get("relative_days", 0),
                "offset_hours": params.get("relative_hours", 0),
                "offset_minutes": params.get("relative_minutes", 0),
                "offset_dekades": params.get("relative_dekades", 0),
            }
            s = tsd.SingleAbsoluteStrategy(ts, **offset_creation_params)
            timeslots = [s.get_timeslot(**offsets)]
        elif strategy == tsd.STRATEGY.SINGLE_RELATIVE:
            filtering_rules = strategy_params
            timeslots = [ts]
        elif strategy == tsd.STRATEGY.MULTIPLE_ORDERED:
            s = tsd.MultipleOrderedStategy(ts, **offset_creation_params)
            multi_params = {
                "start_years": params.get("start_years", 0),
                "start_months": params.get("start_months", 0),
                "start_days": params.get("start_days", 0),
                "start_hours": params.get("start_hours", 0),
                "start_minutes": params.get("start_minutes", 0),
                "start_dekades": params.get("start_dekades", 0),
                "frequency_years": params.get("frequency_years", 0),
                "frequency_months": params.get("frequency_months", 0),
                "frequency_days": params.get("frequency_days", 0),
                "frequency_hours": params.get("frequency_hours", 0),
                "frequency_minutes": params.get("frequency_minutes", 0),
                "frequency_dekades": params.get("frequency_dekades", 0),
                "number_of_timeslots": params.get("number_of_timeslots", 1),
            }
            timeslots = s.get_timeslots(**multi_params)
        result = []
        for slot in timeslots:
            new_resource = copy.copy(resource)
            new_resource.timeslot = slot
            result.append(TaskResource(
                new_resource, optional_when_years=optional_when.get("years"),
                optional_when_months=optional_when.get("months"),
                optional_when_days=optional_when.get("days"),
                optional_when_hours=optional_when.get("hours"),
                optional_when_minutes=optional_when.get("minutes"),
                optional_when_dekades=optional_when.get("dekades"),
                except_when_years=except_when.get("years"),
                except_when_months=except_when.get("months"),
                except_when_days=except_when.get("days"),
                except_when_hours=except_when.get("hours"),
                except_when_minutes=except_when.get("minutes"),
                except_when_dekades=except_when.get("dekades"),
                timeslot_choosing_rules=filtering_rules
            ))
        return result

factory = TaskResourceFactory()
