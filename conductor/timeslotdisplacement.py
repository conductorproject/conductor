"""
Classes for aiding the timeslot displacement strategies used in conductor
"""

from datetime import datetime, timedelta
from calendar import monthrange
import logging

from enum import Enum

logger = logging.getLogger("conductor.{}".format(__name__))

STRATEGY = Enum("STRATEGY",
                "SINGLE_ABSOLUTE "
                "SINGLE_RELATIVE "
                "MULTIPLE_ORDERED")

class TimeslotDisplacementStrategy(object):

    offset_years = 0
    offset_months = 0
    offset_days = 0
    offset_hours = 0
    offset_minutes = 0
    offset_dekades = 0
    base_timeslot = None

    _reference_timeslot = base_timeslot
    """
    This is the timeslot that results from offsetting the base_timeslot
    with the initial offsets
    """

    @property
    def reference_timeslot(self):
        if self.base_timeslot is not None:
            ts = self.offset_timeslot(self.base_timeslot, self.offset_years,
                                      self.offset_months, self.offset_days,
                                      self.offset_hours, self.offset_minutes,
                                      self.offset_dekades)
        else:
            ts = None
        return ts

    def __init__(self, base_timeslot, offset_years=0, offset_months=0,
                 offset_days=0, offset_hours=0, offset_minutes=0,
                 offset_dekades=0):
        self.base_timeslot = base_timeslot
        self.base_offset_years = offset_years
        self.base_offset_months = offset_months
        self.base_offset_days = offset_days
        self.base_offset_hours = offset_hours
        self.base_offset_minutes = offset_minutes
        self.base_offset_dekades = offset_dekades

    @classmethod
    def offset_timeslot(cls, timeslot, years=0, months=0, days=0, hours=0,
                        minutes=0, dekades=0):
        new_year = timeslot.year + years
        if months != 0:
            month_years = (timeslot.month + months - 1) // 12
            new_year += month_years
        new_month = (timeslot.month + months) % 12
        new_month = 12 if new_month == 0 else new_month
        month_n_days = monthrange(new_year, new_month)[1]
        new_day = timeslot.day if timeslot.day < month_n_days else month_n_days
        candidate = datetime(new_year, new_month, new_day, timeslot.hour,
                             timeslot.minute)
        offset_time = (hours * 60 * 60) + minutes * 60
        if dekades != 0:
            t_day = timeslot.day
            first_day = 1 if t_day < 11 else (11 if t_day < 21 else 21)
            timeslot_first_day = datetime(timeslot.year, timeslot.month,
                                          first_day, timeslot.hour,
                                          timeslot.minute)
            end_timeslot = timeslot_first_day
            forward = dekades / abs(dekades) > 0
            factor = 1 if forward else -1
            for d in range(abs(dekades)):
                if (t_day < 21 and forward) or (t_day > 1 and not forward):
                    end_timeslot += factor * timedelta(days=10)
                else:
                    the_ts = end_timeslot if forward else \
                        end_timeslot - timedelta(days=1)
                    n_days = monthrange(the_ts.year, the_ts.month)[1] - 20
                    end_timeslot += factor * timedelta(days=n_days)
            days += factor * abs((end_timeslot - timeslot).days)
        delta = timedelta(days=days, seconds=offset_time)
        return candidate + delta


class SingleAbsoluteStrategy(TimeslotDisplacementStrategy):

    def __init__(self, base_timeslot, offset_years=0, offset_months=0,
                 offset_days=0, offset_hours=0, offset_minutes=0,
                 offset_dekades=0):
        super(SingleAbsoluteStrategy, self).__init__(base_timeslot,
                                                     offset_years,
                                                     offset_months,
                                                     offset_days,
                                                     offset_hours,
                                                     offset_minutes,
                                                     offset_dekades)

    def get_timeslot(self, offset_years=0, offset_months=0, offset_days=0,
                     offset_hours=0, offset_minutes=0, offset_dekades=0):
        return self.offset_timeslot(self.reference_timeslot, offset_years,
                                    offset_months, offset_days, offset_hours,
                                    offset_minutes, offset_dekades)



class SingleRelativeStrategy(TimeslotDisplacementStrategy):
    AGE = Enum("AGE", "MOST_RECENT LEAST_RECENT")
    FIX = Enum("FIX", "YEAR MONTH DAY HOUR MINUTE DEKADE")
    REGARDING = Enum("REGARDING", "BASE_TIMESLOT REFERENCE_TIMESLOT")
    CONSTRAIN = Enum("CONSTRAIN", "BEFORE AFTER REGARDLESS")

    def __init__(self, base_timeslot, offset_years=0, offset_months=0,
                 offset_days=0, offset_hours=0, offset_minutes=0,
                 offset_dekades=0,
                 age=AGE.MOST_RECENT,
                 fix=None,
                 regarding=REGARDING.REFERENCE_TIMESLOT,
                 constrain=CONSTRAIN.REGARDLESS):
        super(SingleRelativeStrategy, self).__init__(base_timeslot,
                                                     offset_years,
                                                     offset_months,
                                                     offset_days,
                                                     offset_hours,
                                                     offset_minutes,
                                                     offset_dekades)
        self.age = age
        self.fix = []
        for f in (fix if fix is not None else []):
            if f in self.REGARDING:
                self.fix.append(f)
        self.regarding = regarding
        self.constrain = constrain

    def get_timeslot(self):
        return self.reference_timeslot


class MultipleOrderedStategy(TimeslotDisplacementStrategy):

    def __init__(self, base_timeslot, offset_years=0, offset_months=0,
                 offset_days=0, offset_hours=0, offset_minutes=0,
                 offset_dekades=0):
        super(MultipleOrderedStategy, self).__init__(base_timeslot,
                                                     offset_years,
                                                     offset_months,
                                                     offset_days,
                                                     offset_hours,
                                                     offset_minutes,
                                                     offset_dekades)

    def get_timeslots(self, start_years=0, start_months=0, start_days=0,
                      start_hours=0, start_minutes=0, start_dekades=0,
                      frequency_years=0, frequency_months=0, frequency_days=0,
                      frequency_hours=0, frequency_minutes=0,
                      frequency_dekades=0, number_of_timeslots=1):
        result = []
        for i in xrange(number_of_timeslots):
            off_years = start_years + i * frequency_years
            off_months = start_months + i * frequency_months
            off_days = start_days + i * frequency_days
            off_hours = start_hours + i * frequency_hours
            off_minutes = start_minutes + i * frequency_minutes
            off_dekades = start_dekades + i * frequency_dekades
            result.append(self.offset_timeslot(self.reference_timeslot,
                                               off_years, off_months, off_days,
                                               off_hours, off_minutes,
                                               off_dekades))
        return result
