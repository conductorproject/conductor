"""
Classes for aiding the timeslot displacement strategies used in conductor
"""

from datetime import datetime, timedelta
from calendar import monthrange
import logging

logger = logging.getLogger(__name__)


class TimeslotDisplacement(object):

    base_timeslot = None
    years = 0
    months = 0
    days = 0
    hours = 0
    minutes = 0
    dekades = 0
    _reference_timeslot = base_timeslot

    @property
    def reference_timeslot(self):
        if self.base_timeslot is not None:
            ts = self.offset_timeslot(self.base_timeslot, self.years,
                                      self.months, self.days,
                                      self.hours, self.minutes,
                                      self.dekades)
        else:
            ts = None
        return ts

    def __init__(self, base_timeslot, years=0, months=0, days=0, hours=0,
                 minutes=0, dekades=0):
        self.base_timeslot = base_timeslot
        self.years = years
        self.months = months
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.dekades = dekades

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

    def get_timeslots(self, start_years=0, start_months=0, start_days=0,
                      start_hours=0, start_minutes=0, start_dekades=0,
                      frequency_years=0, frequency_months=0, frequency_days=0,
                      frequency_hours=0, frequency_minutes=0,
                      frequency_dekades=0, number_of_timeslots=0):
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
