"""
Selection rules for the fileresources used in conductor
"""

from datetime import datetime, timedelta
from calendar import monthrange

class SelectionRule(object):
    """
    Abstract base class for all selection rules.

    The following terminology is used in the context of this class:

    * base_timeslot
    * reference_timeslot
    * timeslot
    * offset_years
    * offset_months
    * offset_days
    * offset_hours
    * offset_minutes
    * offset_decades
    """

    offset_years = 0
    offset_months = 0
    offset_days = 0
    offset_hours = 0
    offset_minutes = 0
    offset_dekades = 0

    _base_timeslot = None
    """
    The timeslot used as a basis for other datetime related calculations
    """

    offset_timeslot = _base_timeslot
    """
    This is the timeslot that results from offsetting the base_timeslot
    with the initial offsets
    """

    @property
    def base_timeslot(self):
        return self._base_timeslot

    @base_timeslot.setter
    def base_timeslot(self, ts):
        if isinstance(ts, basestring):
            self._base_timeslot = datetime.strptime(ts, '%Y%m%d%H%M')
        else:
            self._base_timeslot = ts

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

    def __init__(self, base_timeslot=None, years=0, months=0, days=0,
                 hours=0, minutes=0, dekades=0):
        self.base_timeslot = base_timeslot
        self.offset_years = years
        self.offset_months = months
        self.offset_days = days
        self.offset_hours = hours
        self.offset_minutes = minutes
        self.offset_dekades = dekades

    def __repr__(self):
        return "{}({}, y: {:+}, m: {:+}, d: {:+}, H: {:+}, " \
               "M: {:+}, D: {:+})".format(self.__class__.__name__,
                                          self.base_timeslot,
                                          self.offset_years,
                                          self.offset_months,
                                          self.offset_days,
                                          self.offset_hours,
                                          self.offset_minutes,
                                          self.offset_dekades)

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


class OffsetRule(SelectionRule):

    @property
    def timeslot(self):
        ts = super(OffsetRule, self).offset_timeslot(
            self.reference_timeslot, self.specific_offset_years,
            self.specific_offset_months, self.specific_offset_days,
            self.specific_offset_hours, self.specific_offset_minutes,
            self.specific_offset_dekades)
        return ts


    def __init__(self, base_timeslot, years=0, months=0, days=0, hours=0,
                 minutes=0, dekades=0, specific_years=0, specific_months=0,
                 specific_days=0, specific_hours=0, specific_minutes=0,
                 specific_dekades=0):
        super(OffsetRule, self).__init__(base_timeslot, years, months, days,
                                         hours, minutes, dekades)
        self.specific_offset_years = specific_years
        self.specific_offset_months = specific_months
        self.specific_offset_days = specific_days
        self.specific_offset_hours = specific_hours
        self.specific_offset_minutes = specific_minutes
        self.specific_offset_dekades = specific_dekades

    def choose(self, *resources):
        return sorted(resources)[0]
