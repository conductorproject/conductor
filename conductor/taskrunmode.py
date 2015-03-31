"""
Run modes for conductor's ProcessingTasks
"""

import logging

from enum import Enum

import errors

logger = logging.getLogger(__name__)

RUN_MODES = Enum("RUN_MODES", "CREATION_MODE "
                              "DELETION_MODE "
                              "ARCHIVING_MODE")

def get_run_mode(name, execution_code=None, **params):
    object_map = {
        RUN_MODES.CREATION_MODE: CreationMode,
        RUN_MODES.DELETION_MODE: DeletionMode,
        RUN_MODES.ARCHIVING_MODE: ArchivingMode,
    }
    try:
        mode = RUN_MODES[name]
        run_mode = object_map[mode](execution_code, **params)
    except KeyError:
        raise errors.RunModeError("Mode '{}' is invalid".format(name))
    return run_mode


class TaskRunMode(object):
    execution_code = None

    def __init__(self, execution_code):
        self.execution_code = execution_code

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.execution_code)


class CreationMode(TaskRunMode):

    def __init__(self, execution_code):
        super(CreationMode, self).__init__(execution_code)


class DeletionMode(TaskRunMode):

    def __init__(self, execution_code, start_years=0, start_months=0,
                 start_days=0, start_hours=0, start_minutes=0,
                 start_dekades=0, frequency_years=0, frequency_months=0,
                 frequency_days=0, frequency_hours=0, frequency_minutes=0,
                 frequency_dekades=0, number_of_timeslots=1):
        super(DeletionMode, self).__init__(execution_code)
        self.start_years = start_years
        self.start_months = start_months
        self.start_days = start_days
        self.start_hours = start_hours
        self.start_minutes = start_minutes
        self.start_dekades = start_dekades
        self.frequency_years = frequency_years
        self.frequency_months = frequency_months
        self.frequency_days = frequency_days
        self.frequency_hours = frequency_hours
        self.frequency_minutes = frequency_minutes
        self.frequency_dekades = frequency_dekades
        self.number_of_timeslots = number_of_timeslots

    def __repr__(self):
        sy = "y:{:+}".format(self.start_years)
        sm = "m:{:+}".format(self.start_months)
        sd = "d:{:+}".format(self.start_days)
        sH = "H:{:+}".format(self.start_hours)
        sM = "M:{:+}".format(self.start_minutes)
        sD = "D:{:+}".format(self.start_dekades)
        fy = "y:{:+}".format(self.frequency_years)
        fm = "m:{:+}".format(self.frequency_months)
        fd = "d:{:+}".format(self.frequency_days)
        fH = "H:{:+}".format(self.frequency_hours)
        fM = "M:{:+}".format(self.frequency_minutes)
        fD = "D:{:+}".format(self.frequency_dekades)
        return "{}({} {})".format(
            self.__class__.__name__,
            self.execution_code,
            ", ".join((sy, sm, sd, sH, sM, sD, fy, fm, fd, fH, fM, fD))
        )


class ArchivingMode(TaskRunMode):
    pass
