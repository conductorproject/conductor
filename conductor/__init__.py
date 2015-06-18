import enum
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())


class ConductorScheme(enum.Enum):
    FILE = 1
    FTP = 2
    SFTP = 3
    HTTP = 4


class ServerSchemeMethod(enum.Enum):
    GET = 1
    POST = 2
    FIND = 3


class TimeslotStrategy(enum.Enum):

    SINGLE = 1
    MULTIPLE = 2


class TaskResourceRole(enum.Enum):

    INPUT = 1
    OUTPUT = 2


class TemporalSelectionRule(enum.Enum):

    LATEST = 1
    EARLIEST = 2


class ParameterSelectionRule(enum.Enum):

    HIGHEST = 1
    LOWEST = 2


class TemporalPart(enum.Enum):
    YEAR = 1
    MONTH = 2
    DAY = 3
    HOUR = 4
    MINUTE = 5
    SECOND = 6
    YEAR_DAY = 7
    DEKADE = 8
