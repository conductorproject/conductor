__author__ = 'geo2'

import enum


class ConductorScheme(enum.Enum):
    FILE = 1
    FTP = 2
    SFTP = 3
    HTTP = 4
