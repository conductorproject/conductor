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
