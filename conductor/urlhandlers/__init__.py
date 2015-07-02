import logging

from .. import ConductorScheme
from .filehandlers import FileUrlHandler
from .ftphandlers import (FtpUrlHandler, SftpUrlHandler)
from .httphandlers import HttpUrlHandler


logger = logging.getLogger(__name__)


class UrlHandlerFactory(object):
    """
    A factory for creating URL handlers
    """

    @staticmethod
    def get_handler(scheme):
        result = {
            ConductorScheme.FILE: FileUrlHandler,
            ConductorScheme.FTP: FtpUrlHandler,
            ConductorScheme.SFTP: SftpUrlHandler,
            ConductorScheme.HTTP: HttpUrlHandler,
        }.get(scheme)
        return result()


url_handler_factory = UrlHandlerFactory()
