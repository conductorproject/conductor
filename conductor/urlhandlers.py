"""
Handlers that take care of GETting and POSTing resources to and from URLs
"""

import os
import os.path
import shutil
import logging

from ftputil import FTPHost
import ftputil.error

from conductor import ConductorScheme
import conductor.errors
import conductor.urlparser

logger = logging.getLogger(__file__)


class UrlHandlerFactory(object):
    """
    A factory for creating URL handlers
    """

    @staticmethod
    def get_handler(scheme):
        result = {
            ConductorScheme.FILE: FileUrlHandler,
            ConductorScheme.FTP: FtpUrlHandler,
        }.get(scheme)
        return result()


url_handler_factory = UrlHandlerFactory()


class BaseUrlHandler(object):

    @staticmethod
    def create_local_directory(path):
        if not os.path.isdir(path):
            os.makedirs(path)

    def __repr__(self):
        return "{0}.{1.__class__.__name__}()".format(__name__, self)


class FileUrlHandler(BaseUrlHandler):

    def get_url(self, url, destination_directory):
        """
        Get the representation of the resource available at the input URL

        Copy the input url into the destination directory.

        :arg url: The URL to be searched
        :type url: conductor.urlparser.Url
        :arg destination_directory: Directory where the resource's
            representation will be saved into. It must exist.
        :type destination_directory: str
        :return: The full path to the representation that was retrieved
            from the input URL
        :rtype: str
        :raises: `conductor.errors.ResourceNotFoundError`
        """

        self.create_local_directory(destination_directory)
        path = url.path_part
        destination = os.path.join(destination_directory,
                                   os.path.basename(path))
        try:
            shutil.copyfile(path, destination)
        except IOError as err:
            raise conductor.errors.ResourceNotFoundError(err.args)
        return destination


class FtpUrlHandler(BaseUrlHandler):

    def get_url(self, url, destination_directory):
        """
        Get the representation of the resource available at the input URL

        Copy the input url into the destination directory.

        :arg url: The URL to be searched
        :type url: conductor.urlparser.Url
        :arg destination_directory: Directory where the resource's
            representation will be saved into. It must exist.
        :type destination_directory: str
        :return: The full path to the representation that was retrieved
            from the input URL
        :rtype: str
        :raises: `conductor.errors.ResourceNotFoundError`
        """

        self.create_local_directory(destination_directory)
        path = url.path_part
        destination = os.path.join(destination_directory,
                                   os.path.basename(path))
        try:
            with FTPHost(url.host_name, url.user_name, url.user_password) as h:
                h.download(url.path_part, destination)
        except ftputil.error.PermanentError as err:
            error_code, sep, msg = err.args[0].partition(" ")
            if int(error_code) == 530:
                raise conductor.errors.InvalidUserCredentialsError(err.args)
            elif int(error_code) == 550:
                raise conductor.errors.ResourceNotFoundError(err.args)
        except ftputil.error.FTPOSError as err:
            code, msg = err.args
            if code == -2:
                logger.error("Server {} not found: {}".format(
                    url.host_name, msg))
                raise conductor.errors.HostNotFoundError(
                    "Server {} not found".format(url.host_name))
            raise
