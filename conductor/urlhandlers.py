"""
Handlers that take care of GETting and POSTing resources to and from URLs
"""

import os
import os.path
import shutil
import logging

from ftputil import FTPHost
import ftputil.error

from . import ConductorScheme
from . import errors
from . import urlparser

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
            ConductorScheme.SFTP: SftpUrlHandler,
            ConductorScheme.HTTP: HttpUrlHandler,
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

    def get_from_url(self, url, destination_directory):
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
        :raises: conductor.errors.ResourceNotFoundError
        """

        self.create_local_directory(destination_directory)
        path = url.path_part
        destination = os.path.join(destination_directory,
                                   os.path.basename(path))
        try:
            shutil.copyfile(path, destination)
        except IOError as err:
            raise errors.ResourceNotFoundError(err.args)
        return destination

    def post_to_url(self, url, path):
        """
        Send a file to the input URL.

        The input URL is expected to use the FILE scheme and its path_part
        must end with the directory where the path will be copied into.

        :param url:
        :param path:
        :return:
        """

        destination = os.path.join(url.path_part, os.path.basename(path))
        try:
            if not os.path.isdir(url.path_part):
                os.makedirs(url.path_part)
            shutil.copyfile(path, destination)
        except (OSError, IOError) as err:
            err_no, msg = err.args
            if err_no == 2:
                raise errors.LocalPathNotFoundError(msg)
            elif err_no == 13:
                raise errors.ResourceNotFoundError(msg)
            else:
                raise
        return destination


class FtpUrlHandler(BaseUrlHandler):

    def get_from_url(self, url, destination_directory):
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
        :raises: conductor.errors.ResourceNotFoundError,
            conductor.errors.InvalidUserCredentialsError,
            conductor.errors.HostNotFoundError
        """

        self.create_local_directory(destination_directory)
        path = url.path_part
        destination = os.path.join(destination_directory,
                                   os.path.basename(path))
        try:
            with FTPHost(url.host_name, url.user_name, url.user_password) as h:
                h.download(url.path_part, destination)
        except ftputil.error.PermanentError as err:
            if err.errno == 530:
                raise errors.InvalidUserCredentialsError(err.args)
            elif err.errno == 550:
                raise errors.ResourceNotFoundError(err.args)
            else:
                raise
        except ftputil.error.FTPOSError as err:
            code, msg = err.args
            if code == -2:
                logger.error("Server {} not found: {}".format(
                    url.host_name, msg))
                raise errors.HostNotFoundError(
                    "Server {} not found".format(url.host_name))
            raise
        except ftputil.error.FTPIOError as err:
            raise errors.ResourceNotFoundError(err.args)
        return destination

    def post_to_url(self, url, path):
        destination = os.path.join(url.path_part, os.path.basename(path))
        try:
            with FTPHost(url.host_name, url.user_name, url.user_password) as h:
                destination_dir = os.path.dirname(destination)
                if not h.path.isdir(destination_dir):
                    h.makedirs(destination_dir)
                h.upload(path, destination)
        except ftputil.error.FTPIOError as err:
            raise errors.ResourceNotFoundError(err.args)
        except ftputil.error.PermanentError as err:
            if err.errno == 550:
                raise errors.ResourceNotFoundError(err.args)
            else:
                raise
        except IOError as err:
            err_no, msg = err.args
            if err_no == 2:
                raise errors.LocalPathNotFoundError(msg)
        return destination


class SftpUrlHandler(BaseUrlHandler):
    pass


class HttpUrlHandler(BaseUrlHandler):
    pass
