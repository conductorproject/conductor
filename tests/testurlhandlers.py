"""
Unit tests for conductor's urlhandlers module
"""

import os

from nose.tools import eq_, assert_is_instance, assert_false
import mock

import conductor.urlhandlers
from conductor import ConductorScheme
import conductor.urlparser


class TestUrlHandlerFactory(object):

    @classmethod
    def setup_class(cls):
        cls.factory = conductor.urlhandlers.url_handler_factory

    def test_get_instances(self):
        """
        UrlHandlerFactory creates correct UrlHandlers for each scheme.
        """

        for name, scheme in ConductorScheme.__members__.items():
            h = self.factory.get_handler(scheme)
            assert_is_instance(h, conductor.urlhandlers.BaseUrlHandler)


class TestBaseUrlHandler(object):

    @classmethod
    def setup_class(cls):
        cls.bogus_directory_path = "/home/fake_user/some_directory"

    @mock.patch("conductor.urlhandlers.os.path")
    @mock.patch("conductor.urlhandlers.os")
    def test_create_local_directory(self, mock_os, mock_path):
        """Local directories are correctly created."""

        # test that if the directory exists we do not try to create it
        mock_path.isdir.return_value = True
        conductor.urlhandlers.BaseUrlHandler.create_local_directory(
            self.bogus_directory_path)
        mock_path.isdir.assert_called_with(self.bogus_directory_path)
        assert_false(mock_os.makedirs.called)
        # now test that if the directory does not exist we try to create it
        mock_path.isdir.return_value = False
        conductor.urlhandlers.BaseUrlHandler.create_local_directory(
            self.bogus_directory_path)
        mock_os.makedirs.assert_called_with(self.bogus_directory_path)


class TestFileUrlHandler(object):

    @classmethod
    def setup_class(cls):
        cls.handler = conductor.urlhandlers.url_handler_factory.get_handler(
            ConductorScheme.FILE)
        cls.bogus_url = conductor.urlparser.Url.from_string(
            "file:///fake/path")
        cls.bogus_get_destination_directory = "/home/fake/destination"

    @mock.patch.object(conductor.urlhandlers.FileUrlHandler,
                       "create_local_directory")
    @mock.patch("conductor.urlhandlers.shutil")
    def test_get_from_url(self, mock_shutil, mock_create_local_dir):
        """URLs are correctly GET from the local filesystem."""

        mock_create_local_dir.return_value = True
        result = self.handler.get_from_url(
            self.bogus_url, self.bogus_get_destination_directory)
        self.handler.create_local_directory.assert_called_with(
            self.bogus_get_destination_directory)
        mock_shutil.copyfile.assert_called_with(self.bogus_url.path_part,
                                                result)

    @mock.patch("conductor.urlhandlers.os.path")
    @mock.patch("conductor.urlhandlers.os")
    @mock.patch("conductor.urlhandlers.shutil")
    def test_post_to_url(self, mock_shutil, mock_os, mock_os_path):
        """Files are correctly POSTed to the local filesystem."""

        path = "/fake/path"
        mock_os_path.isdir.return_value = True
        result = self.handler.post_to_url(self.bogus_url, path)
        mock_shutil.copyfile.assert_called_with(path, result)
        mock_os_path.isdir.return_value = False
        result = self.handler.post_to_url(self.bogus_url, path)
        mock_os.makedirs.assert_called_with(self.bogus_url.path_part)
        mock_shutil.copyfile.assert_called_with(path, result)


class TestFtpUrlHandler(object):

    @classmethod
    def setup_class(cls):
        cls.handler = conductor.urlhandlers.url_handler_factory.get_handler(
            ConductorScheme.FTP)
        cls.bogus_url = conductor.urlparser.Url.from_string(
            "ftp://some_user:some_pass@fake_host/dir/subdir/fake_file")
        cls.bogus_get_destination_directory = "/home/fake/destination"

    @mock.patch.object(conductor.urlhandlers.FtpUrlHandler,
                       "create_local_directory")
    @mock.patch("conductor.urlhandlers.FTPHost", autospec=True)
    def test_get_from_url(self, mock_ftp_host_constructor,
                          mock_create_local_dir):
        """URLs are correctly GET from an FTP server."""

        mock_ftp_host = \
            mock_ftp_host_constructor.return_value.__enter__.return_value
        mock_create_local_dir.return_value = True
        result = self.handler.get_from_url(
            self.bogus_url, self.bogus_get_destination_directory)
        self.handler.create_local_directory.assert_called_with(
            self.bogus_get_destination_directory)
        mock_ftp_host_constructor.assert_called_with(
            self.bogus_url.host_name, self.bogus_url.user_name,
            self.bogus_url.user_password
        )
        mock_ftp_host.download.assert_called_with(
            self.bogus_url.path_part, result)

    @mock.patch("conductor.urlhandlers.FTPHost", autospec=True)
    def test_post_to_url(self, mock_ftp_host_constructor):
        """Files are correctly POSTed to an FTP server."""
        mock_ftp_host = \
            mock_ftp_host_constructor.return_value.__enter__.return_value
        path = "/some/fake/path"
        mock_ftp_host.path.isdir.return_value = True
        result = self.handler.post_to_url(self.bogus_url, path)
        mock_ftp_host_constructor.assert_called_with(
            self.bogus_url.host_name, self.bogus_url.user_name,
            self.bogus_url.user_password
        )
        mock_ftp_host.upload.assert_called_with(path, result)
        # now lets test creation of a remote dir
        mock_ftp_host.path.isdir.return_value = False
        result = self.handler.post_to_url(self.bogus_url, path)
        mock_ftp_host.makedirs.assert_called_with(os.path.dirname(result))
        mock_ftp_host.upload.assert_called_with(path, result)
