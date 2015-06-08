"""
Unit tests for conductor's urlhandlers module
"""

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
        UrlHandlerFactory creates correct UrlHandlers for each scheme
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
        """
        Local directories are correctly created.

        :param mock_os:
        :param mock_path:
        :return:
        """

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
        cls.bogus_url_string = "file:///fake/path"
        cls.bogus_get_destination_directory = "/home/fake/destination"

    @mock.patch.object(conductor.urlhandlers.FileUrlHandler,
                       "create_local_directory")
    @mock.patch("conductor.urlhandlers.shutil")
    def test_get_url(self, mock_shutil, mock_create_local_dir):
        """
        URLs are correctly GET.

        :param mock_shutil:
        :param mock_create_local_dir:
        :return:
        """

        url = conductor.urlparser.Url.from_string(self.bogus_url_string)
        mock_create_local_dir.return_value = True
        result = self.handler.get_url(url, self.bogus_get_destination_directory)
        self.handler.create_local_directory.assert_called_with(
            self.bogus_get_destination_directory)
        mock_shutil.copyfile.assert_called_with(url.path_part, result)
