"""
Unit tests for conductor's servers module
"""

import logging
import mock
from nose import tools

import conductor.servers
import conductor.urlhandlers
import conductor.resources
from conductor.urlparser import Url

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__file__)


class TestServer(object):

    @classmethod
    def setup_class(cls):
        cls.server_name = "fake name"

    @mock.patch("conductor.urlhandlers.FileUrlHandler", autospec=True)
    @mock.patch("conductor.resources.Resource", autospec=True)
    def test_get_representation(self, mock_resource, mock_file_handler):
        """A resource's representation is searched correctly."""
        params = {
            "authorization": None,
            "media_type": None,
            "url": Url.from_string("file:///some/fake/file/url"),
        }
        dst = "fake/destination/directory"
        fake_returned_data = "/fake/returned/data"
        mocked_resource = mock_resource.return_value
        mocked_file_handler = mock_file_handler.return_value
        mocked_resource.show_get_parameters.return_value = [params]
        mocked_file_handler.get_from_url.return_value = fake_returned_data
        server = conductor.servers.Server(self.server_name)
        representation = server.get_representation(mocked_resource, dst)
        mocked_file_handler.get_from_url.assert_called_with(params["url"], dst)
        tools.eq_(representation, fake_returned_data)

    def test_post_representation(self):
        raise NotImplementedError
