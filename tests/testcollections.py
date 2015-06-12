"""
Unit tests for conductor's collections module
"""

import logging

import mock
from nose import tools
from conductor import collections
from conductor.settings import settings
from conductor import errors

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__file__)


class TestCollectionFactory(object):

    @mock.patch("conductor.collections.Collection", autospec=True)
    def test_get_collection(self, mock_collection):
        """The Collection factory is able to create new Collection instances"""
        valid_name = "fake_valid_name"
        settings.collections = [{"short_name": valid_name}]
        factory = collections.collection_factory
        factory.get_collection(valid_name)
        tools.assert_true(mock_collection.called)
        invalid_name = "fake_invalid_name"
        tools.assert_raises(errors.CollectionNotDefinedError,
                            factory.get_collection, invalid_name)


class TestCollection(object):

    def test_collection_creation(self):
        """Collections are created with or without a long name."""
        short_name = "fake"
        c1 = collections.Collection(short_name)
        tools.eq_(c1.short_name, short_name)
        tools.eq_(c1.name, short_name)
        long_name = "a fake name"
        c2 = collections.Collection(short_name, name=long_name)
        tools.eq_(c2.short_name, short_name)
        tools.eq_(c2.name, long_name)

