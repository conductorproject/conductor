"""
Unit tests for conductor's collections module
"""

from nose.tools import eq_

from conductor import collections

class TestCollection(object):

    def test_collection_creation(self):
        """Collections are created with or without a long name."""
        short_name = "fake"
        c1 = collections.Collection(short_name)
        eq_(c1.short_name, short_name)
        eq_(c1.name, short_name)
        long_name = "a fake name"
        c2 = collections.Collection(short_name, name=long_name)
        eq_(c2.short_name, short_name)
        eq_(c2.name, long_name)

