"""
Unit tests for conductor's resourcemover module
"""

import os

from nose.tools import eq_
from nose.plugins.skip import SkipTest

import conductor.resourcemover


class TestResourceMover(object):

    @classmethod
    def setup_class(cls):
        cls.data_dirs = [
            "/fake/dir",
            "/fake/another/dir"
        ]
        cls.resource_mover = conductor.resourcemover.ResourceMover(name="test")
        cls.resource_mover.data_dirs = cls.data_dirs

    def test_prepare_find(self):
        first_pattern = "relative/dummy"
        second_pattern = "/absolute/dummy"
        first_prepared = self.resource_mover._prepare_find(first_pattern)
        first_expected = [os.path.split(os.path.join(d, first_pattern)) for
                          d in self.resource_mover.data_dirs]
        eq_(first_prepared, first_expected)
        second_prepared = self.resource_mover._prepare_find(second_pattern)
        second_expected = [os.path.split(second_pattern)]
        eq_(second_prepared, second_expected)


class TestLocalMover(object):

    @classmethod
    def setup_class(cls):
        cls.data_dirs = [
            "/fake/dir",
            "/fake/another/dir"
        ]
        cls.find_patterns = [
            "fake_pattern_.*_stuff",
            "other_fake_pattern_.*_stuff"
        ]
        cls.resource_mover = conductor.resourcemover.LocalMover(name="test")
        cls.resource_mover.data_dirs = cls.data_dirs

    def test_find(self):
        raise SkipTest
