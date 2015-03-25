"""
Unit tests for conductor's fileresource module
"""

from datetime import datetime

from nose.tools import eq_
from nose.plugins.skip import SkipTest

import conductor.fileresource

class TestFileResource(object):

    @classmethod
    def setup_class(cls):
        cls.timeslot = datetime(2015, 12, 10)
        cls.search_pattern = "dummy_pattern_{0.year}_{0.year_day:03d}"
        cls.search_paths = [
            "fake/path/{0.month:02d}",
            "another/fake/path/{0.year}/{0.month:02d}"
        ]
        cls.file_resource = conductor.fileresource.FileResource("dummy")

    def setup(self):
        self.file_resource.timeslot = self.timeslot
        self.file_resource.search_pattern = self.search_pattern
        self.file_resource.search_paths = self.search_paths

    def teardown(self):
        self.file_resource.timeslot = None
        self.file_resource.search_paths = []
        self.file_resource.search_pattern = ""

    def test_timeslot(self):
        """
        Timeslot and related properties have correct values
        """
        eq_(self.file_resource.timeslot, self.timeslot)
        eq_(self.file_resource.year, self.timeslot.year)
        eq_(self.file_resource.month, self.timeslot.month)
        eq_(self.file_resource.day, self.timeslot.day)
        eq_(self.file_resource.hour, self.timeslot.hour)
        eq_(self.file_resource.minute, self.timeslot.minute)
        eq_(self.file_resource.year_day, self.timeslot.timetuple().tm_yday)
        self.file_resource.timeslot = None
        eq_(self.file_resource.year, None)
        eq_(self.file_resource.month, None)
        eq_(self.file_resource.day, None)
        eq_(self.file_resource.hour, None)
        eq_(self.file_resource.minute, None)
        eq_(self.file_resource.year_day, None)

    def test_file_pattern_substitution(self):
        """
        The substitutions on the file pattern are correct
        """
        eq_(self.file_resource.search_pattern, "dummy_pattern_{}_{}".format(
            self.timeslot.year, self.timeslot.timetuple().tm_yday))

    def test_file_path_substitution(self):
        """
        The substitutions on the file paths are correct
        """
        eq_(self.file_resource.search_paths[0],
            "fake/path/{0.month:02d}".format(self.timeslot))

    def test_find_local(self):
        raise SkipTest

    def test_find_predefined_remote_movers(self):
        raise SkipTest

    def test_find_additional_movers(self):
        raise SkipTest
