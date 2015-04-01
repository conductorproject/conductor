"""
Unit tests for conductor's fileresource module
"""

import os
from datetime import datetime
from calendar import monthrange

from nose.tools import eq_
from nose.plugins.skip import SkipTest
import mock

import conductor.fileresource
from conductor.resourcemover import LocalMover

class TestResourceSearchPath(object):

    @classmethod
    def setup_class(cls):
        cls.path_pattern = "dummy/path"
        cls.resource_movers = [LocalMover()]
        cls.search_path = conductor.fileresource.ResourceSearchPath(
            cls.path_pattern, remote_movers=cls.resource_movers)

    @mock.patch(conductor.fileresource.FileResource)
    @mock.patch.object(LocalMover, "find")
    def test_find_in_remotes(self, mock_find, mock_file_resource):
        ts = datetime(2015, 1, 1)
        mock_file_resource.timeslot = ts
        p = "dummy_pattern_{0.year}_{0.year_day:03d}"
        mock_file_resource.search_pattern = (p)
        mover = LocalMover()
        mover.data_dirs = ["/fake/home"]
        self.search_path.remote_movers = [mover]
        patt = p.replace("{0.year}", ts.strftime("%Y"))
        patt = p.replace("{0.year_day:03d}", monthrange(ts.year, ts.month)[1])
        expected_pattern = os.path.join(self.path_pattern, patt)
        in_mover, found = self.search_path.find_in_remotes(mock_file_resource)
        mock_find.return_value = expected_pattern
        mock_find.assert_called_with(expected_pattern)
        eq_(mover, in_mover)
        eq_(found, expected_pattern)





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
