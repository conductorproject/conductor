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
        cls.timeslot = datetime(2015, 1, 1)
        cls.path_pattern = "dummy/path"
        cls.file_resource_search_pattern = "dummy_pattern_{0.year}_{0.year_day:03d}"
        cls.resource_movers = [LocalMover()]
        cls.search_path = conductor.fileresource.ResourceSearchPath(
            cls.path_pattern, remote_movers=cls.resource_movers)
        cls.search_path.remote_movers = [
            LocalMover(data_dirs=["/fake/root"])
        ]

    @mock.patch("conductor.fileresource.FileResource")
    @mock.patch.object(LocalMover, "find")
    def test_find_in_remotes(self, mock_find, mock_file_resource):
        expected_pattern = os.path.join(self.path_pattern,
                                        self.file_resource_search_pattern)
        mock_file_resource.timeslot = self.timeslot
        mock_file_resource.search_pattern = self.file_resource_search_pattern
        patt = self.file_resource_search_pattern.replace(
            "{0.year}", self.timeslot.strftime("%Y"))
        patt = patt.replace("{0.year_day:03d}", "{:03d}".format(
            monthrange(self.timeslot.year, self.timeslot.month)[1]))
        expected_found = [
            os.path.join(self.search_path.remote_movers[0].data_dirs[0],
                         self.path_pattern, patt)
        ]
        mock_find.return_value = expected_found
        in_mover, found = self.search_path.find_in_remotes(mock_file_resource)
        mock_find.assert_called_with(expected_pattern)
        eq_(self.search_path.remote_movers[0], in_mover)
        eq_(found, expected_found)


class TestFileResource(object):

    @classmethod
    def setup_class(cls):
        cls.timeslot = datetime(2015, 12, 10)
        cls.mover = LocalMover(data_dirs=["/fake/root"])
        cls.search_pattern = "dummy_pattern_{0.year}_{0.year_day:03d}"
        cls.search_paths = [
            "fake/path/{0.month:02d}",
            "another/fake/path/{0.year}/{0.month:02d}"
        ]
        cls.file_resource = conductor.fileresource.FileResource("dummy")

    def setup(self):
        self.file_resource.timeslot = self.timeslot
        self.file_resource.search_pattern = self.search_pattern
        for p in self.search_paths:
            self.file_resource.add_search_path(p, remote_movers=[self.mover])

    def teardown(self):
        self.file_resource.timeslot = None
        self.file_resource._search_paths = []
        self.file_resource.search_pattern = ""

    def test_timeslot(self):
        """Timeslot and related properties have correct values"""
        for day in range(1, 25, 5):  # valid days for every month
            ts = datetime(self.timeslot.year, self.timeslot.month, day)
            self.file_resource.timeslot = ts
            eq_(self.file_resource.timeslot, ts)
            eq_(self.file_resource.year, ts.year)
            eq_(self.file_resource.month, ts.month)
            eq_(self.file_resource.day, ts.day)
            eq_(self.file_resource.hour, ts.hour)
            eq_(self.file_resource.minute, ts.minute)
            eq_(self.file_resource.year_day, ts.timetuple().tm_yday)
            eq_(self.file_resource.dekade,
                1 if ts.day < 11 else (2 if ts.day < 21 else 3))
        self.file_resource.timeslot = None
        eq_(self.file_resource.year, None)
        eq_(self.file_resource.month, None)
        eq_(self.file_resource.day, None)
        eq_(self.file_resource.hour, None)
        eq_(self.file_resource.minute, None)
        eq_(self.file_resource.year_day, None)
        eq_(self.file_resource.dekade, None)

    def test_file_pattern_substitution(self):
        """Substitutions on the file pattern are correct"""
        expected = self.search_pattern.replace("{0.year}",
                                               self.timeslot.strftime("%Y"))
        expected = expected.replace(
            "{0.year_day:03d}",
            "{:03d}".format(self.timeslot.timetuple().tm_yday)
        )
        eq_(self.file_resource.search_pattern, expected)

    @mock.patch.object(LocalMover, "find")
    def test_find_local(self, mock_find):
        found_paths = self.file_resource.find()
        for p in self.search_paths:
            expected_pattern = self.search_pattern.replace("")
            full_pattern = os.path.join(p, self.search_pattern)
            assert mock_find.assert_called_with(full_pattern)

    def test_find_predefined_remote_movers(self):
        raise SkipTest

    def test_find_additional_movers(self):
        raise SkipTest
