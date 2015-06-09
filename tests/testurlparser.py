"""
Unit tests for conductor's urlparser module
"""

from nose.tools import eq_

import conductor.urlparser

class TestUrl(object):

    @classmethod
    def setup_class(cls):
        cls.file_url_string = "file:///directory/another/some_file"
        cls.file_path_string = "/some_directory/some_file"
        cls.ftp_url_string = "ftp://user:pass@host/some/path/file_pattern"

    def test_from_string_file(self):
        """
        URLs with FILE scheme are correctly parsed from a string
        """

        u1 = conductor.urlparser.Url.from_string(self.file_url_string)
        eq_(u1.url, self.file_url_string)
        u2 = conductor.urlparser.Url.from_string(self.file_path_string)
        eq_(u2.path_part, self.file_path_string)

    def test_from_string_ftp(self):
        """
        URLs with FTP scheme are correctly parsed from a string
        """

        u = conductor.urlparser.Url.from_string(self.ftp_url_string)
        eq_(u.url, self.ftp_url_string)

