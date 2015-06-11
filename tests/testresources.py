"""
Unit tests for conductor's resources module
"""

import datetime
import pytz

from nose import tools
import mock

from conductor import ConductorScheme
import conductor.resources
import conductor.collections

class TestResourceFinderFactory(object):

    @classmethod
    def setup_class(cls):
        cls.factory = conductor.resources.resource_finder_factory

    def test_get_finders(self):
        for name, scheme in ConductorScheme.__members__.items():
            f = self.factory.get_finder(scheme)
            tools.assert_is_instance(f,
                                     conductor.resources.BaseResourceFinder)


class TestResource(object):

    @classmethod
    def setup_class(cls):
        cls.resource_name = "fake name"
        cls.resource_urn = ("urn:fake:{0.collection.short_name}:"
                            "{0.timeslot_string}")
        cls.resource_collection = None
        cls.resource_timeslot = datetime.datetime.now(pytz.utc)

    def test_properties(self):
        r = conductor.resources.Resource(self.resource_name,
                                         self.resource_urn,
                                         collection=None,
                                         timeslot=None)
        tools.assert_is_instance(r.timeslot, datetime.datetime)
        tools.assert_raises(AttributeError, r.__getattribute__, "urn")
        tools.assert_is_instance(r.timeslot, datetime.datetime)
        #set the timeslot
        r.timeslot = self.resource_timeslot
        tools.eq_(r.timeslot, self.resource_timeslot)
        tools.eq_(r.timeslot_string,
                  self.resource_timeslot.strftime("%Y%m%d%H%M"))
        # set the collection
        col_name = "mock_collection"
        mocked_collection = mock.create_autospec(
            conductor.collections.Collection)
        mocked_collection.short_name.return_value = col_name
        r.collection = mocked_collection
        tools.eq_(r.urn, self.resource_urn.format(r))


