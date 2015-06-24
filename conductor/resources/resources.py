"""
Resource class for conductor
"""

import re
import os
import logging
import pytz
import datetime
import dateutil.parser

from .. import ServerSchemeMethod
from .. import TemporalSelectionRule
from .. import ParameterSelectionRule
from .. import errors
from ..servers import server_factory
from ..collections import collection_factory
from ..settings import settings
from ..urlhandlers import url_handler_factory
from . import resourcelocations

logger = logging.getLogger(__name__)


class ResourceFactory(object):

    def get_resource(self, name, timeslot=None):
        try:
            s = [i for i in settings.resources if i["name"] == name][0]
        except IndexError:
            raise errors.ResourceNotDefinedError(
                "resource {!r} is not defined in the settings".format(name))
        collection = None
        if s.get("collection") is not None:
            collection = collection_factory.get_collection(s["collection"])
        params = dict()
        for p in s.get("parameters", dict()):
            params[p["name"]] = p.get("value")
        r = Resource(name, s["urn"], s["local_pattern"], collection=collection,
                     timeslot=timeslot, parameters=params)
        for name, member in ServerSchemeMethod.__members__.items():
            key_name = "{}_locations".format(name.lower())
            locs = self._parse_resource_locations(s.get(key_name, []), member)
            for loc in locs:
                r.add_location(loc, member)
                loc.parent = r
        return r

    @staticmethod
    def _parse_resource_locations(locations, mover_method):
        result = []
        for loc in locations:
            try:
                server = server_factory.get_server(loc["server"])
                schemes_to_check = {
                    ServerSchemeMethod.GET: server.schemes_get,
                    ServerSchemeMethod.FIND: server.schemes_get,
                    ServerSchemeMethod.POST: server.schemes_post,
                }[mover_method]
                scheme_config = [s for s in schemes_to_check if
                                 s.scheme.name == loc["scheme"].upper()][0]
                scheme = scheme_config.scheme
                relative_paths = loc["relative_paths"]
                authorization = loc.get("authorization")
                media_type = loc.get("media_type")
                if mover_method == ServerSchemeMethod.FIND:
                    temporal_rule = TemporalSelectionRule[
                        loc.get("temporal_rule", "latest").upper()]
                    lock_timeslot = loc.get("lock_timeslot", [])
                    parameter_rule = ParameterSelectionRule[
                        loc.get("parameter_rule", "highest").upper()]
                    parameter = loc.get("parameter")
                    rl = resourcelocations.ResourceLocationFind(
                        relative_paths, media_type, server=server,
                        scheme=scheme, authorization=authorization,
                        temporal_rule=temporal_rule,
                        lock_timeslot=lock_timeslot,
                        parameter_rule=parameter_rule,
                        parameter=parameter
                    )
                else:
                    rl = resourcelocations.ResourceLocation(
                        relative_paths, media_type, server=server,
                        scheme=scheme, authorization=authorization,
                        location_for=mover_method
                    )
                result.append(rl)
            except IndexError:
                logger.warning("resource location uses undefined scheme: "
                               "{!r}. Ignoring...".format(loc["scheme"]))
        return result


resource_factory = ResourceFactory()


class Resource(object):
    """
    A resource represents an object that can be retrieved and operated upon.

    A resource has a URN that is used to uniquely identify it. The resource
    may be available at multiple URLs. Each URL is constructed by providing:

    * information on the ConductorServer that can be used to retrieve the
      resource.
    * the relative URL path where the server will look for
    * any query parameters that should be used to build each URL
    * any hash parameters that should be used to build each URL

    A resource can also be posted to multiple URLs.
    """

    parameters = dict()
    _name = u""
    _urn = u""
    _timeslot = None
    _get_locations = []
    _post_locations = []
    _find_locations = []
    _local_pattern = u""

    @property
    def timeslot(self):
        return self._timeslot

    @timeslot.setter
    def timeslot(self, ts):
        if isinstance(ts, datetime.datetime):
            self._timeslot = ts
        else:
            try:
                self._timeslot = dateutil.parser.parse(ts)
            except AttributeError:
                logger.error("invalid value for timeslot: {}".format(ts))
                raise

    @property
    def timeslot_string(self):
        if self.timeslot is not None:
            result = self.timeslot.strftime("%Y%m%d%H%M")
        else:
            result = ""
        return result

    @property
    def dekade(self):
        result = None
        if self.timeslot is not None:
            day = self.timeslot.day
            result = 1 if day < 11 else (2 if day < 21 else 3)
        return result

    @property
    def year_day(self):
        if self.timeslot is not None:
            result = self.timeslot.timetuple().tm_yday
        else:
            result = None
        return result

    @property
    def name(self):
        return self._name.format(self)

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def safe_name(self):
        return self.name.replace(" ", "_")

    @property
    def urn(self):
        return self._urn.format(self)

    @urn.setter
    def urn(self, urn):
        self._urn = urn

    @property
    def local_pattern(self):
        return self._local_pattern.format(self)

    @local_pattern.setter
    def local_pattern(self, pattern):
        self._local_pattern = pattern

    def __init__(self, name, urn, local_pattern, collection=None,
                 timeslot=None, parameters=None):
        self.parameters = parameters.copy() if parameters else dict()
        self.collection = collection
        self._name = name
        self._urn = urn
        self._local_pattern = local_pattern
        self.timeslot = (timeslot if timeslot is not None
                         else datetime.datetime.now(pytz.utc))
        self._get_locations = []
        self._post_locations = []
        self._find_locations = []

    def __repr__(self):
        return ("{0}.{1.__class__.__name__}({1.name!r}, {1.urn!r}, "
                "collection={1.collection!r}, "
                "timeslot={1.timeslot!r})".format(__name__, self))

    def __str__(self):
        return self.urn

    def add_location(self, resource_location, location_type):
        group = {
            ServerSchemeMethod.GET: self._get_locations,
            ServerSchemeMethod.POST: self._post_locations,
            ServerSchemeMethod.FIND: self._find_locations,
        }[location_type]
        group.append(resource_location)

    def get_representation(self, destination_directory):
        """
        Get a resource's representation.

        This method iterates through all of the available resource_locations
        for a resource and tries to fetch the resource's representation using
        the URLs defined in each resource_location. It stops at the first
        successful URL retrieval.
        """

        representation = None
        i = 0
        while i < len(self._get_locations) and representation is None:
            rl = self._get_locations[i]
            urls = rl.create_urls()
            j = 0
            while j < len(urls) and representation is None:
                u = urls[j]
                logger.debug("Trying URL: {}".format(u.url))
                handler = url_handler_factory.get_handler(u.scheme)
                try:
                    representation = handler.get_from_url(
                        u, destination_directory)
                    logger.debug("found resource")
                except errors.ResourceNotFoundError:
                    logger.debug("did not find resource")
                j += 1
            i += 1
        return representation

    def post_representation(self, representation, post_to=None):
        """
        Post the input representation.

        This method sends the input representation to the servers defined in
        the instance's
        :param representation:
        :param post_to:
        :type post_to: [ResourceLocation]
        :return:
        """

        post_to = post_to if post_to else self._post_locations
        posted_to = []
        for rl in post_to:
            urls = rl.create_urls()
            for u in urls:
                handler = url_handler_factory.get_handler(u.scheme)
                logger.debug("Posting to: {}".format(u.url))
                try:
                    posted_to.append(handler.post_to_url(u, representation))
                except (errors.ResourceNotFoundError,
                        errors.LocalPathNotFoundError):
                    logger.error("Could not post to {}".format(u.url))
        return posted_to

    def find(self):
        """
        Find the information that is needed in order to GET this resource.

        This method will update a resource's timeslot and parameters in place.

        This method will look for the timeslot and parameters that this
        resource needs in order to be retrievable. This method applies to
        resources for which it is not possible to build the appropriate get
        structures.

        :return:
        """

        found_info = None
        i = 0
        while not found_info and i < len(self._find_locations):
            rl = self._find_locations[i]
            j = 0
            urls = rl.create_urls()
            while not found_info and j < len(urls):
                url = urls[j]
                url.parent = None  # to access the format marks on the urls
                handler = url_handler_factory.get_handler(url.scheme)
                logger.debug("Trying to find in: {}".format(url.url))
                found_info = handler.find_resource_info(
                    url, self,
                    lock_timeslot=rl.lock_timeslot,
                    parameter=rl.parameter,
                    temporal_rule=rl.temporal_rule,
                    parameter_rule=rl.parameter_rule
                )
                j += 1
            i += 1
        result = False
        if found_info:
            slot, params = found_info
            self.timeslot = slot
            self.parameters.update(params)
            result = True
        return result

    def find_local(self, path):
        """
        Find the file that matches this resource in the local filesystem.

        :param path:
        :return:
        """

        result = None
        if (os.path.isfile(path) and re.search(self.local_pattern, path)):
            result = path
        elif os.path.isdir(path):
            for i in os.listdir(path):
                i_path = os.path.join(path, i)
                if os.path.isfile(i_path) and re.search(self.local_pattern, i):
                    result = i_path
        return result

    def extract_path_parameters(self, path):
        """
        Extract an instance's parameters from an input path
        """

        sanitized =  re.sub(r"(\{0\.(?!parameters).*?\})", r".*?",
                            self._local_pattern)
        parameters = dict()
        for k, v in self.parameters.iteritems():
            re_pattern = re.sub(r"\{{0\.parameters\[{}\].*?\}}".format(k),
                                r"(?P<{}>.*?)".format(k),
                                sanitized)
            try:
                found_dict = re.search(re_pattern, path).groupdict()
                parameters.update(found_dict)
            except AttributeError:
                pass  # this parameter is not used in the path
        return parameters
