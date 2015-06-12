"""
Resource class for conductor
"""

import re
import os
import logging
import pytz
import datetime
import dateutil.parser

from .urlparser import Url
from . import ConductorScheme
from . import ServerSchemeMethod
from . import errors
from .servers import server_factory
from .collections import collection_factory
from .settings import settings
from .urlhandlers import url_handler_factory

logger = logging.getLogger(__name__)


class ResourceFinderFactory(object):

    @staticmethod
    def get_finder(scheme):
        result = {
            ConductorScheme.FILE: FileResourceFinder,
            ConductorScheme.FTP: FtpResourceFinder,
        }.get(scheme)
        return result

resource_finder_factory = ResourceFinderFactory()


class BaseResourceFinder(object):
    """
    Look for Conductor resources

    This class is used in order to locate resources that are dynamic. Dynamic
    resources are the ones that are defined by conditions that are not
    possible to determine a priori. An example is a resource that represents
    the latest LST file that is available. We cannot know the timeslot of
    the latest LST product without actually running the code.

    This class can search the appropriate servers and determine the actual
    parameters in order to create proper Resource instances.
    """
    pass


class FileResourceFinder(BaseResourceFinder):

    @staticmethod
    def select_path(full_path_pattern, selection_method="latest",
                    except_paths=None):
        """
        Return the full path to an existing directory that meets search criteria

        This function accepts a pattern that is interpreted as being the
        specification for finding a real path on the filesystem.

        >>> server_base_path = "/home/geo2/test_data/giosystem/data"
        >>> relative_path = "OUTPUT_DATA/PRE_PROCESS/LRIT2HDF5_g2/DYNAMIC_OUTPUT/v2.4"
        >>> dynamic_part = "(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{2})"
        >>> path = os.path.join(server_base_path, relative_path, dynamic_part)
        >>> select_path(path, selection_method="latest")

        :param full_path_pattern:
        :param selection_method:
        :param except_paths:
        :return:
        """
        except_paths = except_paths if except_paths is not None else []
        base = full_path_pattern[:full_path_pattern.find("(")]
        dynamic = full_path_pattern[full_path_pattern.find("("):]
        dynamic_parts = dynamic.split("/")
        current_path = base
        if len(dynamic_parts) > 1:
            next_hierarchic_part = 0
            while 0 <= next_hierarchic_part < len(dynamic_parts):
                hierarchic_part = dynamic_parts[next_hierarchic_part]
                old_path = current_path
                candidates = []
                for c in os.listdir(current_path):
                    if os.path.isdir(os.path.join(current_path, c)):
                        re_obj = re.search(hierarchic_part, c)
                        if re_obj is not None:
                            candidates.append(c)
                sorted_candidates = sorted(candidates)
                if selection_method == "latest":
                    sorted_candidates.reverse()
                candidate_index = 0
                found = False
                while candidate_index < len(sorted_candidates) and not found:
                    current_path = os.path.join(
                        old_path, sorted_candidates[candidate_index])
                    found = True if current_path not in except_paths else False
                    candidate_index += 1
                if found:
                    next_hierarchic_part += 1
                else:
                    # cycle back to the previous hierarchic level
                    next_hierarchic_part -= 1
                    cycle_back_path = os.path.dirname(current_path)
                    except_paths.append(cycle_back_path)
                    current_path = os.path.dirname(cycle_back_path)
        else:
            current_path = current_path if current_path not in except_paths else None
        return current_path


class FtpResourceFinder(BaseResourceFinder):
    pass


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
        r = Resource(name, s["urn"], s["local_pattern"], collection=collection,
                     timeslot=timeslot)
        for loc_get in self._parse_resource_locations(
                s["get_locations"], ServerSchemeMethod.GET):
            r.add_get_location(*loc_get)
        for loc_post in self._parse_resource_locations(
                s["post_locations"], ServerSchemeMethod.POST):
            r.add_post_location(*loc_post)
        return r

    @staticmethod
    def _parse_resource_locations(locations, mover_method):
        result = []
        for loc in locations:
            try:
                server = server_factory.get_server(loc["server"])
                schemes_to_check = {
                    ServerSchemeMethod.GET: server.schemes_get,
                    ServerSchemeMethod.POST: server.schemes_post,
                }[mover_method]
                scheme_config = [s for s in schemes_to_check if
                                 s.scheme.name == loc["scheme"].upper()][0]
                scheme = scheme_config.scheme
                relative_paths = loc["relative_paths"]
                authorization = loc.get("authorization")
                media_type = loc["media_type"]
                result.append(
                    (server, scheme, relative_paths, authorization, media_type)
                )
            except IndexError:
                logger.warning("get location uses undefined scheme: {!r}. "
                               "Ignoring...".format(loc["scheme"]))
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

    _name = u""
    _urn = u""
    _timeslot = None
    _get_locations = []
    _post_locations = []
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
                 timeslot=None):
        self.collection = collection
        self._name = name
        self._urn = urn
        self._local_pattern = local_pattern
        self.timeslot = (timeslot if timeslot is not None
                         else datetime.datetime.now(pytz.utc))
        self._get_locations = []
        self._post_locations = []

    def __repr__(self):
        return ("{0}.{1.__class__.__name__}({1.name!r}, {1.urn!r}, "
                "collection={1.collection!r}, "
                "timeslot={1.timeslot!r})".format(__name__, self))

    def __str__(self):
        return self.urn

    def add_get_location(self, server, scheme, relative_paths, authorization,
                         media_type):
        if scheme in [config.scheme for config in server.schemes_get]:
            loc = {
                "server": server,
                "scheme": scheme,
                "relative_paths": relative_paths,
                "authorization": authorization,
                "media_type": media_type,
            }
            self._get_locations.append(loc)
        else:
            logger.error("Unsupported scheme {!r} for server {!r}. "
                         "Ignoring...".format(scheme, server))

    def add_post_location(self, server, scheme, relative_paths, authorization,
                          media_type):
        if scheme in [config.scheme for config in server.schemes_post]:
            loc = {
                "server": server,
                "scheme": scheme,
                "relative_paths": relative_paths,
                "authorization": authorization,
                "media_type": media_type,
            }
            self._post_locations.append(loc)
        else:
            raise errors.InvalidSchemeError("Unsupported scheme {} for server "
                                            "{}. Ignoring...".format(scheme, 
                                                                     server))

    def show_get_parameters(self):
        return self._show_mover_method_parameters(ServerSchemeMethod.GET)

    def show_post_parameters(self):
        return self._show_mover_method_parameters(ServerSchemeMethod.POST)

    def _show_mover_method_parameters(self, mover_method):
        try:
            attr = {
                ServerSchemeMethod.GET: self._get_locations,
                ServerSchemeMethod.POST: self._post_locations,
            }[mover_method]
        except KeyError:
            raise errors.InvalidMoverMethodError(
                "Invalid mover method {}".format(mover_method))
        method_parameters = []
        for p in attr:
            scheme_type = {
                ServerSchemeMethod.GET: p["server"].schemes_get,
                ServerSchemeMethod.POST: p["server"].schemes_post,
            }[mover_method]
            scheme_config = [s for s in scheme_type if
                             s.scheme == p["scheme"]][0]
            url_params = []
            for relative_path in p["relative_paths"]:
                query_params, dequeried = Url.extract_query_params(
                    relative_path)
                hash_part = Url.extract_hash_part(relative_path)[0]
                if relative_path.startswith("/"):
                    url_params.append((dequeried, query_params, hash_part))
                else:
                    for base_path in scheme_config.base_paths:
                        full_path = "/".join((base_path, dequeried))
                        url_params.append((full_path, query_params, hash_part))

            for path, query_params, hash_part in url_params:
                url = Url(scheme_config.scheme,
                                   host_name=p["server"].domain,
                                   port_number=scheme_config.port_number,
                                   user_name=scheme_config.user_name,
                                   user_password=scheme_config.user_password,
                                   path_part=path, hash_part=hash_part,
                                   parent=self, **query_params)
                method_parameters.append({
                    "url": url,
                    "authorization": p["authorization"],
                    "media_type": p["media_type"],
                })
        return method_parameters

    def get_representation(self, destination_directory):
        """
        Get a resource's representation.

        This method iterates through all of the available get_parameters for
        a resource and tries to fetch the resource's representation using
        the URLs defined in each get_parameter. It stops at the first
        successful URL retrieval.
        """

        representation = None
        get_params = self.show_get_parameters()
        i = 0
        while i < len(get_params) and representation is None:
            p = get_params[i]
            logger.debug("Trying URL: {}".format(p["url"].url))
            handler = url_handler_factory.get_handler(p["url"].scheme)
            try:
                representation = handler.get_from_url(p["url"],
                                                      destination_directory)
                logger.debug("found resource")
            except errors.ResourceNotFoundError:
                logger.debug("did not find resource")
            i += 1
        return representation

    def post_representation(self, representation, post_to=None):
        """
        Post the input representation.

        This method sends the input representation to the servers defined in
        the instance's
        :param representation:
        :param post_to:
        :return:
        """

        post_to = post_to if post_to else self.show_post_parameters()
        posted_to = []
        for p in post_to:
            handler = url_handler_factory.get_handler(p["url"].scheme)
            logger.debug("Posting to: {}".format(p["url"].url))
            try:
                posted_to.append(handler.post_to_url(p["url"], representation))
            except (errors.ResourceNotFoundError,
                    errors.LocalPathNotFoundError):
                logger.error("Could not post to {}".format(p["url"].url))
        return posted_to

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
