"""
Resource class for conductor





"""

import re
import os
import logging
import pytz
import datetime
import dateutil.parser

from urlparser import Url
from conductor import ConductorScheme

logger = logging.getLogger(__name__)


class ResourceFinderFactory(object):

    @staticmethod
    def get_finder(self, scheme):
        result = {
            ConductorScheme.FILE: FileResourceFinder,
            ConductorScheme.FTP: FtpResourceFinder,
        }.get(scheme)
        return result

resource_finder_factory = ResourceFinderFactory()


class ResourceFinder(object):
    """
    Look for Conductor resources

    This class is used in order to locate resources that are dynamic. Dynamic
    resources are the ones that are defined by conditions that are not
    possible to determine a priori. An example is a resource that represents
    the latest LST file that is available. We cannot know the timeslot of
    the latest LST product without actually running the code.

    This class can search the appropriate servers and determine the actual
    parameters in order to create proper ConductorResource instances.
    """
    pass


class FileResourceFinder(ResourceFinder):

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


class FtpResourceFinder(ResourceFinder):
    pass


class ConductorCollection(object):

    def __init__(self, short_name, name=None):
        self.short_name = short_name
        self.name = name if name is not None else short_name

    def __repr__(self):
        return ("{0}.{1.__class__.__name__}({1.short_name!r}, "
                "name={1.name!r})".format(__name__, self))


class ConductorResource(object):
    """
    A resource represents an object that can be retrieved and operated upon.

    A resource has a URN that is used to uniquely identify it. The resource
    may be available at multiple URLs. Each URL is constructed by providing:

    * information on the ConductorServer that can be used to retrieve the
      resource.
    * the relative URL path where the server will look for
    * any query parameters that should be used to build each URL
    * any hash parameters that should be used to build each URL
    """

    _name = u""
    _urn = u""
    _timeslot = None
    _get_locations = []

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
            except AttributeError as err:
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

    def __init__(self, name, urn, collection=None, timeslot=None):
        self.collection = collection
        self._name = name
        self._urn = urn
        self.timeslot = datetime.datetime.now(pytz.utc)

    def __repr__(self):
        return ("{0}.{1.__class__.__name__}({1.name!r}, {1.urn!r}, "
                "collection={1.collection!r}, "
                "timeslot={1.timeslot!r})".format(__name__, self))

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
                         "Ignoring...".format(scheme["scheme"], server))

    def show_get_parameters(self):
        get_parameters = []
        for p in self._get_locations:
            scheme_config = [s for s in p["server"].schemes_get if
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
                get_parameters.append({
                    "url": url,
                    "authorization": p["authorization"],
                    "media_type": p["media_type"],
                })
        return get_parameters


class ConductorServer(object):
    """
    A ConductorServer represents a connection with a server.

    It can GET and POST resource representations according to various schemes.
    Each scheme has some specific traits such as an identifier string, a base 
    path, user identification credentials.

    When a ConductorServer is asked for a representation of a resource, it
    uses the resource's relative_path, query_params, hash together with the
    information of each of its defined schemes_get in order to construct a
    URL. It then uses the url in order to contact the host available at the
    domain and get back a representation of the resource
    """

    name = None
    domain = None
    schemes_get = []
    schemes_post = []

    def __init__(self, name, domain=None, schemes_get=None):
        self.name = name
        self.domain = domain
        self.schemes_get = schemes_get if schemes_get is not None else []

    def __repr__(self):
        return ("{0}.{1.__class__.__name__}({1.name!r}, domain={1.domain!r}, "
                "schemes_get={1.schemes_get!r})".format(__name__, self))

    def __str__(self):
        return "{}({}, {}, {})".format(self.__class__.__name__, self.name,
                                       self.domain,
                                       [s.scheme for s in self.schemes_get])

    def get_representation(self, resource):
        pass

    def post_representation(self, resource):
        pass


class ServerScheme(object):

    scheme = None
    port_number = None
    user_name = None
    user_password = None
    base_paths = []

    def __init__(self, scheme, base_paths, port_number=None, user_name=None, 
                 user_password=None):
        try:
            self.scheme = ConductorScheme[scheme.upper()]
            self.port_number = port_number
            self.user_name = user_name
            self.user_password = user_password
            self.base_paths = base_paths
        except KeyError as err:
            logger.error("Invalid scheme: {}".format(scheme))
            raise

    def __repr__(self):
        return ("{0}.{1.__class__.__name__}({1.scheme!r}, {1.base_paths!r}, "
                "port_number={1.port_number!r}, user_name={1.user_name!r}, "
                "user_password={1.user_password!r})".format(__name__, self))

    def __str__(self):
        return ("{0.__class__.__name__}({0.scheme}, "
                "{0.base_paths})".format(self))

