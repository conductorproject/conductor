"""
Resource class for conductor
"""

import os
import socket
import logging
import pytz
import datetime
import dateutil.parser
import json
from urlparse import urlsplit

import enum
from ftputil import FTPHost
import ftputil.error

logger = logging.getLogger(__name__)


class UrlHandlerFactory(object):

    @staticmethod
    def get_handler(scheme):
        result = None
        if scheme == ConductorScheme.FILE:
            result = FileUrlHandler()
        elif scheme == ConductorScheme.FTP:
            result = FtpUrlHandler()
        else:
            logger.error("Scheme {!r} is not supported".format(scheme))
        return result


url_handler_factory = UrlHandlerFactory()


class BaseUrlHandler(object):

    @staticmethod
    def create_local_directory(path):
        if not os.path.isdir(path):
            os.makedirs(path)


class FileUrlHandler(BaseUrlHandler):

    def get_url(self, url, destination_directory):
        path = url.path_part
        raise NotImplementedError


class FtpUrlHandler(BaseUrlHandler):

    def get_url(self, url, destination_directory):
        """
        Get the representation of the resource available at the input URL
        """

        self.create_local_directory(destination_directory)
        try:
            with FTPHost(url.host_name, url.user_name, url.user_password) as host:
                host.download_if_newer(url.path_part, destination_directory)
        except ftputil.error.PermanentError as err:
            error_code, sep, msg = err.args[0].partition(" ")
            if int(error_code) == 530:
                logger.error("Invalid login: {}".format(msg))
            elif int(error_code) == 550:
                logger.error("Invalid path: {}".format(msg))
            raise
        except ftputil.error.FTPOSError as err:
            code, msg = err.args
            if code == -2:
                logger.error("Server {} not found: {}".format(
                    url.host_name, msg))
            raise


class ConductorScheme(enum.Enum):
    FILE = 1
    FTP = 2
    SFTP = 3
    HTTP = 4


class Settings(object):

    settings_source = None
    _servers = []
    _collections = []
    _resources = []

    def __init__(self):
        self.settings_source = None
        self._servers = dict()
        self._collections = dict()
        self._resources = dict()

    def __repr__(self):
        return "{0}.{1.__class__.__name__}({1.settings_source!r})".format(
            __name__, self)

    def available_resources(self):
        return [r["name"] for r in self._resources]

    def available_collections(self):
        return [r["short_name"] for r in self._collections]

    def available_servers(self):
        return [r["name"] for r in self._servers]

    def get_settings(self, url):
        parsed_url = urlsplit(url)
        if parsed_url.scheme == "file":
            self.get_settings_from_file(parsed_url.path)
            self.settings_source = url
        else:
            logger.error("unsupported url scheme: "
                         "{}".format(parsed_url.scheme))

    def get_settings_from_file(self, path):
        try:
            with open(path) as fh:
                all_settings = json.load(fh)
                self._servers = all_settings.get("servers", {})
                self._collections = all_settings.get("collections", {})
                self._resources = all_settings.get("resources", {})
        except IOError as e:
            logger.error(e)

    def get_server(self, name):
        try:
            settings = [i for i in self._servers if i["name"] == name][0]
        except IndexError:
            logger.error("server {} is not defined in the "
                         "settings".format(name))
            raise 
        server_get_schemes = []
        for scheme_settings in settings["schemes"]:
            ss = ServerScheme(
                scheme_settings["scheme_name"],
                scheme_settings["base_paths"],
                port_number=scheme_settings.get("port_number"),
                user_name=scheme_settings.get("user_name"),
                user_password=scheme_settings.get("user_password"),
            )
            server_get_schemes.append(ss)
        server = ConductorServer(name, domain=settings["domain"],
                                 schemes_get=server_get_schemes)
        return server

    def get_collection(self, short_name):
        try:
            settings = [i for i in self._collections if 
                        i["short_name"] == short_name][0]
        except IndexError:
            logger.error("collection {} is not defined in the "
                         "settings".format(short_name))
            raise 
        collection = ConductorCollection(short_name, name=settings.get("name"))
        return collection

    def get_resource(self, name, timeslot=None):
        try:
            settings = [i for i in self._resources if i["name"] == name][0]
        except IndexError:
            logger.error("resource {} is not defined in the "
                         "settings".format(name))
            raise
        collection = None
        if settings.get("collection") is not None:
            collection = self.get_collection(settings["collection"])
        resource = ConductorResource(name, settings["urn"],
                                     collection=collection,
                                     timeslot= timeslot)
        for loc in settings["get_locations"]:
            try:
                server = self.get_server(loc["server"])
                scheme_config = [s for s in server.schemes_get if
                                 s.scheme.name == loc["scheme"].upper()][0]
                scheme = scheme_config.scheme
                relative_paths = loc["relative_paths"]
                authorization = loc.get("authorization")
                media_type = loc["media_type"]
                resource.add_get_location(server, scheme, relative_paths,
                                          authorization, media_type)
            except IndexError:
                logger.warning("get location uses undefined scheme: {!r}. "
                               "Ignoring...".format(loc["scheme"]))
        return resource


settings = Settings()


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
                query_params, dequeried = ConductorUrl.extract_query_params(
                    relative_path)
                hash_part = ConductorUrl.extract_hash_part(relative_path)[0]
                if relative_path.startswith("/"):
                    url_params.append((dequeried, query_params, hash_part))
                else:
                    for base_path in scheme_config.base_paths:
                        full_path = "/".join((base_path, dequeried))
                        url_params.append((full_path, query_params, hash_part))

            for path, query_params, hash_part in url_params:
                url = ConductorUrl(scheme_config.scheme,
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


class ConductorUrl(object):

    scheme = None
    user_information = None
    user_name = None
    user_password = None
    host_name = None
    port_number = None
    _path_part = None
    _query_part = None
    _hash_part = None

    @property
    def path_part(self):
        pp = self._path_part if self._path_part else ""
        return pp.format(self.parent) if self.parent else pp

    @path_part.setter
    def path_part(self, new_path_part):
        self._path_part = new_path_part

    @property
    def query_part(self):
        qp = self._query_part if self._query_part else ""
        return qp.format(self.parent) if self.parent else qp

    @query_part.setter
    def query_part(self, new_query_part):
        self._query_part = new_query_part

    @property
    def hash_part(self):
        hp = self._hash_part if self._hash_part else ""
        return hp.format(self.parent) if self.parent else hp

    @hash_part.setter
    def hash_part(self, new_hash_part):
        self._hash_part = new_hash_part

    @property
    def url(self):
        result = u"{}://".format(self.scheme.name.lower())
        if self.user_information != u"":
            result = u"{}{}@".format(result, self.user_information)
        result = u"{}{}".format(result, self.host_name)
        if self.port_number is not None:
            result = u"{}:{}".format(result, self.port_number)
        if self.path_part != u"":
            result = u"{}{}".format(result, self.path_part)
        if self.query_part != u"":
            result = u"{}?{}".format(result, self.query_part)
        if self.hash_part != u"":
            result = u"{}#{}".format(result, self.hash_part)
        return result

    @property
    def user_information(self):
        info = u""
        if self.user_name is not None:
            info = unicode(self.user_name)
            if self.user_password is not None:
                info = ":".join((info, self.user_password))
        return info

    @property
    def query_part(self):
        q = ["{}={}".format(k, v) for k, v in self.query_params.iteritems()]
        return u"&".join(q)

    def __init__(self, scheme, host_name="localhost", port_number=None,
                 user_name=None, user_password=None, path_part=u"",
                 hash_part=u"", parent=None, **query_params):
        self.scheme = scheme
        local = socket.gethostname()
        if host_name == "localhost" or host_name.partition(".")[0] == local:
            self.host_name = "localhost"
        else:
            self.host_name = host_name
        self.port_number = port_number
        self.user_name = user_name
        self.user_password = user_password
        self.path_part = path_part
        self.query_params = query_params
        self.hash_part = hash_part
        self.parent = parent

    def __repr__(self):
        return ("{0}.{1.__class__.__name__}.from_string("
                "{1.url!r})".format(__name__,self))

    def __str__(self):
        return self.url

    @classmethod
    def from_string(cls, url):
        hash_part, dehashed = cls.extract_hash_part(url)
        query_params, dequeried = cls.extract_query_params(url)
        scheme_name, sep, scheme_specific = dequeried.partition(":")
        try:
            scheme = ConductorScheme.__members__[scheme_name.upper()]
        except KeyError:
            logger.error("Invalid scheme: {!r}".format(scheme_name))
            raise
        if scheme_specific.startswith("//"):
            authority_part, sep, path_part = scheme_specific[2:].partition("/")
            user_info, sep, host_info = authority_part.partition("@")
            if host_info == u"":
                user_name = None
                user_password = None
                host_info = user_info
            else:
                user_name, user_password = user_info.split(":")
            host_name, sep, port_number = host_info.partition(":")
            port_number = port_number if port_number != u"" else None
        else:
            path_part = scheme_specific
        if not path_part.startswith("/"):
            path_part = u"/{}".format(path_part)
        url = cls(scheme, host_name=host_name, port_number=port_number,
                  user_name=user_name, user_password=user_password, 
                  path_part=path_part, hash_part=hash_part, **query_params)
        return url

    @staticmethod
    def extract_query_params(url_string):
        params = dict()
        rest, sep, query_part = url_string.partition("?")
        if query_part != "":
            dehashed, sep, hash_part = query_part.partition("#")
            for pair in dehashed.split("&"):
                name, value = pair.split("=")
                params[name] = value
        return params, rest

    @staticmethod
    def extract_hash_part(url_string):
        dehashed, sep, hash_part = url_string.partition("#")
        result = hash_part if hash_part != "" else None
        return result, dehashed


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

