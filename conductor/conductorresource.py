"""
Resource class for conductor
"""

import logging
import pytz
import datetime
import dateutil.parser
import json

import enum

logger = logging.getLogger(__name__)


class Settings(object):

    _movers = dict()
    _collections = dict()
    _resources = dict()

    def __init__(self):
        self._movers = dict()
        self._collections = dict()
        self._resources = dict()

    def get_settings_from_file(self, path):
        try:
            with open(path) as fh:
                all_settings = json.load(fh)
                self._movers = all_settings.get("movers", {})
                self._collections = all_settings.get("collections", {})
                self._resources = all_settings.get("resources", {})
        except IOError as e:
            logger.error(e)

    def get_mover(self, name):
        try:
            settings = [i for i in self._movers if i["name"] == name][0]
        except IndexError:
            logger.error("mover {} is not defined in the "
                         "settings".format(name))
            raise 
        mover_get_schemes = []
        for scheme_settings in settings["schemes"]:
            ms = MoverScheme(
                scheme_settings["scheme_name"],
                scheme_settings["base_paths"],
                scheme_settings["user_name"],
                scheme_settings["user_password"],
            )
            mover_get_schemes.append(ms)
        mover = ConductorMover(name, domain=settings["domain"],
                               get_schemes=mover_get_schemes)
        return mover

    def get_collection(self, short_name):
        try:
            settings = [i for i in self._collections if 
                        i["short_name"] == short_name][0]
        except IndexError:
            logger.error("collection {} is not defined in the "
                         "settings".format(name_or_short_name))
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
        collection = self.get_collection(settings["collection"])
        resource = ConductorResource(name, collection, settings["urn"],
                                     timeslot)
        for loc in settings["get_locations"]:
            try:
                mover = self.get_mover(loc["mover"])
                scheme = [s for s in mover.get_schemes if 
                          s.scheme == loc["scheme"]][0]
                scheme_type = scheme.scheme
                relative_paths = loc["relative_paths"]
                authorization = loc["authorization"]
                media_type = loc["media_type"]
                resource.add_get_location(mover, scheme_type, relative_paths,
                                          authorization, media_type)
            except IndexError:
                pass
        return resource


settings = Settings()


class ConductorCollection(object):

    def __init__(self, short_name, name=None):
        self.short_name = short_name
        self.name = name if name is not None else short_name


class ConductorResource(object):
    """
    A resource represents an object that can be retrieved and operated upon.

    A resource has a URN that is used to uniquely identify it. The resource
    may be available at multiple URLs. Each URL is constructed by providing:

    * information on the ConductorMover that can be used to retrieve the 
      resource.
    * the relative URL path where the mover will look for
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

    @property
    def urls(self):
        urls = []
        for p in self._get_locations:
            try:
                scheme = [s for s in p["mover"].get_schemes if 
                          s.scheme == p["scheme_type"]][0]
            except IndexError:
                logger.error("Unsupported scheme {} for "
                             "mover {}".format(p["scheme_type"], p["mover"]))
                raise
            url_params = []
            for relative_path in p["relative_paths"]:
                query_params = ConductorUrl.extract_query_params(
                    relative_path)[0]
                hash_part = ConductorUrl.extract_hash_part(relative_path)[0]
                if relative_path.startswith("/"):
                    url_params.append((relative_path, query_params, hash_part))
                else:
                    for base_path in scheme.base_paths:
                        full_path = "/".join((base_path, relative_path))
                        url_params.append((full_path, query_params, hash_part))

            for path, query_params, hash_part in url_params:
                url = ConductorUrl(scheme.scheme.name, 
                                   host_name=p["mover"].domain,
                                   port_number=scheme.port_number,
                                   user_name=scheme.user_name,
                                   user_password=scheme.user_password,
                                   path_part=path, hash_part=hash_part,
                                   parent=self, **query_params)
                urls.append({
                    "url": url,
                    "authorization": p["authorization"],
                    "media_type": p["media_type"],
                })
        return urls

    def __init__(self, name, collection, urn, timeslot=None, **properties):
        self.collection = collection
        self._name = name
        self._urn = urn
        self.timeslot = datetime.datetime.now(pytz.utc)
        for prop, value in properties.iteritems():
            setattr(self, prop, value)

    def add_get_location(self, mover, scheme, relative_paths, authorization,
                         media_type):
        loc = {
            "mover": mover,
            "scheme_type": scheme,
            "relative_paths": relative_paths,
            "authorization": authorization,
            "media_type": media_type,
        }
        self._get_locations.append(loc)


class ConductorUrl(object):

    scheme_name = None
    user_information = None
    user_name = None
    user_password = None
    host_name = None
    port_number = None
    path_part = None
    query_part = None
    hash_part = None

    @property
    def url(self):
        pp = self.path_part if self.path_part else ""
        qp = self.query_part if self.query_part else ""
        hp = self.hash_part if self.hash_part else ""
        if self.parent is not None:
            pp = pp.format(self.parent)
            qp = qp.format(self.parent)
            hp = hp.format(self.parent)
        result = u"{}://".format(self.scheme_name)
        if self.user_information != u"":
            result = u"{}{}@".format(result, self.user_information)
        result = u"{}{}".format(result, self.host_name)
        if self.port_number is not None:
            result = u"{}:{}".format(result, self.port_number)
        if self.path_part is not None:
            result = u"{}{}".format(result, pp)
        if self.query_part != u"":
            result = u"{}?{}".format(result, qp)
        if self.hash_part is not None:
            result = u"{}#{}".format(result, hp)
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

    def __init__(self, scheme_name, host_name="localhost", port_number=None,
                 user_name=None, user_password=None, path_part=None, 
                 hash_part=None, parent=None, **query_params):
        self.scheme_name = scheme_name
        self.host_name = host_name
        self.port_number = port_number
        self.user_name = user_name
        self.user_password = user_password
        self.path_part = path_part
        self.query_params = query_params
        self.hash_part = hash_part
        self.parent = parent

    @classmethod
    def from_string(cls, url):
        hash_part, dehashed = cls.extract_hash_part(url)
        query_params, dequeried = cls.extract_query_params(url)
        scheme_name, sep, scheme_specific = dequeried.partition(":")
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
        url = cls(scheme_name, host_name=host_name, port_number=port_number, 
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


class ConductorMover(object):
    """
    A mover represents a connection with a server.

    It can GET and POST resource representations according to various schemes.
    Each scheme has some specific traits such as an identifier string, a base 
    path, user identification credentials.

    When a mover is asked for a representation of a resource, it uses the
    resource's relative_path, query_params, hash together with the information
    of each of its defined get_schemes in order to construct a URL. It then
    uses the url in order to contact the host available at the domain and get
    back a representation of the resource
    """

    name = None
    domain = None
    get_schemes = []
    post_schemes = []

    def __init__(self, name, domain=None, get_schemes=None):
        self.name = name
        self.domain = domain
        self.get_schemes = get_schemes if get_schemes is not None else []

    def get_representation(self, resource):
        pass

    def post_representation(self, resource):
        pass



class ConductorScheme(enum.Enum):
    FTP = 1
    SFTP = 2
    HTTP = 3


class MoverScheme(object):

    FTP = u"ftp"
    SFTP = u"sftp"
    HTTP = u"http"

    scheme = None
    port_number = None
    user_name = None
    user_password = None
    base_paths = []

    def __init__(self, scheme, base_paths, port_number=None, user_name=None, 
                 user_password=None, **extra):
        self.scheme = scheme
        self.port_number = port_number
        self.user_name = user_name
        self.user_password = user_password
        self.base_paths = base_paths
        for k, b in extra:
            setattr(self, k, v)
