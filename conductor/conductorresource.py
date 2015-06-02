"""
Resource class for conductor
"""

import logging
import pytz
import datetime
import dateutil.parser

logger = logging.getLogger(__name__)

class ConductorResource(object):
    _name = u""
    _urn = u""
    _timeslot = None
    urls = []

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

    def __init__(self, name, urn, timeslot=None, **properties):
        self._name = name
        self._urn = urn
        self.timeslot = datetime.datetime.now(pytz.utc)
        for prop, value in properties.iteritems():
            setattr(self, prop, value)
        self.urls = []

    def add_url(self, url):
        if isinstance(url, ConductorUrl):
            self.urls.append(url)
        else:
            u = ConductorUrl.from_string(url)
            self.urls.append(u)


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
        result = u"{}://".format(self.scheme_name)
        if self.user_information != u"":
            result = u"{}{}@".format(result, self.user_information)
        result = u"{}{}".format(result, self.host_name)
        if self.port_number is not None:
            result = u"{}:{}".format(result, self.port_number)
        if self.path_part is not None:
            result = u"{}{}".format(result, self.path_part)
        if self.query_part != u"":
            result = u"{}?{}".format(result, self.query_part)
        if self.hash_part is not None:
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

    def __init__(self, scheme_name, host_name="localhost", port_number=None,
                 user_name=None, user_password=None, path_part=None, 
                 hash_part=None, **query_params):
        self.scheme_name = scheme_name
        self.host_name = host_name
        self.port_number = port_number
        self.user_name = user_name
        self.user_password = user_password
        self.path_part = path_part
        self.query_params = query_params
        self.hash_part = hash_part

    @classmethod
    def from_string(cls, url):
        dehashed, sep, hash_part = url.rpartition("#")
        if dehashed == u"":
            dehashed = url
            hash_part = None
        dequeried, sep, query_part = dehashed.rpartition("?")
        query_params = dict()
        if dequeried == u"":
            dequeried = dehashed
        else:
            for pair in query_part.split("&"):
                name, value = pair.split("=")
                query_params[name] = value
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

