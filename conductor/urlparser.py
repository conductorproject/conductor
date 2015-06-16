"""
Url classes for conductor
"""

import socket
import logging

from conductor import ConductorScheme
import conductor.errors

logger = logging.getLogger(__name__)


class Url(object):

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
        result = pp
        try:
            result = pp.format(self.parent) if self.parent else pp
        except IndexError:
            logger.debug("Cannot format path_part")
        return result

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

    def __init__(self, scheme=None, host_name="localhost", port_number=None,
                 user_name=None, user_password=None, path_part=u"",
                 hash_part=u"", parent=None, **query_params):
        self.scheme = scheme if scheme is not None else ConductorScheme.FILE
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
                "{1.url!r})".format(__name__, self))

    def __str__(self):
        return self.url

    @classmethod
    def from_string(cls, url):
        hash_part, dehashed = cls.extract_hash_part(url)
        query_params, dequeried = cls.extract_query_params(url)
        scheme_name, sep, scheme_specific = dequeried.partition(":")
        if scheme_specific == "":
            scheme_specific = scheme_name
            scheme_name = "file"
        if scheme_name == "":
            scheme_name = ConductorScheme.FILE.name
        try:
            scheme = ConductorScheme.__members__[scheme_name.upper()]
        except KeyError:
            raise conductor.errors.InvalidSchemeError(
                "Invalid scheme: {}".format(scheme_name))
        host_name="localhost"
        port_number=None
        user_name = None
        user_password = None
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
        try:
            rest, sep, query_part = url_string.partition("?")
            if query_part != "":
                dehashed, sep, hash_part = query_part.partition("#")
                for pair in dehashed.split("&"):
                    name, value = pair.split("=")
                    params[name] = value
        except ValueError:
            logger.debug("there was an error extracting query params from {}. "
                        "Continuing...".format(url_string))
            rest = url_string
        return params, rest

    @staticmethod
    def extract_hash_part(url_string):
        dehashed, sep, hash_part = url_string.partition("#")
        result = hash_part if hash_part != "" else None
        return result, dehashed
