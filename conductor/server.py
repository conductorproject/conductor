"""
Server classes for conductor
"""

import logging

from conductor import ConductorScheme

logger = logging.getLogger(__name__)


class Server(object):
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

    def __init__(self, name, domain=None, schemes_get=None, schemes_post=None):
        self.name = name
        self.domain = domain
        self.schemes_get = schemes_get if schemes_get is not None else []
        self.schemes_post = schemes_post if schemes_post is not None else []

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

