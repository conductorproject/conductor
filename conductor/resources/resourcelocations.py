"""
Resource location classes for conductor
"""

import logging

from .. import ConductorScheme
from .. import ServerSchemeMethod
from .. import TemporalSelectionRule
from .. import ParameterSelectionRule
from .. import errors
from ..servers import server_factory
from ..urlparser import Url

logger = logging.getLogger(__name__)


class ResourceLocation(object):

    parent = None
    server = None
    relative_paths = []
    authorization = u""
    media_type = u""

    def __init__(self, relative_paths, media_type, server=None, scheme=None,
                 authorization=u"", location_for=ServerSchemeMethod.GET,
                 parent=None):
        self.parent = parent
        server = server or server_factory.get_server()
        scheme = scheme or ConductorScheme.FILE
        config = {
            ServerSchemeMethod.GET: server.schemes_get,
            ServerSchemeMethod.FIND: server.schemes_get,
            ServerSchemeMethod.POST: server.schemes_post,
        }[location_for]
        scheme_configuration = None
        for c in config:
            if c.scheme == scheme:
                scheme_configuration = c
        if scheme_configuration is not None:
            self.server = server
            self.scheme_configuration = scheme_configuration
            self.relative_paths = relative_paths
            self.authorization = authorization
            self.media_type = media_type
        else:
            raise errors.InvalidSchemeError(
                "Unsupported scheme {} for server {}".format(scheme, server))

    def __repr__(self):
        return ("{0}.{1.__class__.__name__}({1.relative_paths}, "
                "{1.media_type}, server={1.server}, "
                "scheme_configuration={1.scheme_configuration}), "
                "authorization={1.authorization}, "
                "parent={1.parent})".format(__name__, self))

    def __str__(self):
        return ("{0.__class__.__name__}({0.relative_paths}, "
                "{0.scheme})".format(self))

    def create_urls(self):
        url_params = []
        for p in self.relative_paths:
            query_params, dequeried = Url.extract_query_params(p)
            hash_part = Url.extract_hash_part(p)[0]
            if p.startswith("/"):
                url_params.append((dequeried, query_params, hash_part))
            else:
                for base_path in self.scheme_configuration.base_paths:
                    full_path = "/".join((base_path, dequeried))
                    url_params.append((full_path, query_params, hash_part))
        result = []
        for path, query_params, hash_part in url_params:
            url = Url(self.scheme_configuration.scheme,
                      host_name=self.server.domain,
                      port_number=self.scheme_configuration.port_number,
                      user_name=self.scheme_configuration.user_name,
                      user_password=self.scheme_configuration.user_password,
                      path_part=path, hash_part=hash_part,
                      parent=self.parent, **query_params)
            result.append(url)
        return result


class ResourceLocationFind(ResourceLocation):

    temporal_rule = None
    lock_timeslot = []
    parameter_rule = None
    parameter = None

    def __init__(self, relative_paths, media_type, server=None, scheme=None,
                 authorization=u"", parent=None,
                 temporal_rule=TemporalSelectionRule.LATEST,
                 lock_timeslot=None, parameter=None,
                 parameter_rule=ParameterSelectionRule.HIGHEST):
        super(ResourceLocationFind, self).__init__(
            relative_paths, media_type, server=server, scheme=scheme,
            authorization=authorization, location_for=ServerSchemeMethod.FIND,
            parent=parent
        )
        self.temporal_rule = temporal_rule
        self.lock_timeslot = lock_timeslot or []
        self.parameter = parameter
        self.parameter_rule = parameter_rule
