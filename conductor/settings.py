"""
A settings module for conductor
"""

from urlparse import urlsplit
import logging
import json


logger = logging.getLogger(__name__)


class Settings(object):

    settings_source = None
    servers = []
    collections = []
    resources = []
    tasks = []

    def __init__(self):
        self.settings_source = None
        self.servers = []
        self.collections = []
        self.resources = []
        self.tasks = []

    def __repr__(self):
        return "{0}.{1.__class__.__name__}({1.settings_source!r})".format(
            __name__, self)

    def available_resources(self):
        return [i["name"] for i in self.resources]

    def available_collections(self):
        return [i["short_name"] for i in self.collections]

    def available_servers(self):
        return [i["name"] for i in self.servers]

    def available_tasks(self):
        return[i["name"] for i in self.tasks]

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
                self.servers = all_settings.get("servers", [])
                self.collections = all_settings.get("collections", [])
                self.resources = all_settings.get("resources", [])
                self.tasks = all_settings.get("tasks", [])
        except IOError as e:
            logger.error(e)


settings = Settings()
