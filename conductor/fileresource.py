"""
File resource classes for conductor
"""

import os
import bz2
from datetime import datetime
import logging

from enum import Enum

import errors

logger = logging.getLogger(__name__)


FILTER_RULES = Enum("FILTER_RULES",
                    "MOST_RECENT "
                    "LEAST_RECENT "
                    "ALPHABETICAL "
                    "FIX_YEAR "
                    "FIX_MONTH "
                    "FIX_DAY "
                    "FIX_HOUR "
                    "FIX_MINUTE "
                    "FIX_DEKADE "
                    "BEFORE_REFERENCE_TIMESLOT "
                    "REGARDLESS_REFERENCE_TIMESLOT "
                    "AFTER_REFERENCE_TIMESLOT")

class FileResource(object):
    _timeslot = None
    _search_pattern = ""
    _search_paths = []
    description = ""
    source = None
    product = None
    use_predefined_movers = True
    local_mover = None

    @property
    def timeslot(self):
        return self._timeslot

    @timeslot.setter
    def timeslot(self, timeslot):
        if isinstance(timeslot, basestring):
            timeslot = datetime.strptime(timeslot, "%Y%m%d%H%M")
        self._timeslot = timeslot

    @property
    def year(self):
        return self.timeslot.year if self.timeslot is not None else None

    @property
    def year_day(self):
        if self.timeslot is not None:
            result = self.timeslot.timetuple().tm_yday
        else:
            result = None
        return result

    @property
    def month(self):
        return self.timeslot.month if self.timeslot is not None else None

    @property
    def day(self):
        return self.timeslot.day if self.timeslot is not None else None

    @property
    def hour(self):
        return self.timeslot.hour if self.timeslot is not None else None

    @property
    def minute(self):
        return self.timeslot.minute if self.timeslot is not None else None

    @property
    def dekade(self):
        result = None
        if self.timeslot is not None:
            day = self.timeslot.day
            result = 1 if day < 11 else (2 if day < 21 else 3)
        return result

    @property
    def search_pattern(self):
        return self._search_pattern.format(self)

    @search_pattern.setter
    def search_pattern(self, pattern):
        self._search_pattern = pattern

    @property
    def search_paths(self):
        return [sp.path_pattern.format(self) for sp in self._search_paths]

    def __init__(self, name, timeslot=None, local_mover=None,
                 search_pattern="", description=""):
        self.name = name
        self.description = description
        self.timeslot = timeslot
        self.local_mover = local_mover
        self.search_pattern = search_pattern
        self._search_paths = []

    def __repr__(self):
        return "{0.__class__.__name__}({0.name}, {0.timeslot})".format(self)

    def add_search_path(self, path_pattern, remote_movers=None):
        sp = ResourceSearchPath(path_pattern,
                                remote_movers=remote_movers)
        self._search_paths.append(sp)

    def find(self):
        """
        Search for the file represented by this object.

        Searching can be done across multiple servers and it stops at the
        first match:

        * if there is a local_mover, it is searched;
        * if the use_predefined_movers flag is True, each of the remote movers
          used by the search_paths is searched;

        Searching is done by concatenating each data_directory of each mover
        with the search_path and search_pattern of this object.

        Before concatenating, the data_directories, search_paths and
        search_patterns are formatted using any predefined lookups.

        Searching is done as if the resulting path is a regular expression.

        The result is a tuple with the mover where the match was made and a
        list of all the paths that matched.

        :return: A tuple with the mover and a list of paths
        :rtype: (ResourceMover, [str])
        """

        found_mover = None
        found_paths = []
        if self.local_mover is not None:
            logger.debug("Searching in the local mover...")
            path_patterns = ["/".join((p, self.search_pattern)) for
                             p in self.search_paths]
            found_paths = self.local_mover.find(*path_patterns)
            found_mover = self.local_mover if len(found_paths) > 0 else None
        if self.use_predefined_movers and len(found_paths) == 0:
            logger.debug("Searching in remote movers...")
            index = 0
            while len(found_paths) == 0 and index < len(self.search_paths):
                sp = self._search_paths[index]
                found_mover, found_paths = sp.find_in_remotes(self)
                index += 1
        return found_mover, found_paths

    def fetch(self, destination_dir, filtering_rules=None):
        """
        Fetch the resource represented by this object.

        :param destination_dir:
        :param additional_movers:
        :return:
        """

        found_mover, found_paths = self.find()
        fetched = None
        if found_mover is not None:
            chosen = self.choose(found_paths, filtering_rules=filtering_rules)
            logger.debug("about to use mover {} for fetching "
                         "{}...".format(found_mover, chosen))
            fetched = found_mover.fetch(destination_dir, chosen)
        return fetched[0]

    def decompress(self, *paths):
        result = []
        for p in paths:
            dirname, bzname = os.path.split(p)
            fname, extension = os.path.splitext(p)
            decompressed = os.path.join(dirname, fname)
            if extension == ".bz2":
                with bz2.BZ2File(p) as bzh, open(decompressed, "wb") as fh:
                    fh.write(bzh.read())
            result.append(decompressed)
            os.remove(p)
        return result

    def delete(self, use_remote_mover=False, filtering_rules=None):
        found_mover, found_paths = self.find()
        if found_mover is not None:
            # we have found some file paths
            chosen = self.choose(found_paths, filtering_rules=filtering_rules)
            if found_mover == self.local_mover:
                found_mover.delete(chosen)  # delete from local directory
            elif use_remote_mover:
                found_mover.delete(chosen)  # delete from remote directory
            else:
                # the file was found on a remote mover and we do not want to
                # delete it
                pass
        else:
            logger.warning("Could not find the file to delete")

    def copy_to(self, path, destination, remote_mover=None):
        raise NotImplementedError

    def choose(self, resources, filtering_rules=None):
        """
        Returns a single item which is deemed to be the one this object
        represents

        :param resources: A list with the paths
        :return:
        """

        filters = filtering_rules if filtering_rules is not None else []
        result = resources[:]
        for rule in filters:
            if rule == FILTER_RULES.ALPHABETICAL:
                result.sort()
            else:
                logger.warning("Filtering rule {} is not "
                               "implemented".format(rule))
        return resources[0]


class ResourceSearchPath(object):
    remote_movers = []
    path_pattern = ""

    def __init__(self, path_pattern, remote_movers=None):
        self.path_pattern = path_pattern
        self.remote_movers = remote_movers or []

    def find_in_remotes(self, file_resource):
        found = []
        in_mover = None
        index = 0
        while len(found) == 0 and index < len(self.remote_movers):
            try:
                mover = self.remote_movers[index]
                logger.debug("About to search in mover {}...".format(mover))
                full_pattern = os.path.join(
                    self.path_pattern.format(file_resource),
                    file_resource.search_pattern
                )
                found = mover.find(full_pattern)
                in_mover = mover if len(found) > 0 else None
            except AttributeError:
                raise ValueError("remote_movers must be a list of RemoteMover "
                                 "instances")
            except errors.InvalidFTPHostError as e:
                logger.error(e)
            index += 1
        return in_mover, found

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.path_pattern)
