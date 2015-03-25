"""
File resource classes for conductor
"""

from datetime import datetime
import logging

logger = logging.getLogger("conductor.{}".format(__name__))


# TODO - Merge the SelectionRules into the FileResource class
class FileResource(object):
    MOST_RECENT = "most_recent"
    ALPHABETICAL = "alphabetical"
    _timeslot = None
    _search_paths = []
    _search_pattern = ""
    source = None
    product = None
    use_predefined_movers = True
    local_mover = None
    remote_movers = []
    filtering_rules = []

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
    def search_pattern(self):
        return self._search_pattern.format(self)

    @search_pattern.setter
    def search_pattern(self, pattern):
        self._search_pattern = pattern

    @property
    def search_paths(self):
        return [p.format(self) for p in self._search_paths]

    @search_paths.setter
    def search_paths(self, path_patterns):
        self._search_paths = path_patterns

    def __init__(self, name, timeslot=None, local_mover=None,
                 search_pattern="", search_paths=None, remote_movers=None,
                 filtering_rules=None):
        self.name = name
        self.timeslot = timeslot
        self.local_mover = local_mover
        self.search_pattern = search_pattern
        self.search_paths = search_paths if search_paths is not None else []
        self.remote_movers = remote_movers if remote_movers is not None else []
        self.filtering_rules = filtering_rules if \
            filtering_rules is not None else []

    def __repr__(self):
        return "{0.__class__.__name__}({0.name}, {0.timeslot})".format(self)

    def find(self, additional_movers=None):
        """
        Search for the file represented by this object.

        Searching can be done across multiple servers and it stops at the
        first match:

        * if there is a local_mover, it is searched;
        * if there are predefined remote_movers and the use_predefined_movers
          flag is True, each of the predefined movers is searched;
        * if there are additional movers given as input to this method, they
          are searched.

        Searching is done by concatenating each data_directory of each mover
        with the search_path and search_pattern of this object.

        Before concatenating, the data_directories, search_paths and
        search_patterns are formatted using any predefined lookups.

        Searching is done as if the resulting path is a regular expression.

        The result is a tuple with the mover where the match was made and a
        list of all the paths that matched.

        :param additional_movers:
        :return: A tuple with the mover and a list of paths
        :rtype: (ResourceMover, [str])
        """

        additional = additional_movers if additional_movers is not None else []
        found_paths = []
        found_mover = None
        path_patterns = ["/".join((p, self.search_pattern)) for p in
                         self.search_paths]
        if self.local_mover is not None:
            found_paths = self.local_mover.find(*path_patterns)
            found_mover = self.local_mover if len(found_paths) > 0 else None
        movers = (self.remote_movers + additional) if \
            self.use_predefined_movers else additional
        current_index = 0
        while len(found_paths) == 0 and current_index < len(movers):
            m = movers[current_index]
            logger.debug("About to search in mover {}...".format(m))
            found_paths = m.find(*path_patterns)
            found_mover = m if len(found_paths) > 0 else None
            current_index += 1
        return found_mover, found_paths

    def fetch(self, destination_dir, additional_movers=None):
        """
        Fetch the resource represented by this object.

        :param destination_dir:
        :param additional_movers:
        :return:
        """

        found_mover, found_paths = self.find(
            additional_movers=additional_movers)
        fetched = None
        if found_mover is not None:
            chosen = self.choose(found_paths)
            fetched = found_mover.fetch(destination_dir, chosen)
        return fetched[0]

    def delete(self, use_remote_mover=False, additional_movers=None):
        found_mover, found_paths = self.find(
            additional_movers=additional_movers)
        if found_mover is not None:
            # we have found some file paths
            chosen = self.choose(found_paths)
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

    def choose(self, resources):
        """
        Returns a single item which is deemed to be the one this object
        represents

        :param resources: A list with the paths
        :return:
        """

        result = resources[:]
        for rule in self.filtering_rules:
            if rule == self.ALPHABETICAL:
                result.sort()
            elif rule == self.MOST_RECENT:
                pass
            else:
                logger.warning("Filtering rule {} is not "
                               "recognized".format(rule))
        return resources[0]
