"""
File resource classes for conductor
"""

from datetime import datetime


class FileResource(object):
    _timeslot = None
    _search_path = ""
    _search_pattern = ""
    source = None
    product = None
    selection_rule = None
    use_predefined_movers = True
    local_mover = None
    predefined_remote_movers = []

    @property
    def timeslot(self):
        return self._timeslot

    @timeslot.setter
    def timeslot(self, timeslot):
        if not isinstance(timeslot, datetime):
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
    def search_path(self):
        return self._search_path.format(self)

    @search_path.setter
    def search_path(self, path_pattern):
        self._search_path = path_pattern

    def __init__(self, name, timeslot, local_mover, search_pattern=None,
                 search_path=None, **kwargs):
        self.name = name
        self.timeslot = timeslot
        self.local_mover = local_mover
        self.search_pattern = search_pattern
        self.search_path = search_path

    def __repr__(self):
        return "{0.__class__.__name__}({0.name}, {0.timeslot})".format(self)

    def find(self, additional_movers=[]):
        """
        Search for the file represented by this object.

        Searching can be done across multiple servers and it stops at the
        first match:

        * if there is a local_mover, it is searched;
        * if there are predefined_remote_movers and the use_predefined_movers
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

        found_paths = []
        found_mover = None
        path_pattern = "/".join((self.search_path, self.search_pattern))
        if self.local_mover is not None:
            found_paths = self.local_mover.find(path_pattern)
            found_mover = self.local_mover if len(found_paths) > 0 else None
        movers = self.predefined_remote_movers + additional_movers if \
            self.use_predefined_movers else additional_movers
        current_index = 0
        while len(found_paths) == 0 and current_index < len(movers):
            m = movers[current_index]
            found_paths = m.find(path_pattern)
            found_mover = m if len(found_paths) > 0 else None
            current_index += 1
        return found_mover, found_paths

    def fetch(self):
        raise NotImplementedError

    def delete(self, path, remote_mover=None):
        raise NotImplementedError

    def copy_to(self, path, destination, remote_mover=None):
        raise NotImplementedError
