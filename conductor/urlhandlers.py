"""
Handlers that take care of GETting and POSTing resources to and from URLs
"""

import os
import os.path
import re
import shutil
import logging
import dateutil.parser

from ftputil import FTPHost
import ftputil.error

from . import ConductorScheme
from . import TemporalSelectionRule
from . import ParameterSelectionRule
from . import errors

logger = logging.getLogger(__file__)


class UrlHandlerFactory(object):
    """
    A factory for creating URL handlers
    """

    @staticmethod
    def get_handler(scheme):
        result = {
            ConductorScheme.FILE: FileUrlHandler,
            ConductorScheme.FTP: FtpUrlHandler,
            ConductorScheme.SFTP: SftpUrlHandler,
            ConductorScheme.HTTP: HttpUrlHandler,
        }.get(scheme)
        return result()


url_handler_factory = UrlHandlerFactory()


class BaseUrlHandler(object):

    @staticmethod
    def create_local_directory(path):
        if not os.path.isdir(path):
            os.makedirs(path)

    def __repr__(self):
        return "{0}.{1.__class__.__name__}()".format(__name__, self)

    def _timeslot_is_valid(self, timeslot, respect_timeslot,
                           reference_timeslot):
        valid = True
        for part in respect_timeslot:
            if not self._respects_temporal_part(timeslot, part,
                                                reference_timeslot):
                valid = False
        return valid

    def _respects_temporal_part(self, timeslot, part_name, reference):
        reference_part = getattr(reference, part_name)
        testing_part = getattr(timeslot, part_name)
        result = False
        if testing_part == reference_part:
            result = True
        return result

    @staticmethod
    def _validate_respect_timeslot_inputs(params):
        for p in params:
            if p not in ["year", "month", "day", "hour", "minute", "all"]:
                raise ValueError("Invalid respect_timeslot "
                                 "parameter: {}".format(p))

    @staticmethod
    def _validate_parameter_input(resource, name, rule):
        if name is not None:
            if name not in resource.parameters.keys():
                raise ValueError("Invalid parameter name: {}".format(name))
            if rule not in ParameterSelectionRule:
                raise ValueError("Invalid selection rule: {}".format(rule))

    @staticmethod
    def _extract_path_timeslot(path):
        dirname, basename = os.path.split(path)
        timeslot_file_patterns = [
            r"\d{12}"  # year, month, day, hour, minute
        ]
        timeslot = None
        for patt in timeslot_file_patterns:
            try:
                ts_string = re.search(patt, basename).group()
                timeslot = dateutil.parser.parse(ts_string)
            except (AttributeError, ValueError):
                pass
        return timeslot


class FileUrlHandler(BaseUrlHandler):

    def find_in_url(self, url, selection_method="latest"):
        found = self.select_path(url.path_part,
                                 selection_method=selection_method)
        return found

    def get_from_url(self, url, destination_directory):
        """
        Get the representation of the resource available at the input URL

        Copy the input url into the destination directory.

        :arg url: The URL to be searched
        :type url: conductor.urlparser.Url
        :arg destination_directory: Directory where the resource's
            representation will be saved into. It must exist.
        :type destination_directory: str
        :return: The full path to the representation that was retrieved
            from the input URL
        :rtype: str
        :raises: conductor.errors.ResourceNotFoundError
        """

        self.create_local_directory(destination_directory)
        path = url.path_part
        destination = os.path.join(destination_directory,
                                   os.path.basename(path))
        try:
            shutil.copyfile(path, destination)
        except IOError as err:
            raise errors.ResourceNotFoundError(err.args)
        return destination

    @staticmethod
    def post_to_url(url, path):
        """
        Send a file to the input URL.

        The input URL is expected to use the FILE scheme and its path_part
        must end with the directory where the path will be copied into.

        :param url:
        :param path:
        :return:
        """

        destination = os.path.join(url.path_part, os.path.basename(path))
        try:
            if not os.path.isdir(url.path_part):
                os.makedirs(url.path_part)
            shutil.copyfile(path, destination)
        except (OSError, IOError) as err:
            err_no, msg = err.args
            if err_no == 2:
                raise errors.LocalPathNotFoundError(msg)
            elif err_no == 13:
                raise errors.ResourceNotFoundError(msg)
            else:
                raise
        return destination

    @staticmethod
    def select_path(full_path_pattern, selection_method="latest",
                    except_paths=None):
        """
        Return the full path to an existing directory that meets search criteria

        This function accepts a pattern that is interpreted as being the
        specification for finding a real path on the filesystem.

        >>> server_base_path = "/home/geo2/test_data/giosystem/data"
        >>> relative_path = "OUTPUT_DATA/PRE_PROCESS/LRIT2HDF5_g2/DYNAMIC_OUTPUT/v2.4"
        >>> dynamic_part = "(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{2})"
        >>> path = os.path.join(server_base_path, relative_path, dynamic_part)
        >>> select_path(path, selection_method="latest")

        :param full_path_pattern:
        :param selection_method:
        :param except_paths:
        :return:
        """
        except_paths = except_paths if except_paths is not None else []
        base = full_path_pattern[:full_path_pattern.find("(")]
        dynamic = full_path_pattern[full_path_pattern.find("("):]
        dynamic_parts = dynamic.split("/")
        current_path = base
        if len(dynamic_parts) > 1:
            next_hierarchic_part = 0
            while 0 <= next_hierarchic_part < len(dynamic_parts):
                hierarchic_part = dynamic_parts[next_hierarchic_part]
                old_path = current_path
                candidates = []
                try:
                    for c in os.listdir(current_path):
                        if os.path.isdir(os.path.join(current_path, c)):
                            re_obj = re.search(hierarchic_part, c)
                            if re_obj is not None:
                                candidates.append(c)
                except OSError:
                    pass
                sorted_candidates = sorted(candidates)
                if selection_method == "latest":
                    sorted_candidates.reverse()
                candidate_index = 0
                found = False
                while candidate_index < len(sorted_candidates) and not found:
                    current_path = os.path.join(
                        old_path, sorted_candidates[candidate_index])
                    found = True if current_path not in except_paths else False
                    candidate_index += 1
                if found:
                    next_hierarchic_part += 1
                else:
                    # cycle back to the previous hierarchic level
                    next_hierarchic_part -= 1
                    cycle_back_path = os.path.dirname(current_path)
                    except_paths.append(cycle_back_path)
                    current_path = os.path.dirname(cycle_back_path)
        else:
            current_path = (current_path if current_path not in
                                            except_paths else None)
        return current_path

    def find_path(self, resource, paths, respect_timeslot=None,
                  temporal_rule=TemporalSelectionRule.LATEST,
                  parameter_name=None,
                  parameter_rule=ParameterSelectionRule.HIGHEST):
        """
        Return the path that fits the selection rules.

        :param resource:
        :param paths:
        :param respect_timeslot:
        :param temporal_rule:
        :param parameter_name:
        :param parameter_rule:
        :return:
        """

        respect_timeslot = respect_timeslot or []
        if any(respect_timeslot) and respect_timeslot[0] == "all":
            respect_timeslot = ["year", "month", "day", "hour", "minute"]
        self._validate_respect_timeslot_inputs(respect_timeslot)
        self._validate_parameter_input(resource, parameter_name,
                                       parameter_rule)
        candidates = []
        for p in paths:
            path_slot = self._extract_path_timeslot(p)
            path_parameters = resource.extract_path_parameters(p)
            valid_slot = self._timeslot_is_valid(path_slot, respect_timeslot,
                                                 resource.timeslot)
            if valid_slot:
                candidates.append((p, path_parameters, path_slot))
        temporal_sorted = sorted(candidates, key=lambda i: i[-1])
        if temporal_rule == TemporalSelectionRule.LATEST:
            temporal_sorted.sort(reverse=True)
        result = temporal_sorted
        if parameter_name is not None:
            parameter_sorted = sorted(temporal_sorted,
                                      key=lambda i: i[1][parameter_name])
            if parameter_rule == ParameterSelectionRule.HIGHEST:
                parameter_sorted.sort(reverse=True)
            result = parameter_sorted
        return result[0][0] if any(result) else None


class FtpUrlHandler(BaseUrlHandler):

    def get_from_url(self, url, destination_directory):
        """
        Get the representation of the resource available at the input URL

        Copy the input url into the destination directory.

        :arg url: The URL to be searched
        :type url: conductor.urlparser.Url
        :arg destination_directory: Directory where the resource's
            representation will be saved into. It must exist.
        :type destination_directory: str
        :return: The full path to the representation that was retrieved
            from the input URL
        :rtype: str
        :raises: conductor.errors.ResourceNotFoundError,
            conductor.errors.InvalidUserCredentialsError,
            conductor.errors.HostNotFoundError
        """

        self.create_local_directory(destination_directory)
        path = url.path_part
        destination = os.path.join(destination_directory,
                                   os.path.basename(path))
        try:
            with FTPHost(url.host_name, url.user_name, url.user_password) as h:
                h.download(url.path_part, destination)
        except ftputil.error.PermanentError as err:
            if err.errno == 530:
                raise errors.InvalidUserCredentialsError(err.args)
            elif err.errno == 550:
                raise errors.ResourceNotFoundError(err.args)
            else:
                raise
        except ftputil.error.FTPOSError as err:
            code, msg = err.args
            if code == -2:
                logger.error("Server {} not found: {}".format(
                    url.host_name, msg))
                raise errors.HostNotFoundError(
                    "Server {} not found".format(url.host_name))
            raise
        except ftputil.error.FTPIOError as err:
            raise errors.ResourceNotFoundError(err.args)
        return destination

    @staticmethod
    def post_to_url(url, path):
        destination = os.path.join(url.path_part, os.path.basename(path))
        try:
            with FTPHost(url.host_name, url.user_name, url.user_password) as h:
                destination_dir = os.path.dirname(destination)
                if not h.path.isdir(destination_dir):
                    h.makedirs(destination_dir)
                h.upload(path, destination)
        except ftputil.error.FTPIOError as err:
            raise errors.ResourceNotFoundError(err.args)
        except ftputil.error.PermanentError as err:
            if err.errno == 550:
                raise errors.ResourceNotFoundError(err.args)
            else:
                raise
        except IOError as err:
            err_no, msg = err.args
            if err_no == 2:
                raise errors.LocalPathNotFoundError(msg)
        return destination


class SftpUrlHandler(BaseUrlHandler):
    pass


class HttpUrlHandler(BaseUrlHandler):
    pass
