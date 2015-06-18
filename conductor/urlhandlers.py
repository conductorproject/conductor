"""
Handlers that take care of GETting and POSTing resources to and from URLs
"""

import os
import os.path
import copy
import re
import shutil
import logging
import dateutil.parser

from ftputil import FTPHost
import ftputil.error

from . import ConductorScheme
from . import TemporalSelectionRule
from . import ParameterSelectionRule
from . import TemporalPart
from . import errors
from .resources.resourcelocations import ResourceLocation

logger = logging.getLogger(__name__)


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

    def _timeslot_is_valid(self, timeslot, lock_timeslot,
                           reference_timeslot):
        valid = True
        for part in lock_timeslot:
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
    def _validate_lock_timeslot_inputs(params):
        for p in params:
            if p not in [n.lower() for n, m in
                         TemporalPart.__members__.items()]:
                raise ValueError("Invalid lock_timeslot "
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

    @staticmethod
    def extract_parameter_spec(fragment):
        pattern = r"\{0\.parameters\[(.*?)\]\}"
        re_obj = re.search(pattern, fragment)
        return re_obj.group(1) if re_obj is not None else None

    @staticmethod
    def extract_temporal_spec(fragment):
        spec = None
        re_pattern = None
        format_string = ""
        for n, member in TemporalPart.__members__.items():
            name = n.lower()
            pattern = r"\{{0\.timeslot\.{}:?(.*?)\}}".format(name)
            re_obj = re.search(pattern, fragment)
            if re_obj is not None:
                spec = name
                format_string = re_obj.group(1)
                try:
                    re_pattern = r"\{}{{{}}}".format(format_string[-1],
                                                     len(format_string[:-1]))
                except IndexError:
                    pass
        return spec, format_string, re_pattern


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

    def find_resource_info(self, url, reference_resource,
                           lock_timeslot=None, parameter=None,
                           temporal_rule=TemporalSelectionRule.LATEST,
                           parameter_rule=ParameterSelectionRule.HIGHEST):
        dynamic_path = url.path_part
        resource_info = None
        found = None
        max_num_dirs = 20  # how many directories to scan before bailing
        i = 0
        exclude_dirs = []
        while found is None and i < max_num_dirs:
            try:
                directory = self.find_directory(
                    dynamic_path, resource=reference_resource,
                    exclude=exclude_dirs, lock_timeslot=lock_timeslot,
                    parameter=parameter, parameter_rule=parameter_rule,
                    temporal_rule=temporal_rule
                )
                logger.debug("directory: {}".format(directory))
                found = self.find_info(
                    reference_resource, directory,
                    lock_timeslot=lock_timeslot, temporal_rule=temporal_rule,
                    parameter=parameter, parameter_rule=parameter_rule
                )
                if found is None:
                    exclude_dirs.append(directory)
                    i += 1
            except OSError as err:
                logger.error(err)
                break
        if found is not None:
            path, params, slot = found
            resource_info = slot, params
        return resource_info

    @staticmethod
    def find_directory(relative_path, resource=None, lock_timeslot=None,
                       exclude=None, parameter=None,
                       parameter_rule=ParameterSelectionRule.HIGHEST,
                       temporal_rule=TemporalSelectionRule.LATEST):
        lock_timeslot = lock_timeslot or []
        exclude = exclude or []
        parts = relative_path.split("/")
        path = parts[0]
        i = 1
        while i < len(parts):
            next_part = parts[i]
            next_param_part = BaseUrlHandler.extract_parameter_spec(next_part)
            next_temporal_part, format_string, re_pattern = (
                BaseUrlHandler.extract_temporal_spec(next_part))
            if not (next_param_part or next_temporal_part):
                next_level = next_part  # lets go one level deeper
            elif next_temporal_part and next_temporal_part in lock_timeslot:
                the_string = "{{:{}}}".format(format_string)
                next_level = the_string.format(
                    getattr(resource.timeslot, next_temporal_part))
            else:
                next_parts = os.listdir(path)
                if next_temporal_part:
                    patt = re_pattern or r".*?"
                    # lets choose next part according to the temporal rule
                    candidates = [n for n in next_parts if re.search(patt, n)]
                    candidates.sort(reverse=(
                        temporal_rule == TemporalSelectionRule.LATEST))
                    next_level = candidates[0]
                elif next_param_part and next_param_part == parameter:
                    # lets choose next part according to the param rule
                    candidates = next_parts[:]
                    candidates.sort(reverse=(
                        parameter_rule == ParameterSelectionRule.HIGHEST))
                    next_level = candidates[0]
                else:
                    logger.warning("Found parameter {!r} in path and "
                                   "it is not being used for "
                                   "finding... Selecting the first available "
                                   "sub path".format(next_param_part))
                    next_level = next_parts[0]
            path = "/".join((path, next_level))
            i += 1
        return path

    def find_info(self, resource, directory, lock_timeslot=None,
                  temporal_rule=TemporalSelectionRule.LATEST,
                  parameter=None,
                  parameter_rule=ParameterSelectionRule.HIGHEST):
        """
        Return the resource info that fits the selection rules.

        :param resource:
        :param directory:
        :param lock_timeslot:
        :param temporal_rule:
        :param parameter:
        :param parameter_rule:
        :return:
        """

        lock_timeslot = lock_timeslot or []
        if any(lock_timeslot) and lock_timeslot[0] == "all":
            lock_timeslot = [n.lower() for n, m in
                             TemporalPart.__members__.items()]
        self._validate_lock_timeslot_inputs(lock_timeslot)
        self._validate_parameter_input(resource, parameter, parameter_rule)
        candidates = []
        for p in os.listdir(directory):
            path_slot = self._extract_path_timeslot(p)
            path_parameters = resource.extract_path_parameters(p)
            valid_slot = self._timeslot_is_valid(path_slot, lock_timeslot,
                                                 resource.timeslot)
            if valid_slot:
                candidates.append((p, path_parameters, path_slot))
        temporal_sorted = sorted(
            candidates, key=lambda i: i[-1],
            reverse=(temporal_rule == TemporalSelectionRule.LATEST)
        )
        result = temporal_sorted
        if parameter is not None:
            parameter_sorted = sorted(
                temporal_sorted, key=lambda i: i[1][parameter],
                reverse=(parameter_rule == ParameterSelectionRule.HIGHEST)
            )
            result = parameter_sorted
        return result[0] if any(result) else None


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
