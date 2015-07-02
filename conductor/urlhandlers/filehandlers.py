import os
import os.path
import re
import shutil

import logging

from .base import BaseUrlHandler
from .. import errors
from .. import (TemporalSelectionRule, TemporalPart, ParameterSelectionRule)

logger = logging.getLogger(__name__)


class FileUrlHandler(BaseUrlHandler):

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
        directory_pattern, sep, name_pattern = dynamic_path.rpartition("/")
        name_pattern = name_pattern if name_pattern != "" else ".*"

        resource_info = None
        found = None
        max_num_dirs = 20  # how many directories to scan before bailing
        i = 0
        exclude_dirs = []
        while found is None and i < max_num_dirs:
            try:
                directory = self.find_directory(
                    directory_pattern, resource=reference_resource,
                    exclude=exclude_dirs, lock_timeslot=lock_timeslot,
                    parameter=parameter, parameter_rule=parameter_rule,
                    temporal_rule=temporal_rule
                )
                found = self.find_info(
                    reference_resource, directory,
                    name_pattern=name_pattern,
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

    def find_info(self, resource, directory, name_pattern=r".*",
                  lock_timeslot=None,
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

        pattern = BaseUrlHandler.replace_temporal_specs_with_regex(
            name_pattern.format(resource))
        lock_timeslot = lock_timeslot or []
        if any(lock_timeslot) and lock_timeslot[0] == "all":
            lock_timeslot = [n.lower() for n, m in
                             TemporalPart.__members__.items()]
        self._validate_lock_timeslot_inputs(lock_timeslot)
        self._validate_parameter_input(resource, parameter, parameter_rule)
        candidates_with_timeslot = []
        candidates_without_timeslot = []
        for p in os.listdir(directory):
            if re.search(pattern, p) is not None:
                path_slot = self._extract_path_timeslot(p)
                path_parameters = resource.extract_path_parameters(p)
                if path_slot is not None:
                    valid_slot = self._timeslot_is_valid(
                        path_slot, lock_timeslot, resource.timeslot)
                    if valid_slot:
                        candidates_with_timeslot.append(
                            (p, path_parameters, path_slot))
                else:
                    candidates_without_timeslot.append(
                        (p, path_parameters, None))
        temporal_sorted = sorted(
            candidates_with_timeslot, key=lambda i: i[-1],
            reverse=(temporal_rule == TemporalSelectionRule.LATEST)
        )
        result = temporal_sorted
        if parameter is not None:
            all_candidates = temporal_sorted + candidates_without_timeslot
            parameter_sorted = sorted(
                all_candidates, key=lambda i: i[1][parameter],
                reverse=(parameter_rule == ParameterSelectionRule.HIGHEST)
            )
            result = parameter_sorted
        return result[0] if any(result) else None

