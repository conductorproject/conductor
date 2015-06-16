"""
Resource finder classes for conductor
"""

import os
import os.path
import re
import logging

from .. import ConductorScheme

logger = logging.getLogger(__name__)


class ResourceFinderFactory(object):

    @staticmethod
    def get_finder(scheme):
        result = {
            ConductorScheme.FILE: FileResourceFinder,
            ConductorScheme.FTP: FtpResourceFinder,
        }.get(scheme)
        return result

resource_finder_factory = ResourceFinderFactory()


class BaseResourceFinder(object):
    """
    Look for Conductor resources

    This class is used in order to locate resources that are dynamic. Dynamic
    resources are the ones that are defined by conditions that are not
    possible to determine a priori. An example is a resource that represents
    the latest LST file that is available. We cannot know the timeslot of
    the latest LST product without actually running the code.

    This class can search the appropriate servers and determine the actual
    parameters in order to create proper Resource instances.
    """
    pass


class FileResourceFinder(BaseResourceFinder):

    def find(self, url, selection_method="latest"):
        """
        Look for a resource in a location
        """

        found = self.select_path(url.path_part,
                                 selection_method=selection_method)
        return found

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
                for c in os.listdir(current_path):
                    if os.path.isdir(os.path.join(current_path, c)):
                        re_obj = re.search(hierarchic_part, c)
                        if re_obj is not None:
                            candidates.append(c)
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
            current_path = current_path if current_path not in except_paths else None
        return current_path


class FtpResourceFinder(BaseResourceFinder):
    pass
