"""
Base implementation for URLhandlers for conductor
"""

import os
import os.path
import re
import logging

import dateutil

from .. import (ParameterSelectionRule, TemporalPart)

logger = logging.getLogger(__name__)


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
        if spec is None:
            # lets check for the timeslot_string
            pattern = r"timeslot_string"
            re_obj = re.search(pattern, fragment)
            if re_obj is not None:
                spec = "timeslot_string"
                re_pattern = r"\d{12}"
        return spec, format_string, re_pattern

    @staticmethod
    def replace_temporal_specs_with_regex(fragment):
        """
        Replace temporal pattern placeholders with regular expressions

        This method will scan a string and replace all occurrences of special
        timeslot strings with their correspondent regular expression patterns.
        It is useful to transform a conductor pattern string into a normal
        regular expression

        :param fragment:
        :return:
        """

        f = fragment
        temporal_fragments = list(TemporalPart) + ["timeslot_string"]
        for t in temporal_fragments:
            spec, format_string, re_pattern = \
                BaseUrlHandler.extract_temporal_spec(f)
            if spec is not None:
                re_pattern = re_pattern if re_pattern not in ("", None) \
                    else ".*?"
                f = re.sub(r"{{0\.(timeslot\.)?{}(.*?)?}}".format(spec),
                           re_pattern, f)
        return f

