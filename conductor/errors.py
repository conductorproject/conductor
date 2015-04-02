"""
Custom Exception classes for conductor
"""

class ConductorError(Exception):
    pass


class InvalidSettingsError(ConductorError):
    pass


class InvalidFTPHostError(ConductorError):
    pass


class RunModeError(ConductorError):
    pass


class ExecutionCannotStartError(ConductorError):
    pass


class InvalidExecutionError(ConductorError):
    pass
