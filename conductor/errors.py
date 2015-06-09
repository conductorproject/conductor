"""
Custom Exception classes for conductor
"""

class ConductorError(Exception):
    pass


class ResourceNotFoundError(ConductorError):
    pass


class LocalPathNotFoundError(ConductorError):
    pass


class InvalidUserCredentialsError(ConductorError):
    pass


class InvalidSchemeError(ConductorError):
    pass


class HostNotFoundError(ConductorError):
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
