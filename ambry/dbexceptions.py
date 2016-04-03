"""Common exception objects.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

# Deprecated, use exc instead.


class RequirementError(ImportError):

    """Thrown for required optional modules, such as gdal."""


class BundleError(Exception):

    def __init__(self, message, *args, **kwargs):

        # Call the base class constructor with the parameters it needs
        # Exception.__init__(self,textwrap.fill(message, 120), *args, **kwargs)
        Exception.__init__(self, message, *args, **kwargs)


class LoggedException(Exception):
    """Signal that an exception has been logged and handled, and should not be logged again"""

    def __init__(self, exc, bundle):
        self.exc = exc
        self.bundle = bundle

        Exception.__init__(self, str(exc))

class BadRequest(BundleError):

    """The function call or request was malformed or incorrect."""


class ProcessError(BundleError):

    """Error in the configuration files."""


class ObjectStateError(BundleError):

    """Object not put into appropriate state before uses."""


class ConfigurationError(BundleError):

    """Error in the configuration files."""


class ResultCountError(BundleError):

    """Got too many or too few results."""


class FilesystemError(BundleError):

    """Missing file, etc."""


class DependencyError(Exception):

    """Required bundle dependencies not satisfied."""


class NoLock(BundleError):

    """Error in the configuration files."""


class Locked(BundleError):

    """Error in the configuration files."""


class LockedFailed(BundleError):

    """Error in the configuration files."""


class QueryError(BundleError):

    """Error while executing a query."""


class SyncError(BundleError):

    """Could not sync a resource."""


class NotABundle(BundleError):

    """The referenced object is not a valid bundle, usually because of a non
    existent or malformed database."""


class FatalError(BundleError):

    """A Fatal Bundle Error, generated in testing instead of a system exit."""

class GeoError(Exception):

    """General error doing geographic processing."""



class PhaseError(Exception):

    def __init__(self, *args, **kwargs):
        """General error while running a pipeline phase."""
        super(PhaseError, self).__init__(*args, **kwargs)

        self.exception = kwargs.get('exception', None)
        self.phase = kwargs.get('phase', None)
        self.stage = kwargs.get('stage', None)


class BuildError(PhaseError):

    """General error while building a bundle."""

class IngestionError(PhaseError):


    """General error while ingesting sources."""


class AccessError(PhaseError):

    """Could not access a remote resource"""

class TestError(PhaseError):

    """Error or failure in built in tests"""



