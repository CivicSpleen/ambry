from ambry.dbexceptions import BundleError

__author__ = 'eric'


class DatabaseError(BundleError):

    """A general database error."""


class NotFoundError(DatabaseError):

    """Failed to find resource."""


class MultipleFoundError(DatabaseError):

    """Found multiple when only one was expected."""


class DatabaseMissingError(DatabaseError):

    """A general database error."""


class ConflictError(BundleError):

    """Conflict with existing resource."""


class MetadataError(BundleError):

    """Conflict with existing resource."""