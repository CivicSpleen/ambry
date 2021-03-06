

class MPRError(Exception):
    """ Base class for all warehouse errors. """
    pass

class BadSQLError(MPRError):
    """ Something is wrong with an SQL query """
    pass


class MissingTableError(MPRError):
    """ Thrown if database does not have table for MPR of the partition. """
    pass


class MissingViewError(MPRError):
    """ Thrown if database does not have view for MPR of the partition. """
    pass
