

class WarehouseError(Exception):
    """ Base class for all warehouse errors. """
    pass


class MissingTableError(WarehouseError):
    """ Raises if warehouse does not have table for the partition. """
    pass


class MissingViewError(WarehouseError):
    """ Raises if warehouse does not have view associated with the table. """
    pass
