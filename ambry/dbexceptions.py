"""Common exception objects

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import textwrap

class BundleError(Exception):
    def __init__(self, message, *args, **kwargs):

        # Call the base class constructor with the parameters it needs
        #Exception.__init__(self,textwrap.fill(message, 120), *args, **kwargs)
        Exception.__init__(self, message, *args, **kwargs)


class BadRequest(BundleError):
    '''The functioncall or request was malformed or incorrect'''

class ProcessError(BundleError):
    '''Error in the configuration files'''

class ObjectStateError(BundleError):
    '''Object not put into appropriate state before uses'''

class ConfigurationError(BundleError):
    '''Error in the configuration files'''
    
class ResultCountError(BundleError):
    '''Got too many or too few results'''
    
class FilesystemError(BundleError):
    '''Missing file, etc. '''

class NotFoundError(BundleError):
    '''Failed to find resource'''
    
class DependencyError(Exception):
    """Required bundle dependencies not satisfied"""

class NoLock(BundleError):
    '''Error in the configuration files'''

class Locked(BundleError):
    '''Error in the configuration files'''

class QueryError(BundleError):
    """Error while executing a query"""

class ConflictError(BundleError):
    """Conflict with existing resource"""


class SyncError(BundleError):
    """Could not sync a resource"""

class FatalError(BundleError):
    """A Fatal Bundle Error, generated in testing instead of a system exit. """

class DatabaseError(BundleError):
    """A general database error """

class DatabaseMissingError(DatabaseError):
    """A general database error """