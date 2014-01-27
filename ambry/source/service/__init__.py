"""A Source service is a specific source code control service, like BitBiucket, GitHub, etc
There is a special object for these because they have different interfaces for creating origin repositories

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


def new_service(config):

    if config['service'] == 'github':
        from github import GitHubService #@UnresolvedImport

        return GitHubService(**config)
    else:
        from ambry.dbexceptions import ConfigurationError
        raise ConfigurationError('No source service for name {}'.format(name))


class ServiceException(Exception):
    pass

class ServiceInterface(object):
    
    def create_repository(self):
        '''Create a new upstream repository'''
        raise NotImplemented()  
    
    def ident(self):
        '''Return an identifier for this service'''
        
class GitServiceMarker(object):
    pass