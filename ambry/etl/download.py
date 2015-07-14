"""Download from the web, cache the results, and possibly unzip

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

class _DownloadedPath(str):
    """A String for holding path names that is augmented describe wether the file was downloaded to
    found in the cache. """

    def __new__(cls, string):
        ob = super(_DownloadedPath, cls).__new__(cls, string)
        return ob

    def __init__(self, string, from_net = True):
        super(_DownloadedPath, self).__init__(string)
        self.from_net = from_net







