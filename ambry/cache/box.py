"""Cache for reading and writing to Box.com files.

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


###
### ! Use the Box webdav interface!
### https://support.box.com/hc/en-us/articles/200519748-Does-Box-support-WebDAV-
### This probably has a much simpler authentication interface, since the
### credentials are set by the user calling it, rather than through OAuth.