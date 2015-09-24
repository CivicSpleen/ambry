"""
Calss to generate View contexts from the library database for rendering views
"""

# Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt


class ContextGenerator(object):

    def __init__(self, library, session = None):

        self.library = library
        self.session = session



