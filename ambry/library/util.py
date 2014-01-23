"""A Library is a local collection of bundles. It holds a database for the configuration
of the bundles that have been installed into it.
"""

# Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt


# Setup a default logger. The logger is re-assigned by the
# bundle when the bundle instantiates the logger.
import logging
import logging.handlers

import threading
import time

class DumperThread (threading.Thread):
    """Run a thread for a library to try to dump the database to the retome at regular intervals"""

    lock = threading.Lock()

    def __init__(self,library):

        self.library = library
        threading.Thread.__init__(self)
        #self.daemon = True
        self.library.logger.setLevel(logging.DEBUG)
        self.library.logger.debug("Initialized Dumper")

    def run (self):

        self.library.logger.debug("Run Dumper")

        if not self.library.upstream:
            self.library.logger.debug("No remote")
            return

        with DumperThread.lock:

            time.sleep(5)

            backed_up = self.library.backup()

            if backed_up:
                self.library.logger.debug("Backed up database")
            else:
                self.library.logger.debug("Did not back up database")
