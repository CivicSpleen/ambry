"""The Bundle object is the root object for a bundle, which includes accessors
for partitions, schema, and the filesystem.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from ..util import get_logger

class Bundle(object):

    def __init__(self, dataset, fs=None):
        import logging
        from states import StateMachine

        self._dataset = dataset
        self._logger = None
        self._log_level = logging.INFO
        self._fs = fs
        self._state_machine_class = StateMachine

    @property
    def dataset(self):
        from sqlalchemy import inspect

        if inspect(self._dataset).detached:
            self._dataset = self._dataset._database.dataset(self._dataset.vid)

        return self._dataset

    @property
    def schema(self):
        """Return the Schema acessor"""
        pass

    @property
    def sources(self):
        """Return the Sources acessor"""

    @property
    def metadata(self):
        """Return the Metadata acessor"""

    @property
    def builder(self):
        """Return the build state machine"""

        return self._state_machine_class(self)

    def source_files(self, fs):
        from files import BuildSourceFileAccessor
        return BuildSourceFileAccessor(self.dataset, fs)

    @property
    def logger(self):
        """The bundle logger."""
        import sys
        import os

        if not self._logger:

            try:
                ident = self.identity
                template = "%(levelname)s " + ident.sname + " %(message)s"

                if self.run_args.multi > 1:
                    template = "%(levelname)s " + ident.sname + " %(process)s %(message)s"

            except:
                template = "%(message)s"

            self._logger = get_logger(__name__, template=template, stream=sys.stdout)

            self._logger.setLevel(self._log_level)

        return self._logger

    def log(self, message, **kwargs):
        """Log the messsage."""

        self.logger.info(message)

    def error(self, message):
        """Log an error messsage.

        :param message:  Log message.

        """
        if message not in self._errors:
            self._errors.append(message)
        self.logger.error(message)

    def warn(self, message):
        """Log an error messsage.

        :param message:  Log message.

        """
        if message not in self._warnings:
            self._warnings.append(message)

        self.logger.warn(message)

    def fatal(self, message):
        """Log a fatal messsage and exit.

        :param message:  Log message.

        """
        import sys

        self.logger.fatal(message)
        sys.stderr.flush()
        if self.exit_on_fatal:
            sys.exit(1)
        else:
            from ..dbexceptions import FatalError

            raise FatalError(message)

    #
    # Build Process
    #

    def sync(self):
        if not self._fs:
            from ..dbexceptions import ConfigurationError
            raise ConfigurationError("Can't sync bundle without a configured filesystem")

        return self.builder.sync(self._fs)

    def clean(self):
        return self.builder.do_clean()

    def prepare(self):
        return self.builder.do_prepare()