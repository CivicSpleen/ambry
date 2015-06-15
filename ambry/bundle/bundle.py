"""The Bundle object is the root object for a bundle, which includes accessors
for partitions, schema, and the filesystem.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""

from ..util import get_logger

class Bundle(object):

    def __init__(self, dataset, library, source_fs=None, build_fs=None):
        import logging
        from states import StateMachine

        self._dataset = dataset
        self._library = library
        self._logger = None

        self._log_level = logging.INFO
        self._source_fs = source_fs
        self._build_fs = build_fs
        self._state_machine_class = StateMachine
        self._errors = []

    def cast_to_subclass(self):
        from ambry.orm import File
        mod = self.source_files.file(File.BSFILE.BUILD).import_file()
        return mod.Bundle(self._dataset, self._library, self._source_fs, self._build_fs)


    @property
    def dataset(self):
        from sqlalchemy import inspect

        if inspect(self._dataset).detached:
            vid = self._dataset.vid
            self._dataset = self._dataset._database.dataset(vid)

        return self._dataset

    @property
    def identity(self):
        return self.dataset.identity

    @property
    def schema(self):
        """Return the Schema acessor"""
        from schema import Schema
        return Schema(self)

    @property
    def partitions(self):
        """Return the Schema acessor"""
        from partitions import Partitions
        return Partitions(self)

    @property
    def sources(self):
        """Return the Sources acessor"""
        from sources import SourceFilesAcessor
        return SourceFilesAcessor(self)

    @property
    def metadata(self):
        """Return the Metadata acessor"""
        return self.dataset.config.metadata

    @property
    def builder(self):
        """Return the build state machine"""

        return self._state_machine_class(self)

    @property
    def source_files(self):
        from files import BuildSourceFileAccessor
        return BuildSourceFileAccessor(self.dataset, self._source_fs)

    @property
    def logger(self):
        """The bundle logger."""
        import sys

        if not self._logger:

            ident = self.identity
            template = "%(levelname)s " + ident.sname + " %(message)s"

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
        """
        Synchronize with filesystem files.
        :return: True if the synchronization succeedes
        """
        if not self._source_fs:
            from ..dbexceptions import ConfigurationError
            raise ConfigurationError("Can't sync bundle without a configured filesystem")

        return self.builder.sync()

    def clean(self):
        """
        Remove all objects generated synchronized files.
        :return:
        """
        return self.builder.do_clean()

    def prepare(self):
        """Create objects from schonized files and prepare the bundle for building"""

        return self.builder.do_prepare()

    def build(self):
        """Create objects from schonized files and prepare the bundle for building"""

        return self.builder.do_build()