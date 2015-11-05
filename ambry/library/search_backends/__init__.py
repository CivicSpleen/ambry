# -*- coding: utf-8 -*-

# Shortcuts for user convenience
from .whoosh_backend import WhooshSearchBackend
from .sqlite_backend import SQLiteSearchBackend
from .postgres_backend import PostgreSQLSearchBackend

__all__ = [
    WhooshSearchBackend,
    SQLiteSearchBackend,
    PostgreSQLSearchBackend]
