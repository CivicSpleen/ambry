
# FIXME. These should not be imported together, since Multicorn is difficult to install on some platforms
# and not required if the user only needs the Sqlite warehouse.
#from .backends.sqlite import SQLiteBackend
#from .backends.postgresql import PostgreSQLBackend
from .core import execute_sql