"""
PostgreSQL database backend for Django.

Requires bpgsql: http://barryp.org/software/bpgsql

"""

from django.db.backends import *
from django.db.backends.postgresql.operations import DatabaseOperations as PostgresqlDatabaseOperations
from django.db.backends.postgresql.client import DatabaseClient
from django.db.backends.postgresql.creation import DatabaseCreation
from django.db.backends.postgresql.version import get_version
from django.db.backends.postgresql.introspection import DatabaseIntrospection

# Quick-and-dirty tracing of SQL activity
def debuglog(msg):
#    open('/tmp/bpgsql.log', 'a').write(msg)
    return

try:
    import bpgsql
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading bpgsql module: %s" % e)

DatabaseError = bpgsql.DatabaseError
IntegrityError = bpgsql.IntegrityError

class DatabaseFeatures(BaseDatabaseFeatures):
    needs_datetime_string_cast = False
    uses_savepoints = False

class DatabaseOperations(PostgresqlDatabaseOperations):
    def last_executed_query(self, cursor, sql, params):
        # With bpgsql, cursor objects have a "query" attribute that is the
        # exact query sent to the database.
        return cursor.query

CHANGE_OPS = ['select', 'insert', 'update', 'delete']
TRANSACTION_OPS = ['begin', 'commit', 'rollback']

def _wrapped_time_to_python(timepart):
    return bpgsql._time_to_python(timepart).replace(tzinfo=None)

def _wrapped_timestamp_to_python(s):
    return bpgsql._timestamp_to_python(s).replace(tzinfo=None)

class CursorWrapper(bpgsql.Cursor):
    """
    Override bpgsql.Cursor to change a few behaviors to
    satisfy Django unittests.

    """
    def fetchmany(self, size=None):
        """
        Make the result of fetchmany() a list of tuples
        instead of a list of lists.

        """
        return [tuple(x) for x in bpgsql.Cursor.fetchmany(self, size)]

    def fetchone(self):
        """
        Make the result of fetchone() a tuple instead of a list

        """
        result = bpgsql.Cursor.fetchone(self)
        if result is not None:
            result = tuple(result)
        return result


class ConnectionWrapper(bpgsql.Connection):
    """
    Wrapper around bpgsql.Connection to simulate the non-autocommit
    behavior that Django seems to expect from psycopg, and to cause
    bpgsql to return timezone-naive datetime and time objects.

    """
    def __init__(self, *args, **kwargs):
        self.django_needs_begin = True
        bpgsql.Connection.__init__(self, *args, **kwargs)

    def _execute(self, cmd, args=None):
        operation = cmd.split(' ', 1)[0].lower()
        if self.django_needs_begin and operation in CHANGE_OPS:
            bpgsql.Connection._execute(self, 'BEGIN')
            self.django_needs_begin = False
        if (not self.django_needs_begin) and (operation not in CHANGE_OPS) and (operation not in TRANSACTION_OPS):
            bpgsql.Connection._execute(self, 'COMMIT')
            debuglog('>>FORCED COMMIT\n')
            self.django_needs_begin = True

        result = bpgsql.Connection._execute(self, cmd, args)

        # Django expects some DatabaseErrors to be more specifically
        # identified as IntegrityErrors, If the word 'violates' is in
        # the error message, then guess that it's an IntegrityError
        #
        if isinstance(result.error, DatabaseError) and ('violates' in result.error.message):
            result.error = IntegrityError(result.error.message)

        debuglog(repr(result.query)+'\n')
        return result

    def _initialize_types(self):
        bpgsql.Connection._initialize_types(self)

        # Override two of the default bpgsql converters to return timezone naive values
        self.register_pgsql('timetz', _wrapped_time_to_python, bpgsql.DATETIME)
        self.register_pgsql('timestamptz', _wrapped_timestamp_to_python, bpgsql.DATETIME)

    def commit(self):
        bpgsql.Connection.commit(self)
        self.django_needs_begin = True
        debuglog('>>COMMIT\n')

    def cursor(self):
        return CursorWrapper(self)

    def rollback(self):
        bpgsql.Connection.rollback(self)
        self.django_needs_begin = True
        debuglog('>>ROLLBACK\n')

class DatabaseWrapper(BaseDatabaseWrapper):
    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': 'LIKE %s',
        'icontains': 'LIKE UPPER(%s)',
        'regex': '~ %s',
        'iregex': '~* %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE UPPER(%s)',
        'iendswith': 'LIKE UPPER(%s)',
    }

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        self.features = DatabaseFeatures()
        self.ops = DatabaseOperations()
        self.client = DatabaseClient()
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = BaseDatabaseValidation()

    def _cursor(self, settings):
        set_tz = False
        if self.connection is None:
            set_tz = True
            if settings.DATABASE_NAME == '':
                from django.core.exceptions import ImproperlyConfigured
                raise ImproperlyConfigured("You need to specify DATABASE_NAME in your Django settings file.")
            conn_string = "dbname=%s" % settings.DATABASE_NAME
            if settings.DATABASE_USER:
                conn_string = "user=%s %s" % (settings.DATABASE_USER, conn_string)
            if settings.DATABASE_PASSWORD:
                conn_string += " password='%s'" % settings.DATABASE_PASSWORD
            if settings.DATABASE_HOST:
                conn_string += " host=%s" % settings.DATABASE_HOST
            if settings.DATABASE_PORT:
                conn_string += " port=%s" % settings.DATABASE_PORT
            self.connection = ConnectionWrapper(conn_string, **self.options)
        cursor = self.connection.cursor()
        cursor.tzinfo_factory = None
        if set_tz:
            cursor.execute("SET TIME ZONE %s", [settings.TIME_ZONE])
            if not hasattr(self, '_version'):
                self.__class__._version = get_version(cursor)
            if self._version < (8, 0):
                # No savepoint support for earlier version of PostgreSQL.
                self.features.uses_savepoints = False
        return cursor
