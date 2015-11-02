#!/usr/bin/env python
"""
BPgSQL unittests

2004-03-29 Barry Pederson <bp@barryp.org>

"""
import unittest
from datetime import date, datetime, time
try:
    from decimal import Decimal
except:
    Decimal = float
from optparse import OptionParser
from .. import bpgsql

DEFAULT_DSN = 'host=10.66.0.1 user=barryp dbname=test'

class ConnectedTests(unittest.TestCase):
    """
    Superclass for test suites that need to be connected
    to the backend with a test database.

    """
    TEST_DSN = 'mydsn'

    def setUp(self):
        self.cnx = bpgsql.connect(self.TEST_DSN)
        self.cur = self.cnx.cursor()

    def tearDown(self):
        self.cnx.close()
        self.cnx = self.cur = None


class TableTests(ConnectedTests):
    """
    Superclass for test suites that may create tables.  Make
    sure no test tables exist before test starts, and clean out any
    test tables left over afterwards.

    """
    def __drop_existing(self):
        #
        # Drop any existing test tables
        #
        cur = self.cnx.cursor()
        cur.execute("SELECT tablename FROM pg_tables WHERE tablename LIKE 'test_%'")
        tables = [x[0] for x in cur]
        for t in tables:
            cur.execute("DROP TABLE " + t)

    def setUp(self):
        ConnectedTests.setUp(self)
        self.__drop_existing()

    def tearDown(self):
        self.__drop_existing()
        ConnectedTests.tearDown(self)


class DBAPIInterfaceTests(unittest.TestCase):
    """
    Make sure the module has some basic things required by the DB-API 2.0 spec
        http://www.python.org/peps/pep-0249.html

    Won't bother checking for the presence of methods like connect(), since later
    tests will surely find if they're missing.

    """
    def test_globals(self):
        self.assertEqual(bpgsql.apilevel, '2.0')
        self.assertEqual(bpgsql.threadsafety, 1)
        self.assertEqual(bpgsql.paramstyle, 'pyformat')

    def test_module_exception_hierarchy(self):
        self.assert_(issubclass(bpgsql.Warning, StandardError))
        self.assert_(issubclass(bpgsql.Error, StandardError))
        self.assert_(issubclass(bpgsql.InterfaceError, bpgsql.Error))
        self.assert_(issubclass(bpgsql.DatabaseError, bpgsql.Error))
        self.assert_(issubclass(bpgsql.DataError, bpgsql.DatabaseError))
        self.assert_(issubclass(bpgsql.OperationalError, bpgsql.DatabaseError))
        self.assert_(issubclass(bpgsql.IntegrityError, bpgsql.DatabaseError))
        self.assert_(issubclass(bpgsql.InternalError, bpgsql.DatabaseError))
        self.assert_(issubclass(bpgsql.ProgrammingError, bpgsql.DatabaseError))
        self.assert_(issubclass(bpgsql.NotSupportedError, bpgsql.DatabaseError))



class InternalDSNParserTests(unittest.TestCase):
    """
    Test the internal parser that handles connection-info strings.

    """
    def test_blank(self):
        self.assertEqual(bpgsql._parseDSN(None), {})
        self.assertEqual(bpgsql._parseDSN(''), {})

    def test_single(self):
        d = bpgsql._parseDSN('foo=bar')
        self.assertEqual(len(d), 1)
        self.assertEqual(d['foo'], 'bar')

    def test_spaced(self):
        d = bpgsql._parseDSN("foo='bar with space'")
        self.assertEqual(len(d), 1)
        self.assertEqual(d['foo'], 'bar with space')

    def test_multiple(self):
        d = bpgsql._parseDSN("abc=xyz foo='bar with space' rst= uvw i='1 2 3' j = '21 32 abc'")
        self.assertEqual(len(d), 5)
        self.assertEqual(d['abc'], 'xyz')
        self.assertEqual(d['foo'], 'bar with space')
        self.assertEqual(d['rst'], 'uvw')
        self.assertEqual(d['i'], '1 2 3')
        self.assertEqual(d['j'], '21 32 abc')


class TypeTests(ConnectedTests):

    def test_binary(self):
        b = bpgsql.Binary(''.join([chr(x) for x in range(256)]))
        self.cur.execute(r"SELECT %s, 'foo'::bytea", (b,))
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 2)
        self.assertEqual(row[0], b)
        self.assertEqual(row[1], 'foo')
        self.assertEqual(isinstance(row[1], bpgsql.Binary), True)

    def test_boolean(self):
        self.cur.execute("SELECT True")
        row = self.cur.fetchone()
        self.assertEqual(row[0], True)
        self.cur.execute("SELECT False")
        row = self.cur.fetchone()
        self.assertEqual(row[0], False)

    def test_date(self):
        d1 = date.today()
        d2 = date(1986, 1, 21)
        self.cur.execute("SELECT '2008-06-11'::date, %s, %s", (d1, d2))
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 3)
        self.assertEqual(row[0], date(2008, 6, 11))
        self.assertEqual(row[1], d1)
        self.assertEqual(row[2], d2)

    def test_time(self):
        now = datetime.now()
        t1 = time(now.hour, now.minute, now.second, now.microsecond)
        t2 = time(1, 2, 3, 8765)
        self.cur.execute("SELECT %s, %s, current_time", (t1, t2))
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 3)
        self.assertEqual(row[0], t1)
        self.assertEqual(row[1], t2)
        self.assertEqual(type(row[2]), time)
        #
        # PgSQL current_time should have a timezone attached, check
        # if that survives a roundtrip
        #
        pgsql_now = row[2]
        self.cur.execute("SELECT %s", (pgsql_now,))
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], pgsql_now)


    def test_datetime(self):
        d1 = datetime.now()
        d2 = datetime(2008, 12, 31, 15, 21, 17)
        self.cur.execute("SELECT %s, %s, now()", (d1, d2))
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 3)
        self.assertEqual(row[0], d1)
        self.assertEqual(row[1], d2)
        self.assertEqual(type(row[2]), datetime)
        #
        # PgSQL now() should have a timezone attached, check
        # if that survives a roundtrip
        #
        pgsql_now = row[2]
        self.cur.execute("SELECT %s", (pgsql_now,))
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], pgsql_now)

    def test_float(self):
        self.cur.execute("SELECT sin(0) - 0.5")
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], -0.5)

    def test_integer(self):
        self.cur.execute("SELECT -273")
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], -273)

    def test_long(self):
        self.cur.execute("SELECT trunc(pow(2,40))")
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], 2**40)

    def test_multiple(self):
        self.cur.execute("SELECT -7.5, 'hello world', 253452345265654456, 891")
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 4)
        self.assertEqual(row[0], Decimal('-7.5'))
        self.assertEqual(row[1], 'hello world')
        self.assertEqual(row[2], 253452345265654456)
        self.assertEqual(row[3], 891)

    def test_numeric(self):
        d = Decimal('3.14159')
        self.cur.execute("SELECT 1.5, %s", (d,))
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 2)
        self.assertEqual(row[0], Decimal('1.5'))
        self.assertEqual(row[1], d)

    def test_string(self):
        s = r"abc'def''ghi''''\'\r\n"
        t = ''.join([chr(x) for x in range(1,128)])
        self.cur.execute(r"SELECT 'foo', '\bar', %s, %s", (s, t))
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 4)
        self.assertEqual(row[0], 'foo')
        self.assertEqual(row[1], r'\bar')
        self.assertEqual(row[2], s)
        self.assertEqual(row[3], t)

    def test_unicode(self):
        #
        # Unicode command
        #
        self.cur.execute(u"SELECT 'Hello\u1234World!'")
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], u'Hello\u1234World!')

        #
        # String command with unicode arg
        #
        self.cur.execute('SELECT %s', (u'Hello\u1234World!',))
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], u'Hello\u1234World!')

        #
        # Unicode command with string arg
        #
        self.cur.execute(u'SELECT %s', ('Hello World!',))
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], 'Hello World!')

        #
        # Unicode command with unicode arg
        #
        self.cur.execute(u'SELECT %s', (u'Hello\u1234World!',))
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], u'Hello\u1234World!')


class SelectTests(ConnectedTests):
    def test_description(self):
        self.cur.execute("SELECT oid, typname, typlen, typtype  from pg_type")
        self.assertEqual(len(self.cur.description), 4)
        self.assertEqual(self.cur.description[0][0], 'oid')
        self.assertEqual(self.cur.description[1][0], 'typname')
        self.assertEqual(self.cur.description[2][0], 'typlen')
        self.assertEqual(self.cur.description[3][0], 'typtype')

    def test_rowcount(self):
        self.cur.execute("SELECT * from pg_type limit 5")
        self.assertEqual(self.cur.rowcount, 5)
        rows = self.cur.fetchall()
        self.assertEqual(len(rows), 5)


class CursorTests(ConnectedTests):
    def test_close(self):
        self.cur.close()
        self.assertEqual(self.cur.connection, None)

        # a closed cursor should otherwise act just like an unused one
        self.test_initial_properties()


    def test_connection(self):
        self.assertEqual(self.cur.connection, self.cnx)


    def test_initial_properties(self):
        """
        Check the properties and behavior of a cursor that hasn't executed anything yet
        """
        self.assertEqual(self.cur.arraysize, 1)
        self.assertEqual(self.cur.description, None)
        self.assertEqual(self.cur.messages, [])
        self.assertEqual(self.cur.rowcount, -1)
        self.assertEqual(self.cur.rownumber, None)

        self.assertRaises(bpgsql.Error, self.cur.fetchone)
        self.assertRaises(bpgsql.Error, self.cur.fetchmany)
        self.assertRaises(bpgsql.Error, self.cur.fetchmany, 10)
        self.assertRaises(bpgsql.Error, self.cur.fetchall)
        self.assertRaises(bpgsql.Error, self.cur.next)


    def test_scroll(self):
        """
        Dance around the result set using the scroll() method

        """
        self.cur.execute("SELECT *  from pg_type")
        self.assertEqual(self.cur.rownumber, 0)

        self.cur.scroll(1)
        self.assertEqual(self.cur.rownumber, 1)

        self.cur.scroll(3, 'relative')
        self.assertEqual(self.cur.rownumber, 4)

        self.cur.scroll(-2)
        self.assertEqual(self.cur.rownumber, 2)

        self.cur.scroll(-1, 'relative')
        self.assertEqual(self.cur.rownumber, 1)

        self.cur.scroll(0)
        self.assertEqual(self.cur.rownumber, 1)

        self.cur.scroll(0, 'relative')
        self.assertEqual(self.cur.rownumber, 1)

        self.cur.scroll(0, 'absolute')
        self.assertEqual(self.cur.rownumber, 0)

        self.cur.scroll(7, 'absolute')
        self.assertEqual(self.cur.rownumber, 7)

        self.cur.scroll(5, 'absolute')
        self.assertEqual(self.cur.rownumber, 5)

        self.assertRaises(IndexError, self.cur.scroll, -1, 'absolute')
        self.assertEqual(self.cur.rownumber, 5)

        self.cur.scroll(2)
        self.assertEqual(self.cur.rownumber, 7)

        self.assertRaises(IndexError, self.cur.scroll, self.cur.rowcount)
        self.assertEqual(self.cur.rownumber, 7)

        self.cur.scroll(self.cur.rowcount-1, 'absolute')
        self.assertEqual(self.cur.rownumber, self.cur.rowcount-1)

        self.assertNotEqual(self.cur.fetchone(), None)  # Should be the last row
        self.assertEqual(self.cur.fetchone(), None)     # Should be no more rows
        self.assertEqual(self.cur.fetchone(), None)     # Should still be no more rows


class BasicTableTests(TableTests):
        def test_create_existing(self):
            #
            # Creating a table with a name that already exists should raise an error
            #
            self.cur.execute("CREATE TABLE test_foo (id integer, name text)")
            self.assertRaises(bpgsql.Error, self.cur.execute, "CREATE TABLE test_foo (id integer, name text)")

        def test_fill_table(self):
            self.cur.execute("CREATE TABLE test_foo (id integer, name text)")
            for i in range(100):
                self.cur.execute("INSERT INTO test_foo (id, name) VALUES (%d, %s)", (i, 'bar-' + str(i)))

            self.cur.execute("SELECT * FROM test_foo")
            self.assertEqual(self.cur.rowcount, 100)

            self.cur.scroll(99)
            row = self.cur.fetchone()
            self.assertEqual(row[1], 'bar-99')


class LargeObjectTests(ConnectedTests):
        def test_lobj(self):
            self.cur.execute("BEGIN")
            loid = self.cnx.lo_create()

            o = self.cnx.lo_open(loid, bpgsql.INV_WRITE)
            o.write('hello')
            o.write('world')
            o.close()

            o = self.cnx.lo_open(loid, bpgsql.INV_READ)
            s = o.read(4096)
            self.assertEqual(s, 'helloworld')

            o.seek(5, bpgsql.SEEK_SET)
            s = o.read(4096)
            self.assertEqual(s, 'world')

            o.seek(-2, bpgsql.SEEK_END)
            s = o.read(4096)
            self.assertEqual(s, 'ld')

            o.seek(5, bpgsql.SEEK_SET)
            o.seek(-2, bpgsql.SEEK_CUR)
            s = o.read(2)
            self.assertEqual(s, 'lo')

            self.assertEquals(o.tell(), 5)

            o.close()

            self.cnx.lo_unlink(loid)
            self.cnx.commit()


def main():
    parser = OptionParser(usage='usage: %prog [options]')
    parser.add_option('--dsn', dest='dsn',
                        help='DataSource Name to use for test (default: %default)',
                        default=DEFAULT_DSN)

    options, args = parser.parse_args()
    ConnectedTests.TEST_DSN = options.dsn

    all_tests = []
    all_tests.append(unittest.makeSuite(DBAPIInterfaceTests, 'test_'))
    all_tests.append(unittest.makeSuite(InternalDSNParserTests, 'test_'))
    all_tests.append(unittest.makeSuite(TypeTests, 'test_'))
    all_tests.append(unittest.makeSuite(SelectTests, 'test_'))
    all_tests.append(unittest.makeSuite(CursorTests, 'test_'))
    all_tests.append(unittest.makeSuite(BasicTableTests, 'test_'))
    all_tests.append(unittest.makeSuite(LargeObjectTests, 'test_'))

    suite = unittest.TestSuite(all_tests)

    runner = unittest.TextTestRunner()
    runner.run(suite)

if __name__ == "__main__":
    main()
