"""
BPgSQL unittests

2004-03-29 Barry Pederson <bp@barryp.org>

"""
import unittest
import bpgsql

TEST_DSN = 'host=10.66.0.1 user=barryp dbname=template1'

class ConnectedTests(unittest.TestCase):
    def setUp(self):
        self.cnx = bpgsql.connect(TEST_DSN)
        self.cur = self.cnx.cursor()

    def tearDown(self):
        self.cnx.close()
        self.cnx = self.cur = None


class TypeTests(ConnectedTests):
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
        self.cur.execute("SELECT -7.5, 'hello world', 25345234526565445623, 891")
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 4)
        self.assertEqual(row[0], -7.5)
        self.assertEqual(row[1], 'hello world')
        self.assertEqual(row[2], 25345234526565445623)
        self.assertEqual(row[3], 891)

    def test_numeric(self):
        self.cur.execute("SELECT 1.5")
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], 1.5)

    def test_string(self):
        self.cur.execute("SELECT 'foo'")
        self.assertEqual(self.cur.rowcount, 1)
        row = self.cur.fetchone()
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], 'foo')


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



def main():
    type_tests = unittest.makeSuite(TypeTests, 'test_')
    select_tests = unittest.makeSuite(SelectTests, 'test_')
    suite = unittest.TestSuite((select_tests, type_tests))
    runner = unittest.TextTestRunner()
    runner.run(suite)

if __name__ == "__main__":
    main()