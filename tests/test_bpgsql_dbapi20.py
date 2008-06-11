#!/usr/bin/env python
import dbapi20
import unittest
import bpgsql
import popen2

class test_BPgSQL(dbapi20.DatabaseAPI20Test):
    driver = bpgsql
    connect_args = (['host=10.66.0.1 user=barryp dbname=test'])
    connect_kw_args = {}

    lower_func = 'lower' # For stored procedure test

    def setUp(self):
        # Call superclass setUp In case this does something in the
        # future
        dbapi20.DatabaseAPI20Test.setUp(self) 

        try:
            con = self._connect()
            con.close()
        except:
            cmd = "psql -c 'create database dbapi20_test'"
            cout,cin = popen2.popen2(cmd)
            cin.close()
            cout.read()

    def tearDown(self):
        dbapi20.DatabaseAPI20Test.tearDown(self)

    def test_nextset(self): pass
    def test_setoutputsize(self): pass

if __name__ == '__main__':
    unittest.main()
