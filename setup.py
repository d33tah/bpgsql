#!/usr/bin/env python
"""
Python Distutils setup for for bpgsql.  Build and install with

    python setup.py install

2008-06-10 Barry Pederson <bp@barryp.org>

"""

import sys
from distutils.core import setup

setup(name = "bpgsql",
      description = "Barebones pure-Python PostGreSQL client",
      version = "1.3",
      license = "LGPL",
      author = "Barry Pederson",
      author_email = "bp@barryp.org",
      url = "http://barryp.org/software/bpgsql/",
      py_modules = ['bpgsql']
     )
