#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for NOAAWEB

"""

import os
import unittest
from noaaweb.noaaweb import main
from . import capture

NCDC_TOKEN = os.environ.get('NCDC_TOKEN', None)

@unittest.skipIf(NCDC_TOKEN is None, 'No NCDC token found in environment.')
class TestNoaaWeb(unittest.TestCase):

    def setUp(self):
        with open('input.csv', 'w') as f:
            f.write("""no,uniqid,zip,year,month,day
2000,2,70503,1999,12,15""")

    def tearDown(self):
        os.unlink('input.csv')
        os.unlink('output.csv')

    def test_noaaweb(self):
        with capture(main, ['', 'input.csv', '-o', 'output.csv']) as output:
            self.assertRegexpMatches(output, r'.*values.*')


if __name__ == '__main__':
    unittest.main()
