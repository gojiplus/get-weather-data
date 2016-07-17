#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for ZIP2WD

"""

import unittest

from zip2wd.zip2wd import WeatherByZip
from argparse import Namespace


class TestZip2Wd(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_search(self):
        args = Namespace()
        args.dbpath = 'zip2wd/data'
        args.uses_sqlite = 'yes'
        args.nth = 0
        args.distance = 30
        args.columns = 'column-names.txt'
        args.zip2ws_db = 'zip2ws/data/zip2ws.sqlite'
        weather = WeatherByZip(args)
        z = {'uniqid': '1', 'zip': '10451', 'from.year': 1877, 'from.month': 12,
             'from.day': 15, 'to.year': 1877, 'to.month': 12,
             'to.day': 15}
        result = weather.search(z)
        self.assertEqual(result[0]['TMIN'], '6')


if __name__ == '__main__':
    unittest.main()
