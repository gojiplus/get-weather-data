#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for ZIP2WS

"""

import os
import unittest

from zip2ws.zip2ws import main


class TestZip2Ws(unittest.TestCase):

    def setUp(self):
        try:
            os.unlink('zip2ws/data/zip2ws.sqlite')
        except:
            pass

    def tearDown(self):
        pass

    def test_import(self):
        main(['', '-i'])
        self.assertTrue(os.path.exists('zip2ws/data/zip2ws.sqlite'))

if __name__ == '__main__':
    unittest.main()
