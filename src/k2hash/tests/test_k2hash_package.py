# -*- coding: utf-8 -*-
#
# K2hash Python Driver under MIT License
#
# Copyright (c) 2022 Yahoo Japan Corporation
#
# For the full copyright and license information, please view
# the license file that was distributed with this source code.
#
# AUTHOR:   Hirotaka Wakabayashi
# CREATE:   Tue Feb 08 2022
# REVISION:
#
import unittest
import k2hash
import logging

class TestK2hashPackage(unittest.TestCase):

    def test_get_library_handle(self):
        libk2hash = k2hash.get_library_handle()
        self.assertTrue(libk2hash)
        self.assertTrue(isinstance(libk2hash, dict))
        self.assertTrue(libk2hash['c'])
        self.assertTrue(libk2hash['k2hash'])

    def test_set_log_level(self):
        k2hash.set_log_level(logging.INFO)
        logger = logging.getLogger('k2hash')
        self.assertEqual(logging.getLevelName(logger.level), 'INFO')


if __name__ == '__main__':
    unittest.main()

#
# Local variables:
# tab-width: 4
# c-basic-offset: 4
# End:
# vim600: expandtab sw=4 ts=4 fdm=marker
# vim<600: expandtab sw=4 ts=4
#
