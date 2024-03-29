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
import logging
import time
import unittest

import k2hash


class TestQueue(unittest.TestCase):
    def test_Queue_construct(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_put(self):
        db = k2hash.K2hash()
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        self.assertRaises(TypeError, q.put)
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_put_with_obj(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        obj = "v1"
        self.assertTrue(q.put(obj), True)
        self.assertTrue(q.get(), obj)
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_put_with_obj_attrs(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        obj = "v1"
        attrs = {"attrname1": "attrval1"}
        self.assertTrue(q.put(obj, attrs), True)
        self.assertTrue(q.get(), obj)
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_clear(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        obj = "v1"
        self.assertTrue(q.put(obj), True)
        self.assertTrue(q.qsize() == 1)
        self.assertTrue(q.clear(), True)
        self.assertTrue(q.qsize() == 0)
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_close(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_qsize(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        obj = "v1"
        self.assertTrue(q.put(obj), True)
        self.assertTrue(q.qsize() == 1)
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_element(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        obj = "v1"
        self.assertTrue(q.put(obj), True)
        self.assertTrue(q.element(), obj)
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_element_with_position(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        obj = "v1"
        self.assertTrue(q.put(obj), True)
        self.assertTrue(q.element(1) == "")
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_handle(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        self.assertFalse(q.handle == k2hash.K2hash.K2H_INVALID_HANDLE)
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_empty(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        self.assertTrue(q.empty(), True)
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_get(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        obj = "v1"
        self.assertTrue(q.put(obj), True)
        self.assertTrue(q.get(), obj)
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_print(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        self.assertTrue(q.print(), True)
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_remove(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        obj = "v1"
        self.assertTrue(q.put(obj), True)
        self.assertTrue(q.qsize() == 1)
        self.assertTrue(q.remove(), True)
        self.assertTrue(q.qsize() == 0)
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_remove_with_count(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        obj = "v1"
        self.assertTrue(q.put(obj), True)
        self.assertTrue(q.qsize() == 1)
        self.assertTrue(q.remove(1), True)
        self.assertTrue(q.qsize() == 0)
        self.assertTrue(q.close(), True)
        db.close()

    def test_Queue_repr(self):
        db = k2hash.K2hash()
        q = k2hash.Queue(db)
        self.assertTrue(isinstance(q, k2hash.Queue))
        self.assertTrue(q.close(), True)
        db.close()


if __name__ == "__main__":
    unittest.main()

#
# Local variables:
# tab-width: 4
# c-basic-offset: 4
# End:
# vim600: expandtab sw=4 ts=4 fdm=marker
# vim<600: expandtab sw=4 ts=4
#
