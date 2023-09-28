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
import ctypes
import logging
import time
import unittest

import k2hash


class TestK2hashIterator(unittest.TestCase):
    def test_K2hashIterator_construct(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        ki = k2hash.K2hashIterator(db)
        self.assertTrue(isinstance(ki, k2hash.K2hashIterator))
        self.assertTrue(key == next(ki))
        db.close()

    def test_K2hashIterator_construct_key(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        subkey = "subkey1"
        subval = "subval1"
        self.assertTrue(db.add_subkey(key, subkey, subval), True)
        ki = k2hash.K2hashIterator(db, key)
        self.assertTrue(isinstance(ki, k2hash.K2hashIterator))
        self.assertTrue(subkey == next(ki))
        db.close()


class TestK2hash(unittest.TestCase):
    def test_K2hash_construct(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        db.close()

    def test_K2hash_get_iterator(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        ki = db.get_iterator()
        # Note: handle should be undefined before setting no keys.
        self.assertTrue(isinstance(ki, k2hash.K2hashIterator))
        db.close()

    def test_K2hash_set(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        db.close()

    def test_K2hash_get(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        db.close()

    @unittest.skip("skipping because no plugin lib prepared")
    def test_K2hash_add_attribute_plugin_lib(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        db.close()

    def test_K2hash_add_decryption_password(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        password = "secretstring"
        self.assertTrue(db.add_decryption_password(password), True)
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key, password), val)
        self.assertTrue(db.get(key), "")
        db.close()

    def test_K2hash_add_subkey(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        subkey = "subkey"
        subval = "subval"
        self.assertTrue(db.set(subkey, subval), True)
        self.assertTrue(db.get(subkey), subval)
        self.assertTrue(db.add_subkey(key, subkey, subval), True)
        self.assertTrue(db.get_subkeys(key) == [subkey])
        db.close()

    def test_K2hash_begin_tx(self):
        db = k2hash.K2hash()
        # for debugging, uncomment the following line.
        # db.set_log_level(k2hash.LogLevel.ERROR)
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        k2h_tx_log = "test.log"
        self.assertTrue(db.begin_tx(k2h_tx_log), True)
        # TODO how to check whether transaction is enabled.
        db.close()

    def test_K2hash_close(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        self.assertTrue(db.close(), True)

    def test_K2hash_create(self):
        k2h_file = "test.k2h"
        self.assertTrue(k2hash.K2hash.create(k2h_file), True)

    def test_K2hash_dump_to_file(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        k2h_file = "test.k2h"
        self.assertTrue(db.dump_to_file(k2h_file), val)
        db.close()

    def test_K2hash_enable_encryption(self):
        db = k2hash.K2hash()
        # db.set_log_level(k2hash.LogLevel.ERROR)
        self.assertTrue(isinstance(db, k2hash.K2hash))
        password = "secretstring"
        # Calls set_default_encryption_password before calling enable_encryption
        self.assertTrue(db.set_default_encryption_password(password), True)
        self.assertTrue(db.enable_encryption(), True)
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key, password), val)
        # for debugging, uncomment the following line.
        db.close()

    def test_K2hash_enable_history(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        self.assertTrue(db.enable_history(), True)
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        db.close()

    def test_K2hash_enable_mtime(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        self.assertTrue(db.enable_mtime(), True)
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        db.close()

    def test_K2hash_get_attributes(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        attr_key = "attrkey1"
        attr_val = "attrval1"
        attrs = {attr_key: attr_val}
        self.assertTrue(db.set_attribute(key, attr_key, attr_val), True)
        self.assertTrue(db.get_attributes(key), attrs)
        db.close()

    def test_K2hash_handle(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        # TODO insufficient check
        self.assertTrue(db.handle != k2hash.K2hash.K2H_INVALID_HANDLE)
        db.close()

    def test_K2hash_get_subkeys(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        subkey = "subkey"
        subval = "subval"
        self.assertTrue(db.set(subkey, subval), True)
        self.assertTrue(db.get(subkey), subval)
        self.assertTrue(db.add_subkey(key, subkey, subval), True)
        self.assertTrue(db.get_subkeys(key) == [subkey])
        db.close()

    def test_K2hash_get_tx_file_fd(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        k2h_tx_log = "test.log"
        self.assertTrue(db.begin_tx(k2h_tx_log), True)
        # TODO how to check whether transaction is enabled.
        self.assertTrue(db.get_tx_file_fd() != k2hash.K2hash.K2H_INVALID_HANDLE)
        db.close()

    def test_K2hash_get_tx_pool_size(self):
        self.assertTrue(k2hash.K2hash.get_tx_pool_size() == 0)

    def test_K2hash_libk2hash(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        self.assertTrue(isinstance(db.libk2hash, ctypes.CDLL))
        db.close()

    def test_K2hash_libc(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        self.assertTrue(isinstance(db.libc, ctypes.CDLL))
        db.close()

    def test_K2hash_load_from_file(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        k2h_file = "test.k2h"
        self.assertTrue(db.dump_to_file(k2h_file), val)
        db.close()

        db = None
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        self.assertTrue(db.load_from_file(k2h_file), val)
        self.assertTrue(db.get(key), val)
        db.close()

    def test_K2hash_print_attribute_plugins(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        self.assertTrue(db.print_attribute_plugins(), True)
        db.close()

    def test_K2hash_print_attributes(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        self.assertTrue(db.print_attributes(), True)
        db.close()

    def test_K2hash_print_data_stats(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        self.assertTrue(db.print_data_stats(), True)
        db.close()

    def test_K2hash_print_table_stats(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        self.assertTrue(db.print_table_stats(), True)
        db.close()

    def test_K2hash_remove(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        self.assertTrue(db.remove(key), True)
        self.assertTrue(db.get(key) == "")
        db.close()

    def test_K2hash_remove_subkeys(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        subkey = "subkey"
        subval = "subval"
        self.assertTrue(db.set(subkey, subval), True)
        self.assertTrue(db.get(subkey), subval)
        self.assertTrue(db.add_subkey(key, subkey, subval), True)
        self.assertTrue(db.get_subkeys(key) == [subkey])
        self.assertTrue(db.remove_subkeys(key, [subkey]), True)
        self.assertTrue(db.get_subkeys(key) == [])
        self.assertTrue(db.get(subkey) == "")
        db.close()

    def test_K2hash_rename(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        newkey = key[::-1]
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        self.assertTrue(db.rename(key, newkey), val)
        self.assertTrue(db.get(newkey), val)
        db.close()

    def test_K2hash_set_attribute(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        attr_key = "attrkey1"
        attr_val = "attrval1"
        attrs = {attr_key: attr_val}
        self.assertTrue(db.set_attribute(key, attr_key, attr_val), True)
        self.assertTrue(db.get_attributes(key), attrs)
        db.close()

    def test_K2hash_set_encryption_password_file(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        # 1. make the password file for test
        password_file = "password.txt"
        password = "secretstring"
        import os

        with open(password_file, "w") as f:
            print("{}".format(password), file=f)

        # 2. call the api
        self.assertTrue(db.set_encryption_password_file(password_file), True)
        db.close()

    def test_K2hash_set_expiration_duration(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        duration = 3
        self.assertTrue(db.set_expiration_duration(duration), True)
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        time.sleep(duration + 1)
        self.assertTrue(db.get(key) == "")
        db.close()

    def test_K2hash_set_log_level(self):
        db = k2hash.K2hash()
        db.set_log_level(k2hash.LogLevel.ERROR)
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        db.set_log_level(k2hash.LogLevel.WARNING)
        db.close()

    def test_K2hash_set_subkeys(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        subkey = "subkey"
        subval = "subval"
        self.assertTrue(db.set_subkeys(key, {subkey: subval}), True)
        self.assertTrue(db.get_subkeys(key) == [subkey])
        db.close()

    def test_K2hash_set_tx_pool_size(self):
        self.assertTrue(k2hash.K2hash.set_tx_pool_size(1), True)
        self.assertTrue(k2hash.K2hash.get_tx_pool_size() == 1)

    def test_K2hash_stop_tx(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        k2h_tx_log = "test.log"
        self.assertTrue(db.begin_tx(k2h_tx_log), True)
        # TODO how to check whether transaction is enabled.
        self.assertTrue(db.get_tx_file_fd() != k2hash.K2hash.K2H_INVALID_HANDLE)
        self.assertTrue(db.stop_tx(), True)
        db.close()

    def test_K2hash_repr(self):
        db = k2hash.K2hash()
        self.assertTrue(isinstance(db, k2hash.K2hash))
        key = "hello"
        val = "world"
        self.assertTrue(db.set(key, val), True)
        self.assertTrue(db.get(key), val)
        self.assertRegex(repr(db), "<_K2hash _k2hfile=.*")
        db.close()

    def test_K2hash_version(self):
        self.assertTrue(k2hash.K2hash.version() == None)


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
