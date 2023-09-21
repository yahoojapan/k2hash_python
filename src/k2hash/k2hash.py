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
"""K2hash Python Driver under MIT License"""
from __future__ import absolute_import

import ctypes
import logging
import os
import sys
from ctypes import byref, c_bool, c_char_p, c_int, c_size_t, c_uint64, pointer
from pathlib import Path

import k2hash
from k2hash import DumpLevel, LogLevel, OpenFlag, TimeUnit

LOG = logging.getLogger(__name__)


class K2hashIterator:
    """implements iterator of k2hash"""

    def __init__(self, k2h, key=None):
        """Provides constructor"""
        if not isinstance(k2h, K2hash):
            raise TypeError("k2h should be a K2hash object")
        self._k2h_handle = k2h.handle
        self._libc = k2h.libc
        self._libk2hash = k2h.libk2hash
        self._key = None
        if key and isinstance(key, str):
            self._key = key
        if key and not isinstance(key, str):
            raise TypeError("key should be a str object")

        if self._key:
            handle = self._libk2hash.k2h_find_first_str_subkey(
                self._k2h_handle, (c_char_p(self._key.encode()))
            )
        else:
            handle = self._libk2hash.k2h_find_first(self._k2h_handle)

        if handle == K2hash.K2H_INVALID_HANDLE:
            raise RuntimeError("handle should not be K2H_INVALID_HANDLE")
        self._handle = handle

    def __iter__(self):
        """Implements iter() itrator interface"""
        return self

    def __next__(self):
        """Implements next() itrator interface"""
        ppkey = pointer(c_char_p("".encode()))
        pkeylength = pointer(c_size_t(0))
        res = self._libk2hash.k2h_find_get_key(self._handle, ppkey, pkeylength)
        if res:
            keylength = pkeylength.contents.value
            if keylength > 0:
                pkey = ppkey.contents.value.decode()
                self._libc.free(ppkey.contents)
                self._handle = self._libk2hash.k2h_find_next(self._handle)
                return pkey
        raise StopIteration


class K2hash:  # noqa: pylint: disable=too-many-instance-attributes,too-many-public-methods
    """
    K2hash class provides methods to handle key/value pairs in k2hash hash database.
    """

    K2H_INVALID_HANDLE = 0

    def get_iterator(self, key=None):
        """Returns the k2hash iterator"""
        if not self._iterator:
            self._iterator = K2hashIterator(self, key)
        return self._iterator

    def _set_k2h_handle(self):
        """Sets the k2h handle"""
        if self._k2hfile == "":
            if self._flag is None:
                # case1. no k2hfile + no flag ---> k2hash_open_memory
                handle = self._libk2hash.k2h_open_mem(
                    self._maskbit, self._cmaskbit, self._maxelementcnt, self._pagesize
                )
            elif self._flag == OpenFlag.MEMORY:
                # case2. no k2hfile + flag. k2hash_open_memory if flag is memory, otherwise error.
                handle = self._libk2hash.k2h_open_mem(
                    self._maskbit, self._cmaskbit, self._maxelementcnt, self._pagesize
                )
            else:
                raise ValueError("flag should be memory if k2hfile is empty")
        else:
            if self._flag is None:
                # case3. k2hfile + no flag ---> k2h_open
                handle = self._libk2hash.k2h_open(
                    c_char_p(self._k2hfile.encode()),
                    self._readonly,
                    self._removefile,
                    self._fullmap,
                    self._maskbit,
                    self._cmaskbit,
                    self._maxelementcnt,
                    self._pagesize,
                )
            elif self._flag == OpenFlag.EDIT:
                # case4. k2hfile + flag ---> k2hash_open_{rw|r|tempfile} if flag is not memory,
                # Otherwise error
                handle = self._libk2hash.k2h_open_rw(
                    c_char_p(self._k2hfile.encode()),
                    self._fullmap,
                    self._maskbit,
                    self._cmaskbit,
                    self._maxelementcnt,
                    self._pagesize,
                )
            elif self._flag == OpenFlag.READ:
                # Checks if self._k2hfile exists.
                if os.path.exists(Path(self._k2hfile)) is True:
                    handle = self._libk2hash.k2h_open_ro(
                        c_char_p(self._k2hfile.encode()),
                        self._fullmap,
                        self._maskbit,
                        self._cmaskbit,
                        self._maxelementcnt,
                        self._pagesize,
                    )
                else:
                    raise RuntimeError(f"{self._k2hfile} should exist")
            elif self._flag == OpenFlag.TEMPFILE:
                handle = self._libk2hash.k2h_open_tempfile(
                    c_char_p(self._k2hfile.encode()),
                    self._fullmap,
                    self._maskbit,
                    self._cmaskbit,
                    self._maxelementcnt,
                    self._pagesize,
                )
            else:
                raise ValueError("k2hfile should be empty if using flag is memory")

        if handle == self.__class__.K2H_INVALID_HANDLE:
            raise RuntimeError("handle should not be K2H_INVALID_HANDLE")
        self._handle = handle

    def __init__(  # noqa: pylint: disable=too-many-branches,too-many-arguments
        self,
        k2hfile="",
        flag=None,
        readonly=True,
        removefile=True,
        fullmap=True,
        maskbit=8,
        cmaskbit=4,
        maxelementcnt=1024,
        pagesize=512,
        waitms=0,
        logfile="",
    ):
        """
        Initialize a new K2hash instnace.
        """
        self._handle = 0
        self._iterator = None
        self._flag = None

        if not isinstance(k2hfile, str):
            raise TypeError("k2hfile should currently be a str object")
        self._k2hfile = k2hfile
        if flag and not isinstance(flag, OpenFlag):
            raise TypeError("flag should be a OpenFlag object")
        self._flag = flag
        if self._k2hfile == "":
            self._flag = OpenFlag.MEMORY  # overrides flag
        if not isinstance(readonly, bool):
            raise TypeError("readonly should be an bool object")
        self._readonly = readonly
        if not isinstance(removefile, bool):
            raise TypeError("removefile should be an bool object")
        self._removefile = removefile
        if not isinstance(fullmap, bool):
            raise TypeError("fullmap should be an bool object")
        self._fullmap = fullmap
        if not isinstance(maskbit, int):
            raise TypeError("maskbit should be an bool object")
        self._maskbit = maskbit
        if not isinstance(cmaskbit, int):
            raise TypeError("cmaskbit should be an bool object")
        self._cmaskbit = cmaskbit
        if not isinstance(maxelementcnt, int):
            raise TypeError("maxelementcnt should be an bool object")
        self._maxelementcnt = maxelementcnt
        if not isinstance(pagesize, int):
            raise TypeError("pagesize should be an bool object")
        self._pagesize = pagesize
        if not isinstance(waitms, int):
            raise TypeError("waitms should be an bool object")
        self._waitms = waitms
        if not isinstance(logfile, str):
            raise TypeError("logfile should currently be a str object")
        self._logfile = logfile

        try:
            # https://docs.python.org/3/library/ctypes.html#ctypes.LibraryLoader.LoadLibrary
            self._libc = k2hash.get_library_handle()["c"]
            self._libk2hash = k2hash.get_library_handle()["k2hash"]
            if not self._libk2hash:
                raise RuntimeError("unable to load k2hash library")
        except:
            LOG.error("Unexpected error:{%s}", sys.exc_info()[0])
            raise

        self._set_k2h_handle()

    @property
    def libk2hash(self):
        """returns libk2hash handle"""
        return self._libk2hash

    @property
    def libc(self):
        """returns libc handle"""
        return self._libc

    def set(  # noqa: pylint: disable=too-many-arguments
        self, key, val, password=None, expire_duration=None, time_unit=TimeUnit.SECONDS
    ):
        """Sets a key/value pair"""
        if not isinstance(key, str):
            raise TypeError("key should currently be a str object")
        if not key:
            raise ValueError("key should not be empty")
        if not isinstance(val, str):
            raise TypeError("val should currently be a str object")
        if password and not isinstance(password, str):
            raise TypeError("password should be a str object")
        if password and password == "":
            raise ValueError("password should not be empty")
        if expire_duration and not isinstance(expire_duration, int):
            raise TypeError("expire_duration should be a int object")
        if expire_duration and expire_duration <= 0:
            raise ValueError("expire_duration should not be positive")
        if time_unit and not isinstance(time_unit, TimeUnit):
            raise TypeError("time_unit should be a TimeUnit object")

        res = self._libk2hash.k2h_set_str_value_wa(
            self._handle,
            c_char_p(key.encode()),
            c_char_p(val.encode()),
            (c_char_p(password.encode()) if password else None),
            (pointer(c_uint64(expire_duration)) if expire_duration else None),
        )

        LOG.debug("ret:%s", res)
        return res

    def get(self, key, password=None):
        """Gets the value"""
        if not isinstance(key, str):
            raise TypeError("key should currently be a str object")
        if not key:
            raise ValueError("key should not be empty")
        if password and not isinstance(password, str):
            raise TypeError("password should be a str object")
        if password and password == "":
            raise ValueError("password should not be empty")

        val = self._libk2hash.k2h_get_str_direct_value_wp(
            self._handle,
            c_char_p(key.encode()),
            (c_char_p(password.encode()) if password else None),
        )

        if val:
            return val.decode()
        return ""

    def add_attribute_plugin_lib(self, path):
        """Adds a shared library that handles an attribute"""
        if not isinstance(path, str):
            raise TypeError("path should currently be a str object")
        if not os.path.exists(Path(path)):
            raise RuntimeError(f"path {path} should exists")

        res = self._libk2hash.k2h_add_attr_plugin_library(
            self._handle, c_char_p(path.encode())
        )

        if not res:
            LOG.error("error in k2h_add_attr_plugin_library")
        return res

    def add_decryption_password(self, password):
        """Adds a passphrase to decrypt a value."""
        if not isinstance(password, str):
            raise TypeError("key should currently be a str object")
        if password and password == "":
            raise ValueError("password should not be empty")

        res = self._libk2hash.k2h_add_attr_crypt_pass(
            self._handle, c_char_p(password.encode()), False
        )

        if not res:
            LOG.error("error in k2h_add_attr_crypt_pass")
        return res

    def add_subkey(  # noqa: pylint: disable=too-many-arguments
        self,
        key,
        subkey,
        subval,
        password=None,
        expire_duration=None,
        time_unit=TimeUnit.SECONDS,
    ):
        """Adds subkeys to a key/value pair."""
        if not isinstance(key, str):
            raise TypeError("key should currently be a str object")
        if not key:
            raise ValueError("key should not be empty")
        if not isinstance(subkey, str):
            raise TypeError("subkey should be a str object")
        if not subkey:
            raise ValueError("subkey should not be empty")
        if not isinstance(subval, str):
            raise TypeError("subval should be a str object")
        if password and not isinstance(password, str):
            raise TypeError("password should currently be a str object")
        if password and password == "":
            raise ValueError("password should not be empty")
        if expire_duration and not isinstance(expire_duration, int):
            raise TypeError("expire_duration should be a int object")
        if expire_duration and expire_duration <= 0:
            raise ValueError("expire_duration should not be positive")
        if time_unit and not isinstance(time_unit, TimeUnit):
            raise TypeError("time_unit should be a TimeUnit object")

        res = self._libk2hash.k2h_add_subkey_wa(
            self._handle,
            c_char_p(key.encode()),
            c_size_t(len(key) + 1),
            c_char_p(subkey.encode()),
            c_size_t(len(subkey) + 1),
            c_char_p(subval.encode()),
            c_size_t(len(subval) + 1),
            (c_char_p(password.encode()) if password else None),
            (pointer(c_uint64(expire_duration)) if expire_duration else None),
        )

        if not res:
            LOG.error("error in k2h_add_str_subkey_wa")
        return res

    def begin_tx(self, txfile, prefix=None, param=None, expire_duration=None):
        """Starts a transaction logging."""
        if not isinstance(txfile, str):
            raise TypeError("txfile should be a str object")
        if txfile == "":
            raise ValueError("txfile should not be empty")
        if prefix and not isinstance(prefix, str):
            raise TypeError("prefix should be a str object")
        if prefix and prefix == "":
            raise ValueError("prefix should not be empty")
        if param and not isinstance(param, str):
            raise TypeError("param should be a str object")
        if param and param == "":
            raise ValueError("param should not be empty")
        if expire_duration and not isinstance(expire_duration, int):
            raise TypeError("expire_duration should be a int object")
        if expire_duration and expire_duration <= 0:
            raise ValueError("expire_duration should not be positive")

        res = self._libk2hash.k2h_transaction_param_we(
            self._handle,
            True,
            c_char_p(txfile.encode()),
            (c_char_p(prefix.encode()) if prefix else None),
            (c_size_t(len(prefix)) if prefix else 0),
            (c_char_p(param.encode()) if param else None),
            (c_size_t(len(param)) if param else 0),
            (pointer(c_uint64(expire_duration)) if expire_duration else None),
        )

        if not res:
            LOG.error("error in k2h_transaction_param_we")
        return res

    def close(self):
        """Closes a k2h file."""
        res = self._libk2hash.k2h_close_wait(self._handle, self._waitms)
        if not res:
            LOG.error("error in k2h_close_wait")
        return res

    @staticmethod
    def create(  # noqa: pylint: disable=too-many-branches
        pathname, maskbit=8, cmaskbit=4, maxelementcnt=1024, pagesize=512
    ):
        """Creates a k2hash file."""
        if not isinstance(pathname, str):
            raise TypeError("pathname should be a str object")
        if not pathname:
            raise ValueError("pathname should not be empty")
        if not isinstance(maskbit, int):
            raise TypeError("maskbit should be an bool object")
        if maskbit <= 0:
            raise ValueError("maskbit should not be positive")
        if not isinstance(cmaskbit, int):
            raise TypeError("cmaskbit should be an bool object")
        if cmaskbit <= 0:
            raise ValueError("cmaskbit should not be positive")
        if not isinstance(maxelementcnt, int):
            raise TypeError("maxelementcnt should be an bool object")
        if maxelementcnt <= 0:
            raise ValueError("maxelementcnt should not be positive")
        if not isinstance(pagesize, int):
            raise TypeError("pagesize should be an bool object")
        if pagesize <= 0:
            raise ValueError("pagesize should not be positive")

        try:
            # https://docs.python.org/3/library/ctypes.html#ctypes.LibraryLoader.LoadLibrary
            libk2hash = k2hash.get_library_handle()["k2hash"]
            if not libk2hash:
                raise RuntimeError("unable to load k2hash library")
        except:
            LOG.error("Unexpected error:{%s}", sys.exc_info()[0])
            raise

        res = libk2hash.k2h_create(
            c_char_p(pathname.encode()),
            c_int(maskbit),
            c_int(cmaskbit),
            c_int(maxelementcnt),
            c_size_t(pagesize),
        )
        if not res:
            LOG.error("error in k2h_create")
        return res

    def dump_to_file(self, path, is_skip_error=True):
        """Dumps data to a file."""
        if not isinstance(path, str):
            raise TypeError("path should be a str object")
        if not path:
            raise ValueError("path should not be empty")
        if not isinstance(is_skip_error, bool):
            raise TypeError("is_skip_error should be a boolean object")

        res = self._libk2hash.k2h_put_archive(
            self._handle, c_char_p(path.encode()), is_skip_error
        )
        if not res:
            LOG.error("error in k2h_create")
        return res

    def enable_encryption(self, enable=True):
        """Enables a feature to encrypt a value."""
        if not isinstance(enable, bool):
            raise TypeError("enable should be a bool object")

        res = self._libk2hash.k2h_set_common_attr(
            self._handle, None, c_bool(enable), None, None, None
        )
        if not res:
            LOG.error("error in k2h_set_common_attr")
        return res

    def enable_history(self, enable=True):
        """Enables a feature to record a key modification history."""
        if not isinstance(enable, bool):
            raise TypeError("enable should be a bool object")

        res = self._libk2hash.k2h_set_common_attr(
            self._handle, None, None, None, c_bool(enable), None
        )
        if not res:
            LOG.error("error in k2h_set_common_attr")
        return res

    def enable_mtime(self, enable=True):
        """Enables a feature to record value modification time."""
        if not isinstance(enable, bool):
            raise TypeError("enable should be a bool object")

        res = self._libk2hash.k2h_set_common_attr(
            self._handle, c_bool(enable), None, None, None, None
        )
        if not res:
            LOG.error("error in k2h_set_common_attr")
        return res

    def get_attributes(self, key, use_str=True):
        """Gets attributes of a key."""
        if not isinstance(key, str):
            raise TypeError("key should currently be a str object")
        if not key:
            raise ValueError("key should not be empty")
        pattrspckcnt = c_int()
        res = self._libk2hash.k2h_get_direct_attrs(
            self._handle,
            c_char_p(key.encode()),
            c_size_t(len(key)),
            byref(pattrspckcnt),
        )
        LOG.debug(
            "type(res):{%s} pattrspckcnt.value{%s}", type(res), pattrspckcnt.value
        )
        attrs = {}
        for i in range(pattrspckcnt.value):
            key_buf = ctypes.create_string_buffer(res[i].keylength)
            val_buf = ctypes.create_string_buffer(res[i].vallength)
            for j in range(res[i].keylength):
                key_buf[j] = res[i].pkey[j]
            for k in range(res[i].vallength):
                val_buf[k] = res[i].pval[k]
            if use_str:
                attrs[key_buf.value.decode()] = val_buf.value.decode()
            else:
                attrs[key_buf.value] = val_buf.value
        return attrs

    @property
    def handle(self):
        """Returns a Queue handle."""
        return self._handle

    def get_subkeys(self, key, use_str=True):
        """Gets keys of subkeys of a key."""
        if not isinstance(key, str):
            raise TypeError("key should currently be a str object")
        if not key:
            raise ValueError("key should not be empty")

        pskeypckcnt = c_int()
        res = self._libk2hash.k2h_get_direct_subkeys(
            self._handle,
            c_char_p(key.encode()),
            c_size_t(len(key) + 1),
            byref(pskeypckcnt),
        )
        LOG.debug("%s", pskeypckcnt.value)
        subkeys = []
        for i in range(pskeypckcnt.value):
            buf = ctypes.create_string_buffer(res[i].length)
            for j in range(res[i].length):
                buf[j] = res[i].pkey[j]
            if use_str:
                subkeys.append(buf.value.decode())
            else:
                subkeys.append(buf.value)
        return subkeys

    def get_tx_file_fd(self):
        """Gets a transaction log file descriptor."""
        res = self._libk2hash.k2h_get_transaction_archive_fd(self._handle)
        return res

    @staticmethod
    def get_tx_pool_size():
        """Gets the number of transaction thread pool."""
        try:
            # https://docs.python.org/3/library/ctypes.html#ctypes.LibraryLoader.LoadLibrary
            libk2hash = k2hash.get_library_handle()["k2hash"]
            if not libk2hash:
                raise RuntimeError("unable to load k2hash library")
        except:
            LOG.error("Unexpected error:{%s}", sys.exc_info()[0])

            raise

        res = libk2hash.k2h_get_transaction_thread_pool()
        return res

    def load_from_file(self, path, is_skip_error=True):
        """Loads data from a file."""
        if not isinstance(path, str):
            raise TypeError("path should currently be a str object")
        if not os.path.exists(Path(path)):
            raise RuntimeError(f"path{path} should exists")
        if not isinstance(is_skip_error, bool):
            raise TypeError("is_skip_error should be a boolean object")

        res = self._libk2hash.k2h_load_archive(
            self._handle, c_char_p(path.encode()), c_bool(is_skip_error)
        )
        if not res:
            LOG.error("error in k2h_load_archive")
        return res

    def print_attribute_plugins(self):
        """Prints attribute plugins to stderr."""
        res = self._libk2hash.k2h_print_attr_version(self._handle, None)
        return res

    def print_attributes(self):
        """Prints attributes to stderr."""
        res = self._libk2hash.k2h_print_attr_information(self._handle, None)
        return res

    def print_data_stats(self):
        """Prints data statistics."""
        res = self._libk2hash.k2h_print_state(self._handle, None)
        return res

    def print_table_stats(self, level=DumpLevel.HEADER):
        """Prints k2hash key table information."""
        if not isinstance(level, DumpLevel):
            raise TypeError("level should be a DumpLevel object")

        if level == DumpLevel.HEADER:
            res = self._libk2hash.k2h_dump_head(self._handle, None)
        elif level == DumpLevel.HASH_TABLE:
            res = self._libk2hash.k2h_dump_keytable(self._handle, None)
        elif level == DumpLevel.SUB_HASH_TABLE:
            res = self._libk2hash.k2h_dump_full_keytable(self._handle, None)
        elif level == DumpLevel.ELEMENT:
            res = self._libk2hash.k2h_dump_elementtable(self._handle, None)
        elif level == DumpLevel.PAGE:
            res = self._libk2hash.k2h_dump_full(self._handle, None)
        else:
            raise ValueError(
                "level should be either HEADER, HASH_TABLE, SUB_HASH_TABLE, ELEMENT or PAGE"
            )
        return res

    def remove(self, key, remove_all_subkeys=False):
        """Removes a key."""
        if not isinstance(key, str):
            raise TypeError("key should be a str object")
        if not key:
            raise ValueError("key should not be empty")
        if not isinstance(remove_all_subkeys, bool):
            raise TypeError("remove_all_subkeys should be a boolean object")
        if remove_all_subkeys:
            res = self._libk2hash.k2h_remove_str_all(
                self._handle, c_char_p(key.encode())
            )
        else:
            res = self._libk2hash.k2h_remove_str(self._handle, c_char_p(key.encode()))
        return res

    def remove_subkeys(self, key, subkeys):
        """Removes subkeys from the key."""
        if not isinstance(key, str):
            raise TypeError("key should be a str object")
        if not key:
            raise ValueError("key should not be empty")
        if not isinstance(subkeys, list):
            raise TypeError("subkeys should be a list object")
        if not subkeys:
            raise ValueError("subkey should not be empty")
        for subkey in subkeys:
            if not isinstance(subkey, str):
                LOG.warning("subkey should be a str object")
                continue
            if not subkey:
                LOG.warning("subkey should not be empty")
                continue
            res = self._libk2hash.k2h_remove_str_subkey(
                self._handle, c_char_p(key.encode()), c_char_p(subkey.encode())
            )
            if not res:
                return False
        return True

    def rename(self, key, newkey):
        """Renames a key with a new key."""
        if not isinstance(key, str):
            raise TypeError("key should be a str object")
        if not key:
            raise ValueError("key should not be empty")
        if not isinstance(newkey, str):
            raise TypeError("newkey should be a str object")
        if not newkey:
            raise ValueError("newkey should not be empty")
        res = self._libk2hash.k2h_rename_str(
            self._handle, c_char_p(key.encode()), c_char_p(newkey.encode())
        )
        return res

    def set_attribute(self, key, attr_name, attr_val):
        """Sets an attribute of a key."""
        if not isinstance(key, str):
            raise TypeError("key should be a str object")
        if not key:
            raise ValueError("key should not be empty")
        if not isinstance(attr_name, str):
            raise TypeError("attr_name should be a str object")
        if not attr_name:
            raise ValueError("attr_name should not be empty")
        if not isinstance(attr_val, str):
            raise TypeError("attr_val should be a str object")
        if not attr_val:
            raise ValueError("attr_val should not be empty")
        res = self._libk2hash.k2h_add_attr(
            self._handle,
            c_char_p(key.encode()),
            c_size_t(len(key)),
            c_char_p(attr_name.encode()),
            c_size_t(len(attr_name) + 1),
            c_char_p(attr_val.encode()),
            c_size_t(len(attr_val) + 1),
        )
        return res

    def set_default_encryption_password(self, password):
        """Sets the default encryption passphrase."""
        if not isinstance(password, str):
            raise TypeError("password should be a str object")
        if not password:
            raise ValueError("password should not be empty")
        res = self._libk2hash.k2h_add_attr_crypt_pass(
            self._handle, c_char_p(password.encode()), c_bool(True)
        )
        return res

    def set_encryption_password_file(self, path):
        """Sets the data encryption password file."""
        if not isinstance(path, str):
            raise TypeError("path should currently be a str object")
        if not os.path.exists(Path(path)):
            raise RuntimeError(f"path{path} should exists")
        res = self._libk2hash.k2h_set_common_attr(
            self._handle, None, None, c_char_p(path.encode()), None, None
        )
        if not res:
            LOG.error("error in k2h_set_common_attr")
        return res

    def set_expiration_duration(self, expire_duration, time_unit=TimeUnit.SECONDS):
        """Sets the duration to expire a value."""
        if not isinstance(expire_duration, int):
            raise TypeError("expire_duration should be a int object")
        if expire_duration <= 0:
            raise ValueError("expire_duration should not be positive")
        if not isinstance(time_unit, TimeUnit):
            raise TypeError("time_unit should be a TimeUnit object")
        res = self._libk2hash.k2h_set_common_attr(
            self._handle, None, None, None, None, pointer(c_uint64(expire_duration))
        )
        if not res:
            LOG.error("error in k2h_set_common_attr")
        return res

    def set_log_level(self, level=LogLevel.INFO):
        """Creates a k2hash file."""
        if not isinstance(level, LogLevel):
            raise TypeError("level should be a LogLevel object")
        if not level:
            raise ValueError("level should not be empty")

        if level == LogLevel.SILENT:
            self._libk2hash.k2h_set_debug_level_silent()
        elif level == LogLevel.ERROR:
            self._libk2hash.k2h_set_debug_level_error()
        elif level == LogLevel.WARNING:
            self._libk2hash.k2h_set_debug_level_warning()
        elif level == LogLevel.INFO:
            self._libk2hash.k2h_set_debug_level_message()
        elif level == LogLevel.DEBUG:
            self._libk2hash.k2h_set_debug_level_dump()
        else:
            raise ValueError(
                "level should be either SILENT, ERROR, WARN, INFO or DEBUG"
            )

    def set_subkeys(  # noqa: pylint: disable=too-many-branches,too-many-arguments
        self,
        key,
        subkeys,
        password=None,
        expire_duration=None,
        time_unit=TimeUnit.SECONDS,
    ):
        """Sets subkeys."""
        if not isinstance(key, str):
            raise TypeError("key should be a str object")
        if not key:
            raise ValueError("key should not be empty")
        if not isinstance(subkeys, dict):
            raise TypeError("subkeys should be a dict object")
        if not subkeys:
            raise ValueError("subkey should not be empty")
        if password and not isinstance(password, str):
            raise TypeError("password should currently be a str object")
        if password and password == "":
            raise ValueError("password should not be empty")
        if expire_duration and not isinstance(expire_duration, int):
            raise TypeError("expire_duration should be a int object")
        if expire_duration and expire_duration <= 0:
            raise ValueError("expire_duration should not be positive")
        if time_unit and not isinstance(time_unit, TimeUnit):
            raise TypeError("time_unit should be a TimeUnit object")
        for subkey, subval in subkeys.items():
            if not isinstance(subkey, str):
                LOG.warning("subkey should be a str object")
                continue
            if not subkey:
                LOG.warning("subkey should not be empty")
                continue
            if not isinstance(subval, str):
                LOG.warning("subval should be a str object")
                continue
            if not subval:
                LOG.warning("subval should not be empty")
                continue
            res = self._libk2hash.k2h_add_subkey_wa(
                self._handle,
                c_char_p(key.encode()),
                c_size_t(len(key) + 1),
                c_char_p(subkey.encode()),
                c_size_t(len(subkey) + 1),
                c_char_p(subval.encode()),
                c_size_t(len(subval) + 1),
                (c_char_p(password.encode()) if password else None),
                (pointer(c_uint64(expire_duration)) if expire_duration else None),
            )
            if res:
                LOG.debug("k2h_add_str_subkeys:{%s}", res)
            else:
                return False
        return True

    @staticmethod
    def set_tx_pool_size(size):
        """Sets the number of transaction thread pool."""
        if not isinstance(size, int):
            raise TypeError("size should be a int object")
        if size < 0:
            raise ValueError("size should be positive")
        try:
            # https://docs.python.org/3/library/ctypes.html#ctypes.LibraryLoader.LoadLibrary
            libk2hash = k2hash.get_library_handle()["k2hash"]
            if not libk2hash:
                raise RuntimeError("unable to load k2hash library")
        except:
            LOG.error("Unexpected error:{%s}", sys.exc_info()[0])
            raise

        res = libk2hash.k2h_set_transaction_thread_pool(size)
        if not res:
            LOG.error("error in k2h_set_transaction_thread_pool")
        return res

    def stop_tx(self):
        """Stops a transaction logging."""
        res = self._libk2hash.k2h_disable_transaction(self._handle)
        if not res:
            LOG.error("error in k2h_disable_transaction")
        return res

    def __repr__(self):
        """Returns full of members as a string."""
        attrs = []
        for attr in [
            "_k2hfile",
            "_flag",
            "_readonly",
            "_removefile",
            "_fullmap",
            "_maskbit",
            "_cmaskbit",
            "_maxelementcnt",
            "_pagesize",
            "_waitms",
            "_logfile",
            "_libc",
            "_libk2hash",
            "_handle",
        ]:  # should be hardcoded.
            val = getattr(self, attr)
            if val:
                attrs.append((attr, repr(val)))
            else:
                attrs.append((attr, ""))
            values = ", ".join(["%s=%s" % i for i in attrs])  # noqa: pylint: disable=consider-using-f-string
        return "<_K2hash " + values + ">"

    @staticmethod
    def version():
        """Prints version information."""
        try:
            # https://docs.python.org/3/library/ctypes.html#ctypes.LibraryLoader.LoadLibrary
            libk2hash = k2hash.get_library_handle()["k2hash"]
            if not libk2hash:
                raise RuntimeError("unable to load k2hash library")
        except:
            LOG.error("Unexpected error:{%s}", sys.exc_info()[0])
            raise

        libk2hash.k2h_print_version(None)


#
# Local variables:
# tab-width: 4
# c-basic-offset: 4
# End:
# vim600: expandtab sw=4 ts=4 fdm=marker
# vim<600: expandtab sw=4 ts=4
#
