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
"""
k2hash package
"""
from __future__ import absolute_import

__all__ = [
    "K2hash",
    "Queue",
    "BaseQueue",
    "KeyQueue",
    "K2hashIterator",
    "LogLevel",
    "KeyPack",
    "AttrPack",
    "OpenFlag",
    "DumpLevel",
    "LogLevel",
    "TimeUnit",
]

import ctypes
import logging
import sys
from ctypes import (
    POINTER,
    Structure,
    c_bool,
    c_char_p,
    c_int,
    c_long,
    c_size_t,
    c_ubyte,
    c_uint64,
    c_ulong,
)
from ctypes.util import find_library
from enum import Enum
from logging import StreamHandler
from logging.handlers import TimedRotatingFileHandler
from typing import List  # noqa: pylint: disable=unused-import
from typing import Dict, Optional, Set, Tuple, Union

LOG = logging.getLogger(__name__)


# https://docs.python.org/3/library/ctypes.html#incomplete-types
class FILE(Structure):  # noqa: pylint:disable=too-few-public-methods
    """C FILE structure"""


# https://docs.python.org/3/library/ctypes.html#incomplete-types
class time_t(Structure):  # noqa: pylint:disable=too-few-public-methods,invalid-name
    """C time_t structure"""


# KeyPack structure
# See: https://github.com/yahoojapan/k2hash/blob/master/lib/k2hash.h#L75
#
# typedef struct k2h_key_pack{
# 	unsigned char*	pkey;
# 	size_t			length;
# }K2HKEYPCK, *PK2HKEYPCK;
#
class KeyPack(Structure):  # noqa: pylint:disable=too-few-public-methods
    """C KeyPack structure"""

    _fields_ = [("pkey", POINTER(c_ubyte)), ("length", c_size_t)]


# AttrPack structure
# See: https://github.com/yahoojapan/k2hash/blob/master/lib/k2hash.h#L81
#
# typedef struct k2h_attr_pack{
# 	unsigned char*	pkey;
# 	size_t			keylength;
# 	unsigned char*	pval;
# 	size_t			vallength;
# }K2HATTRPCK, *PK2HATTRPCK;
#
class AttrPack(Structure):  # noqa: pylint:disable=too-few-public-methods
    """C Attr structure"""

    _fields_ = [
        ("pkey", POINTER(c_ubyte)),
        ("keylength", c_size_t),
        ("pval", POINTER(c_ubyte)),
        ("vallength", c_size_t),
    ]


class OpenFlag(Enum):
    """k2hash file open flags"""

    READ = 1
    EDIT = 2
    TEMPFILE = 3
    MEMORY = 4


class TimeUnit(Enum):
    """k2hash time units"""

    DAYS = 1
    HOURS = 2
    MILLISECONDS = 3
    MINUTES = 4
    SECONDS = 5


class DumpLevel(Enum):
    """k2hash file status information"""

    # Dump headers
    HEADER = 1
    # Dump headers and hash tables
    HASH_TABLE = 2
    # Dump headers, hash tables and sub hash tables
    SUB_HASH_TABLE = 3
    # Dump headers, hash tables, sub hash tables and elements
    ELEMENT = 4
    # Dump headers, hash tables, sub hash tables, elements and pages
    PAGE = 5


class LogLevel(Enum):
    """k2hash log level"""

    # Silent disables logging.
    SILENT = 1
    # logs on errors
    ERROR = 2
    # logs on (errors || warnings)
    WARNING = 3
    # logs on (errors || warnings || info)
    INFO = 4
    # logs on (errors || warnings || info || debug)
    DEBUG = 5


# Library handles
_HANDLE: Dict[str, str] = {}


# Initializes library handles and stores the result in the _HANDLE cache
def _init_library_handle():
    global _HANDLE  # noqa: pylint: disable=global-statement
    if _HANDLE:
        return _HANDLE

    # Loads libc and libk2hash and ...
    result = {}
    result["c"] = _load_libc()
    result["k2hash"] = _load_libk2hash()
    _HANDLE = result

    return result


def _load_libc():
    ret = ctypes.cdll.LoadLibrary(find_library("c"))
    # print("type(ret).{}".format(type(ret)))
    if ret is None:
        raise FileNotFoundError
    return ret


def _load_libk2hash():  # noqa: pylint: disable=too-many-statements
    ret = ctypes.cdll.LoadLibrary(find_library("k2hash"))
    if ret is None:
        return None

    # Defines prototypes for python code
    #
    # 4. find API
    #
    # k2h_find_h k2h_find_first(k2h_h handle)
    ret.k2h_find_first.restype = c_uint64
    ret.k2h_find_first.argtypes = [c_uint64]
    # k2h_find_h k2h_find_first_str_subkey(k2h_h handle, const char* pkey)
    ret.k2h_find_first_str_subkey.restype = c_uint64
    ret.k2h_find_first_str_subkey.argtypes = [c_uint64, c_char_p]
    # k2h_find_h k2h_find_next(k2h_find_h findhandle)
    ret.k2h_find_next.restype = c_uint64
    ret.k2h_find_next.argtypes = [c_uint64]
    # bool k2h_find_get_key(k2h_find_h findhandle, unsigned char** ppkey, size_t* pkeylength)
    ret.k2h_find_get_key.restype = c_uint64
    ret.k2h_find_get_key.argtypes = [c_uint64, POINTER(c_char_p), POINTER(c_size_t)]
    #
    # 3. keyqueue API
    #
    # k2h_keyq_h k2h_keyq_handle_str_prefix(k2h_h handle, bool is_fifo, const char* pref)
    ret.k2h_keyq_handle_str_prefix.restype = c_uint64
    ret.k2h_keyq_handle_str_prefix.argtypes = [c_uint64, c_bool, c_char_p]
    # bool k2h_keyq_str_push_keyval(k2h_keyq_h keyqhandle, const char* pkey, const char* pval)
    ret.k2h_keyq_str_push_keyval.restype = c_bool
    ret.k2h_keyq_str_push_keyval.argtypes = [c_uint64, c_char_p, c_char_p]
    # bool k2h_keyq_str_push_keyval_wa(
    # k2h_keyq_h keyqhandle, const char* pkey, const char* pval, const char* encpass,
    # const time_t* expire)
    ret.k2h_keyq_str_push_keyval_wa.restype = c_bool
    ret.k2h_keyq_str_push_keyval_wa.argtypes = [
        c_uint64,
        c_char_p,
        c_char_p,
        c_char_p,
        POINTER(c_long),
    ]
    # bool k2h_keyq_dump(k2h_keyq_h qhandle, FILE* stream)
    ret.k2h_keyq_dump.restype = c_bool
    ret.k2h_keyq_dump.argtypes = [c_uint64, POINTER(FILE)]
    # bool k2h_keyq_free(k2h_keyq_h qhandle)
    ret.k2h_keyq_free.restype = c_bool
    ret.k2h_keyq_free.argtypes = [c_uint64]
    # int k2h_keyq_count(k2h_keyq_h qhandle)
    ret.k2h_keyq_count.restype = c_int
    ret.k2h_keyq_count.argtypes = [c_uint64]
    # bool k2h_keyq_str_read_keyval_wp(
    # k2h_keyq_h keyqhandle, char** ppkey, char** ppval, int pos, const char* encpass)
    ret.k2h_keyq_str_read_wp.restype = c_bool
    ret.k2h_keyq_str_read_keyval_wp.argtypes = [
        c_uint64,
        POINTER(c_char_p),
        POINTER(c_char_p),
        c_int,
        c_char_p,
    ]
    # bool k2h_keyq_empty(k2h_keyq_h qhandle)
    ret.k2h_keyq_empty.restype = c_bool
    ret.k2h_keyq_empty.argtypes = [c_uint64]
    # bool k2h_keyq_str_pop_keyval_wp(
    # k2h_keyq_h keyqhandle, char** ppkey, char** ppval, const char* encpass)
    ret.k2h_keyq_str_pop_keyval_wp.restype = c_bool
    ret.k2h_keyq_str_pop_keyval_wp.argtypes = [
        c_uint64,
        POINTER(c_char_p),
        POINTER(c_char_p),
        c_char_p,
    ]
    # bool k2h_keyq_remove(k2h_keyq_h qhandle, int count)
    ret.k2h_keyq_remove.restype = c_bool
    ret.k2h_keyq_remove.argtypes = [c_uint64, c_int]

    #
    # 2. queue API
    #
    # k2h_q_h k2h_q_handle_str_prefix(k2h_h handle, bool is_fifo, const char* pref)
    ret.k2h_q_handle_str_prefix.restype = c_uint64
    ret.k2h_q_handle_str_prefix.argtypes = [c_uint64, c_bool, c_char_p]
    # bool k2h_q_str_push_wa(
    #   k2h_q_h qhandle, const char* pdata, const PK2HATTRPCK pattrspck,
    #   int attrspckcnt, const char* encpass, const time_t* expire)
    ret.k2h_q_str_push_wa.restype = c_bool
    ret.k2h_q_str_push_wa.argtypes = [
        c_uint64,
        c_char_p,
        POINTER(AttrPack),
        c_int,
        c_char_p,
        POINTER(c_ulong),
    ]
    # bool k2h_q_str_push(k2h_q_h qhandle, const char* pval)
    ret.k2h_q_str_push.restype = c_bool
    ret.k2h_q_str_push.argtypes = [c_uint64, c_char_p]
    # bool k2h_q_remove(k2h_q_h qhandle, int count)
    ret.k2h_q_remove.restype = c_bool
    ret.k2h_q_remove.argtypes = [c_uint64, c_int]
    # bool k2h_q_free(k2h_q_h qhandle)
    ret.k2h_q_free.restype = c_bool
    ret.k2h_q_free.argtypes = [c_uint64]
    # int k2h_q_count(k2h_q_h qhandle)
    ret.k2h_q_count.restype = c_int
    ret.k2h_q_count.argtypes = [c_uint64]
    # bool k2h_q_str_read_wp(k2h_q_h qhandle, char** ppdata, int pos, const char* encpass)
    ret.k2h_q_read_wp.restype = c_bool
    ret.k2h_q_read_wp.argtypes = [
        c_uint64,
        POINTER(c_char_p),
        POINTER(c_size_t),
        c_int,
        c_char_p,
    ]
    # bool k2h_q_empty(k2h_q_h qhandle)
    ret.k2h_q_empty.restype = c_bool
    ret.k2h_q_empty.argtypes = [c_uint64]
    # bool k2h_q_str_pop(k2h_q_h qhandle, char** ppval)
    # bool k2h_q_str_pop_wp(k2h_q_h qhandle, char** ppdata, const char* encpass)
    ret.k2h_q_str_pop_wp.restype = c_bool
    ret.k2h_q_str_pop_wp.argtypes = [c_uint64, POINTER(c_char_p), c_char_p]
    # bool k2h_q_dump(k2h_q_h qhandle, FILE* stream)
    ret.k2h_q_dump.restype = c_bool
    ret.k2h_q_dump.argtypes = [c_uint64, POINTER(FILE)]

    #
    # 1. k2hash API
    #
    # add attr crypt API
    # bool k2h_add_attr_crypt_pass(k2h_h handle, const char* pass, bool is_default_encrypt)
    ret.k2h_add_attr_crypt_pass.argtypes = [c_uint64, c_char_p, c_bool]
    ret.k2h_add_attr_crypt_pass.restype = c_bool

    # add attr plugin API
    # bool k2h_add_attr_plugin_library(k2h_h handle, const char* libpath)
    ret.k2h_add_attr_plugin_library.argtypes = [c_uint64, c_char_p]
    ret.k2h_add_attr_plugin_library.restype = c_bool

    # add attr API
    # bool k2h_add_str_attr(
    # k2h_h handle, const char* pkey, const char* pattrkey, const char* pattrval)
    ret.k2h_add_str_attr.argtypes = [c_uint64, c_char_p, c_char_p, c_char_p]
    ret.k2h_add_str_attr.restype = c_bool
    # bool k2h_add_attr(
    # k2h_h handle, const unsigned char* pkey, size_t keylength, const unsigned char* pattrkey,
    # size_t attrkeylength, const unsigned char* pattrval, size_t attrvallength)
    ret.k2h_add_attr.argtypes = [
        c_uint64,
        c_char_p,
        c_size_t,
        c_char_p,
        c_size_t,
        c_char_p,
        c_size_t,
    ]
    ret.k2h_add_attr.restype = c_bool

    # add subkey API
    # bool k2h_add_subkey(
    # k2h_h handle, const unsigned char* pkey, size_t keylength, const unsigned char* psubkey,
    # size_t skeylength, const unsigned char* pval, size_t vallength)
    ret.k2h_add_subkey.argtypes = [
        c_uint64,
        c_char_p,
        c_size_t,
        c_char_p,
        c_size_t,
        c_char_p,
        c_size_t,
    ]
    ret.k2h_add_subkey.restype = c_bool
    # bool k2h_add_subkey_wa(k2h_h handle, const unsigned char* pkey, size_t keylength,
    # const unsigned char* psubkey, size_t skeylength, const unsigned char* pval,
    # size_t vallength, const char* pass, const time_t* expire)
    ret.k2h_add_subkey_wa.argtypes = [
        c_uint64,
        c_char_p,
        c_size_t,
        c_char_p,
        c_size_t,
        c_char_p,
        c_size_t,
        c_char_p,
        POINTER(c_ulong),
    ]
    ret.k2h_add_subkey_wa.restype = c_bool

    # close API
    # bool k2h_close(k2h_h handle)
    # bool k2h_close_wait(k2h_h handle, long waitms)
    ret.k2h_close_wait.argtypes = [c_uint64, c_long]
    ret.k2h_close_wait.restype = c_bool

    # create API
    # bool k2h_create(const char* filepath, int maskbitcnt, int cmaskbitcnt,
    # int maxelementcnt, size_t pagesize)
    ret.k2h_create.argtypes = [c_char_p, c_int, c_int, c_int, c_size_t]
    ret.k2h_create.restype = c_bool

    # disable tx API
    # bool k2h_disable_transaction(k2h_h handle)
    ret.k2h_disable_transaction.argtypes = [c_uint64]
    ret.k2h_disable_transaction.restype = c_bool

    # dump API
    # bool k2h_dump_head(k2h_h handle, FILE* stream)
    ret.k2h_dump_head.argtypes = [c_uint64, POINTER(FILE)]
    ret.k2h_dump_head.restype = c_bool
    # bool k2h_dump_keytable(k2h_h handle, FILE* stream)
    ret.k2h_dump_keytable.argtypes = [c_uint64, POINTER(FILE)]
    ret.k2h_dump_keytable.restype = c_bool
    # bool k2h_dump_full_keytable(k2h_h handle, FILE* stream)
    ret.k2h_dump_full_keytable.argtypes = [c_uint64, POINTER(FILE)]
    ret.k2h_dump_full_keytable.restype = c_bool
    # bool k2h_dump_elementtable(k2h_h handle, FILE* stream)
    ret.k2h_dump_elementtable.argtypes = [c_uint64, POINTER(FILE)]
    ret.k2h_dump_elementtable.restype = c_bool
    # bool k2h_dump_full(k2h_h handle, FILE* stream)
    ret.k2h_dump_full.argtypes = [c_uint64, POINTER(FILE)]
    ret.k2h_dump_full.restype = c_bool

    # get value API
    # char* k2h_get_str_direct_value_wp(k2h_h handle, const char* pkey, const char* pass)
    ret.k2h_get_str_direct_value_wp.argtypes = [c_uint64, c_char_p, c_char_p]
    ret.k2h_get_str_direct_value_wp.restype = c_char_p

    # get attrs API
    # PK2HATTRPCK k2h_get_direct_attrs(k2h_h handle, const unsigned char* pkey,
    # size_t keylength, int* pattrspckcnt)
    ret.k2h_get_direct_attrs.argtypes = [c_uint64, c_char_p, c_size_t, POINTER(c_int)]
    ret.k2h_get_direct_attrs.restype = POINTER(AttrPack)

    # get subkeys API
    # PK2HKEYPCK k2h_get_direct_subkeys(k2h_h handle, const unsigned char* pkey,
    # size_t keylength, int* pskeypckcnt)
    ret.k2h_get_direct_subkeys.argtypes = [c_uint64, c_char_p, c_size_t, POINTER(c_int)]
    ret.k2h_get_direct_subkeys.restype = POINTER(KeyPack)

    # get transaction API
    # int k2h_get_transaction_archive_fd(k2h_h handle)
    ret.k2h_get_transaction_archive_fd.argtypes = [c_uint64]
    ret.k2h_get_transaction_archive_fd.restype = c_int

    # int k2h_get_transaction_thread_pool(void)
    ret.k2h_get_transaction_thread_pool.argtypes = []
    ret.k2h_get_transaction_thread_pool.restype = c_int

    # load archive API
    # bool k2h_load_archive(k2h_h handle, const char* filepath, bool errskip)
    ret.k2h_load_archive.argtypes = [c_uint64, c_char_p, c_bool]
    ret.k2h_load_archive.restype = c_bool

    # open API
    ret.k2h_open_mem.restype = c_uint64
    ret.k2h_open_mem.argtypes = [c_int, c_int, c_int, c_int]
    ret.k2h_open.restype = c_uint64
    ret.k2h_open.argtypes = [
        c_char_p,
        c_bool,
        c_bool,
        c_bool,
        c_int,
        c_int,
        c_int,
        c_int,
    ]
    ret.k2h_open_rw.restype = c_uint64
    ret.k2h_open_rw.argtypes = [c_char_p, c_bool, c_int, c_int, c_int, c_int]
    ret.k2h_open_ro.restype = c_uint64
    ret.k2h_open_ro.argtypes = [c_char_p, c_bool, c_int, c_int, c_int, c_int]
    ret.k2h_open_tempfile.restype = c_uint64
    ret.k2h_open_tempfile.argtypes = [c_char_p, c_bool, c_int, c_int, c_int, c_int]

    # print API
    # bool k2h_print_attr_version(k2h_h handle, FILE* stream)
    ret.k2h_print_attr_version.argtypes = [c_uint64, POINTER(FILE)]
    ret.k2h_print_attr_version.restype = c_bool
    #
    # bool k2h_print_attr_information(k2h_h handle, FILE* stream)
    ret.k2h_print_attr_information.argtypes = [c_uint64, POINTER(FILE)]
    ret.k2h_print_attr_information.restype = c_bool
    #
    # bool k2h_print_state(k2h_h handle, FILE* stream)
    ret.k2h_print_state.argtypes = [c_uint64, POINTER(FILE)]
    ret.k2h_print_state.restype = c_bool

    # void k2h_print_version(FILE* stream)
    ret.k2h_print_version.argtypes = [POINTER(FILE)]
    ret.k2h_print_version.restype = None

    # put_archive API
    # bool k2h_put_archive(k2h_h handle, const char* filepath, bool errskip)
    ret.k2h_put_archive.argtypes = [c_uint64, c_char_p, c_bool]
    ret.k2h_put_archive.restype = c_bool

    # remove API
    # bool k2h_remove_str_all(k2h_h handle, const char* pkey)
    ret.k2h_remove_str_all.argtypes = [c_uint64, c_char_p]
    ret.k2h_remove_str_all.restype = c_bool
    # bool k2h_remove_str(k2h_h handle, const char* pkey)
    ret.k2h_remove_str.argtypes = [c_uint64, c_char_p]
    ret.k2h_remove_str.restype = c_bool

    # rename API
    # bool k2h_rename_str(k2h_h handle, const char* pkey, const char* pnewkey)
    ret.k2h_rename_str.argtypes = [c_uint64, c_char_p, c_char_p]
    ret.k2h_rename_str.restype = c_bool

    # remove subkey API
    # bool k2h_remove_str_subkey(k2h_h handle, const char* pkey, const char* psubkey)
    ret.k2h_remove_str_subkey.argtypes = [c_uint64, c_char_p, c_char_p]
    ret.k2h_remove_str_subkey.restype = c_bool

    # set_common_attr
    # bool k2h_set_common_attr(k2h_h handle, const bool* is_mtime, const bool* is_defenc,
    # const char* passfile, const bool* is_history, const c_ulong* expire)
    ret.k2h_set_common_attr.argtypes = [
        c_uint64,
        POINTER(c_bool),
        POINTER(c_bool),
        c_char_p,
        POINTER(c_bool),
        POINTER(c_ulong),
    ]
    ret.k2h_set_common_attr.restype = c_bool

    # set loglevel
    ret.k2h_set_debug_level_silent.argtypes = []
    ret.k2h_set_debug_level_silent.restype = None
    ret.k2h_set_debug_level_error.argtypes = []
    ret.k2h_set_debug_level_error.restype = None
    ret.k2h_set_debug_level_warning.argtypes = []
    ret.k2h_set_debug_level_warning.restype = None
    ret.k2h_set_debug_level_message.argtypes = []
    ret.k2h_set_debug_level_message.restype = None
    # set value
    # bool k2h_set_str_value_wa(k2h_h handle, const char* pkey, const char* pval, const char* pass,
    # const time_t* expire)
    ret.k2h_set_str_value_wa.argtypes = [
        c_uint64,
        c_char_p,
        c_char_p,
        c_char_p,
        POINTER(c_ulong),
    ]
    ret.k2h_set_str_value_wa.restype = c_bool

    # set transaction
    # bool k2h_transaction_param(k2h_h handle, bool enable, const char* transfile,
    # const unsigned char* pprefix, size_t prefixlen, const unsigned char* pparam, size_t paramlen)
    # bool k2h_transaction_param_we(k2h_h handle, bool enable, const char* transfile,
    # const unsigned char* pprefix, size_t prefixlen, const unsigned char* pparam, size_t paramlen,
    # const time_t* expire)
    ret.k2h_transaction_param_we.argtypes = [
        c_uint64,
        c_bool,
        c_char_p,
        c_char_p,
        c_size_t,
        c_char_p,
        c_size_t,
        POINTER(c_ulong),
    ]
    ret.k2h_transaction_param_we.restype = c_bool

    # bool k2h_set_transaction_thread_pool(int count)
    ret.k2h_set_transaction_thread_pool.argtypes = [c_int]
    ret.k2h_set_transaction_thread_pool.restype = c_int

    return ret


# Initializes library handlers
_init_library_handle()


# Gets library handler
def get_library_handle():
    """Gets C library handles"""
    return _init_library_handle()


# Configures logger using std logging. default puts to stderr in warning.
def _configure_logger(log_file="sys.stderr", log_level=logging.WARNING):
    LOG.setLevel(log_level)

    # 2. formatter
    formatter = logging.Formatter(
        "%(asctime)-15s %(levelname)s %(name)s:%(lineno)d %(message)s"
    )  # hardcoding

    # 3. log_file
    if log_file and isinstance(log_file, str):
        if log_file != "sys.stderr":
            # Add the log message handler to the logger
            handler = TimedRotatingFileHandler(
                log_file, when="midnight", encoding="UTF-8", backupCount=31
            )
            handler.setFormatter(formatter)
            LOG.addHandler(handler)
            return

    # default logger
    stream_handler = StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    LOG.addHandler(stream_handler)


# Configures the loglevel.
def set_log_level(log_level):
    """Sets the log level"""
    LOG.setLevel(log_level)


# Initializes loggging handlers
_configure_logger()

#
# import k2hash modules
#
from k2hash.k2hash import K2hash, K2hashIterator  # noqa: pylint:disable=wrong-import-position
from k2hash.basequeue import BaseQueue  # noqa: pylint:disable=wrong-import-position
from k2hash.keyqueue import KeyQueue  # noqa: pylint:disable=wrong-import-position
from k2hash.queue import Queue  # noqa: pylint:disable=wrong-import-position

#
# Local variables:
# tab-width: 4
# c-basic-offset: 4
# End:
# vim600: expandtab sw=4 ts=4 fdm=marker
# vim<600: expandtab sw=4 ts=4
#
