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

import logging
from ctypes import c_int, c_uint64, c_char_p, pointer
from k2hash import K2hash

LOG = logging.getLogger(__name__)


class KeyQueue:
    """
    KeyQueue class provides methods to handle key/value pairs in k2hash hash database.
    """
    def __init__(self,
                 k2h,
                 fifo=True,
                 prefix=None,
                 password=None,
                 expire_duration=None):
        """
        Initialize a new KeyQueue instnace.
        """
        if not isinstance(k2h, K2hash):
            raise TypeError("k2h should be a K2hash object")
        self._k2h_handle = k2h.handle
        self._libc = k2h.libc
        self._libk2hash = k2h.libk2hash

        if fifo and not isinstance(fifo, bool):
            raise TypeError("fifo should be a boolean object")
        self._fifo = fifo
        if prefix and not isinstance(prefix, str):
            raise TypeError("prefix should be a string object")
        self._prefix = prefix
        if password and not isinstance(password, str):
            raise TypeError("password should be a string object")
        self._password = password
        if expire_duration and not isinstance(expire_duration, int):
            raise TypeError("expire_duration should be a boolean object")
        if expire_duration and expire_duration <= 0:
            raise ValueError("expire_duration should not be positive")
        self._expire_duration = expire_duration

        handle = self._libk2hash.k2h_keyq_handle_str_prefix(
            self._k2h_handle, self._fifo,
            (c_char_p(self._prefix.encode()) if self._prefix else None))

        if handle == K2hash.K2H_INVALID_HANDLE:
            raise RuntimeError("handle should not be K2H_INVALID_HANDLE")
        self._handle = handle

    def put(self, obj):
        """Inserts an element into the tail of this queue.
        """
        if obj and not isinstance(obj, dict):
            raise TypeError("obj should be a dict object")
        if len(obj) == 0:
            raise ValueError("obj size should be greater than zero")
        for key, val in obj.items():
            if not isinstance(key, str):
                raise TypeError("key should be a str obj")
            if not isinstance(val, str):
                raise TypeError("val should be a str obj")

        for key, val in obj.items():
            res = self._libk2hash.k2h_keyq_str_push_keyval_wa(
                self._handle, c_char_p(key.encode()), c_char_p(val.encode()),
                (c_char_p(self._password.encode()) if self._password else
                 None), (pointer(c_uint64(self._expire_duration))
                         if self._expire_duration else None))
            if res:
                LOG.debug("q_push:{%s}", res)
            else:
                return False
        return True

    def clear(self):
        """Removes all of the elements from this collection (optional operation).
        """
        count = self.qsize()
        if count > 0:
            res = self._libk2hash.k2h_keyq_remove(self._handle, c_int(count))
            return res
        return True

    def close(self):
        """
        Free QueueHandle
        """
        res = self._libk2hash.k2h_keyq_free(self._handle)
        return res

    def qsize(self):
        """Returns the number of queue.
        """
        res = self._libk2hash.k2h_keyq_count(self._handle)
        return res

    def element(self, position=0):
        """Finds and gets a object from the head of this queue.
        """
        if not isinstance(position, int):
            raise TypeError("position should be a int object")
        if position < 0:
            raise ValueError("count should be positive")

        ppkey = pointer(c_char_p("".encode()))
        ppval = pointer(c_char_p("".encode()))
        res = self._libk2hash.k2h_keyq_str_read_keyval_wp(
            self._handle, ppkey, ppval, c_int(position),
            (c_char_p(self._password.encode()) if self._password else None))
        res = {}
        if ppkey.contents.value:
            pkey = ppkey.contents.value.decode()
            self._libc.free(ppkey.contents)
            res[pkey] = ""
        if ppval.contents.value:
            pval = ppval.contents.value.decode()
            self._libc.free(ppval.contents)
            res[pkey] = pval
        return res

    @property
    def handle(self):
        """Returns a Queue handle.
        """
        return self._handle

    def empty(self):
        """Returns true if, and only if, queue size is 0.
        """
        res = self._libk2hash.k2h_keyq_empty(self._handle)
        return res

    def get(self):
        """Finds and gets a object from the head of this queue.
        """
        ppkey = pointer(c_char_p("".encode()))
        ppval = pointer(c_char_p("".encode()))
        res = self._libk2hash.k2h_keyq_str_pop_keyval_wp(
            self._handle, ppkey, ppval,
            (c_char_p(self._password.encode()) if self._password else None))
        res = {}
        if ppkey.contents.value:
            pkey = ppkey.contents.value.decode()
            self._libc.free(ppkey.contents)
            res[pkey] = ""
        if ppval.contents.value:
            pval = ppval.contents.value.decode()
            self._libc.free(ppval.contents)
            res[pkey] = pval
        return res

    def print(self):
        """Print the objects in this queue.
        """
        res = self._libk2hash.k2h_keyq_dump(self._handle, None)
        return res

    def remove(self, count=1):
        """Removes objects from this queue.
        """
        if not isinstance(count, int):
            raise TypeError("count should be a int object")
        if count <= 0:
            raise ValueError("count should be positive")

        vals = []
        for _ in range(count):
            val = self._libk2hash.k2h_keyq_remove(self._handle, 1)
            if val:
                vals.append(val)
        return vals

    def __repr__(self):
        """Returns full of members as a string.
        """
        attrs = []
        for attr in [
                '_handle', '_k2h_handle', '_libk2hash', '_fifo', '_prefix',
                '_expire_duration'
        ]:  # should be hardcoded.
            val = getattr(self, attr)
            if val:
                attrs.append((attr, repr(val)))
            else:
                attrs.append((attr, ''))
            values = ', '.join(['%s=%s' % i for i in attrs])
        return '<_K2hQueue ' + values + '>'


#
# Local variables:
# tab-width: 4
# c-basic-offset: 4
# End:
# vim600: expandtab sw=4 ts=4 fdm=marker
# vim<600: expandtab sw=4 ts=4
#
