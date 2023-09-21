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

from k2hash import K2hash

LOG = logging.getLogger(__name__)


class BaseQueue:  # noqa: pylint: disable=too-many-instance-attributes
    """
    Baseueue class provides methods to handle key/value pairs in k2hash hash database.
    """

    def __init__(  # noqa: pylint: disable=too-many-arguments
        self, k2h, fifo=True, prefix=None, password=None, expire_duration=None
    ):
        """
        Initialize a new BaseQueue instnace.
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

        # initializes self._handle, which should be set in subclasses
        self._handle = K2hash.K2H_INVALID_HANDLE

    @property
    def handle(self):
        """Returns a Queue handle."""
        return self._handle

    def __repr__(self):
        """Returns full of members as a string."""
        attrs = []
        for attr in [
            "_handle",
            "_k2h_handle",
            "_libk2hash",
            "_fifo",
            "_prefix",
            "_expire_duration",
        ]:  # should be hardcoded.
            val = getattr(self, attr)
            if val:
                attrs.append((attr, repr(val)))
            else:
                attrs.append((attr, ""))
            values = ", ".join(["%s=%s" % i for i in attrs])  # noqa: pylint:disable=consider-using-f-string
        return f"<_{self.__class__.__name__} " + values + ">"

#
# Local variables:
# tab-width: 4
# c-basic-offset: 4
# End:
# vim600: expandtab sw=4 ts=4 fdm=marker
# vim<600: expandtab sw=4 ts=4
#
