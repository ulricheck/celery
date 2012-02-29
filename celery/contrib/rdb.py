# -*- coding: utf-8 -*-
"""
celery.contrib.rdb
==================

Remote debugger for Celery tasks running in multiprocessing pool workers.
Inspired by http://snippets.dzone.com/posts/show/7248

**Usage**

.. code-block:: python

    from celery.contrib import rdb
    from celery.task import task

    @task
    def add(x, y):
        result = x + y
        rdb.set_trace()
        return result


**Environment Variables**

.. envvar:: CELERY_RDB_HOST

    Hostname to bind to.  Default is '127.0.01', which means the socket
    will only be accessible from the local host.

.. envvar:: CELERY_RDB_PORT

    Base port to bind to.  Default is 6899.
    The debugger will try to find an available port starting from the
    base port.  The selected port will be logged by celeryd.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import, print_function

import errno
import os
import socket
import sys

from pdb import Pdb

default_port = 6899

CELERY_RDB_HOST = os.environ.get("CELERY_RDB_HOST") or "127.0.0.1"
CELERY_RDB_PORT = int(os.environ.get("CELERY_RDB_PORT") or default_port)

#: Holds the currently active debugger.
_current = [None]

_frame = getattr(sys, "_getframe")

NO_AVAILABLE_PORT = """\
{self.ident}: Couldn't find an available port.

Please specify one using the CELERY_RDB_PORT environment variable.
"""

BANNER = """\
{self.ident}: Please telnet into {self.host} {self.port}.

Type `exit` in session to continue.

{self.ident}: Waiting for client...
"""

SESSION_STARTED = "{self.ident}: Now in session with {self.remote_addr}."
SESSION_ENDED = "{self.ident}: Session with {self.remote_addr} ended."


class Rdb(Pdb):
    me = "Remote Debugger"
    _prev_outs = None
    _sock = None

    def __init__(self, host=CELERY_RDB_HOST, port=CELERY_RDB_PORT,
            port_search_limit=100, port_skew=+0):
        self.active = True

        try:
            from multiprocessing import current_process
            _, port_skew = current_process().name.split('-')
        except (ImportError, ValueError):
            pass
        port_skew = int(port_skew)

        self._prev_handles = sys.stdin, sys.stdout
        this_port = None
        for i in xrange(port_search_limit):
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            this_port = port + port_skew + i
            try:
                self._sock.bind((host, this_port))
            except socket.error as exc:
                if exc.errno in [errno.EADDRINUSE, errno.EINVAL]:
                    continue
                raise
            else:
                break
        else:
            raise Exception(NO_AVAILABLE_PORT.format(self=self))

        self._sock.listen(1)
        self.ident = ':'.join([self.me, this_port])
        self.host = host
        self.port = this_port
        print(BANNER.format(self=self), file=sys.stderr)

        self._client, address = self._sock.accept()
        self.remote_addr = ":".join(map(str, address))
        print(SESSION_STARTED.format(self=self), file=sys.stderr)
        self._handle = sys.stdin = sys.stdout = self._client.makefile("rw")
        Pdb.__init__(self, completekey="tab",
                           stdin=self._handle, stdout=self._handle)

    def _close_session(self):
        self.stdin, self.stdout = sys.stdin, sys.stdout = self._prev_handles
        self._handle.close()
        self._client.close()
        self._sock.close()
        self.active = False
        print(SESSION_ENDED.format(self=self), file=sys.stderr)

    def do_continue(self, arg):
        self._close_session()
        self.set_continue()
        return 1
    do_c = do_cont = do_continue

    def do_quit(self, arg):
        self._close_session()
        self.set_quit()
        return 1
    do_q = do_exit = do_quit

    def set_trace(self, frame=None):
        if frame is None:
            frame = _frame().f_back
        try:
            Pdb.set_trace(self, frame)
        except socket.error as exc:
            # connection reset by peer.
            if exc.errno != errno.ECONNRESET:
                raise

    def set_quit(self):
        # this raises a BdbQuit exception that we are unable to catch.
        sys.settrace(None)


def debugger():
    """Returns the current debugger instance (if any),
    or creates a new one."""
    rdb = _current[0]
    if rdb is None or not rdb.active:
        rdb = _current[0] = Rdb()
    return rdb


def set_trace(frame=None):
    """Set breakpoint at current location, or a specified frame"""
    if frame is None:
        frame = _frame().f_back
    return debugger().set_trace(frame)
