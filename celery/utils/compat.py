# -*- coding: utf-8 -*-
"""
    celery.utils.compat
    ~~~~~~~~~~~~~~~~~~~

    Backward compatible implementations of features
    only available in newer Python versions.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

############## py3k #########################################################
import sys

try:
    reload = reload                     # noqa
except NameError:
    from imp import reload              # noqa

try:
    from UserList import UserList       # noqa
except ImportError:
    from collections import UserList    # noqa

try:
    from UserDict import UserDict       # noqa
except ImportError:
    from collections import UserDict    # noqa

if sys.version_info >= (3, 0):
    from io import StringIO, BytesIO
    from .encoding import bytes_to_str

    class WhateverIO(StringIO):

        def write(self, data):
            StringIO.write(self, bytes_to_str(data))
else:
    try:
        from cStringIO import StringIO  # noqa
    except ImportError:
        from StringIO import StringIO   # noqa
    BytesIO = WhateverIO = StringIO     # noqa


############## collections.OrderedDict ######################################
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict  # noqa
