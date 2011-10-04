from __future__ import absolute_import
from __future__ import with_statement

import sys
import threading
import traceback
import warnings

from functools import wraps
from pprint import pprint

from kombu.utils import cached_property, gen_unique_id, kwdict  # noqa
uuid = gen_unique_id

from ..exceptions import CPendingDeprecationWarning, CDeprecationWarning

from .compat import StringIO
from .encoding import safe_repr as _safe_repr
from .imports import get_full_cls_name
from .log import LOG_LEVELS  # noqa

def kwdict(d):
    return d

PENDING_DEPRECATION_FMT = """
    %(description)s is scheduled for deprecation in \
    version %(deprecation)s and removal in version v%(removal)s. \
    %(alternative)s
"""

DEPRECATION_FMT = """
    %(description)s is deprecated and scheduled for removal in
    version %(removal)s. %(alternative)s
"""


def isatty(fh):
    # Fixes bug with mod_wsgi:
    #   mod_wsgi.Log object has no attribute isatty.
    return getattr(fh, "isatty", None) and fh.isatty()


def warn_deprecated(description=None, deprecation=None, removal=None,
        alternative=None):
    ctx = {"description": description,
           "deprecation": deprecation, "removal": removal,
           "alternative": alternative}
    if deprecation is not None:
        w = CPendingDeprecationWarning(PENDING_DEPRECATION_FMT % ctx)
    else:
        w = CDeprecationWarning(DEPRECATION_FMT % ctx)
    warnings.warn(w)


def deprecated(description=None, deprecation=None, removal=None,
        alternative=None):

    def _inner(fun):

        @wraps(fun)
        def __inner(*args, **kwargs):
            warn_deprecated(description=description or get_full_cls_name(fun),
                            deprecation=deprecation,
                            removal=removal,
                            alternative=alternative)
            return fun(*args, **kwargs)
        return __inner
    return _inner


def lpmerge(L, R):
    """Left precedent dictionary merge.  Keeps values from `l`, if the value
    in `r` is :const:`None`."""
    return dict(L, **dict((k, v) for k, v in R.iteritems() if v is not None))


def mattrgetter(*attrs):
    """Like :func:`operator.itemgetter` but returns :const:`None` on missing
    attributes instead of raising :exc:`AttributeError`."""
    return lambda obj: dict((attr, getattr(obj, attr, None))
                                for attr in attrs)


def cry():  # pragma: no cover
    """Return stacktrace of all active threads.

    From https://gist.github.com/737056

    """
    tmap = {}
    main_thread = None
    # get a map of threads by their ID so we can print their names
    # during the traceback dump
    for t in threading.enumerate():
        if getattr(t, "ident", None):
            tmap[t.ident] = t
        else:
            main_thread = t

    out = StringIO()
    sep = "=" * 49 + "\n"
    for tid, frame in sys._current_frames().iteritems():
        thread = tmap.get(tid, main_thread)
        out.write("%s\n" % (thread.getName(), ))
        out.write(sep)
        traceback.print_stack(frame, file=out)
        out.write(sep)
        out.write("LOCAL VARIABLES\n")
        out.write(sep)
        pprint(frame.f_locals, stream=out)
        out.write("\n\n")
    return out.getvalue()


def reprkwargs(kwargs, sep=', ', fmt="%s=%s"):
    return sep.join(fmt % (k, _safe_repr(v)) for k, v in kwargs.iteritems())


def reprcall(name, args=(), kwargs=(), sep=', '):
    return "%s(%s%s%s)" % (name, sep.join(map(_safe_repr, args)),
                           (args and kwargs) and sep or "",
                           reprkwargs(kwargs, sep))
