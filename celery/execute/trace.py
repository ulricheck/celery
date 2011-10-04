from __future__ import absolute_import

import os
import socket
import sys
import traceback
import warnings

from .. import current_app
from .. import states, signals
from ..datastructures import ExceptionInfo
from ..exceptions import RetryTaskError
from ..utils.serialization import get_pickleable_exception

_task_prerun = signals.task_prerun
_task_postrun = signals.task_postrun
_task_failure = signals.task_failure


class TraceInfo(object):

    def __init__(self, status=states.PENDING, retval=None, exc_info=None):
        self.status = status
        self.retval = retval
        self.exc_info = exc_info
        self.exc_type = None
        self.exc_value = None
        self.tb = None
        self.strtb = None
        if self.exc_info:
            self.exc_type, self.exc_value, self.tb = exc_info
            self.strtb = "\n".join(traceback.format_exception(*exc_info))

    @classmethod
    def trace(cls, fun, args, kwargs, propagate=False):
        """Trace the execution of a function, calling the appropiate callback
        if the function raises retry, an failure or returned successfully.

        :keyword propagate: If true, errors will propagate to the caller.

        """
        try:
            return cls(states.SUCCESS, retval=fun(*args, **kwargs))
        except RetryTaskError, exc:
            return cls(states.RETRY, retval=exc, exc_info=sys.exc_info())
        except Exception, exc:
            if propagate:
                raise
            return cls(states.FAILURE, retval=exc, exc_info=sys.exc_info())
        except BaseException, exc:
            raise
        except:  # pragma: no cover
            # For Python2.5 where raising strings are still allowed
            # (but deprecated)
            if propagate:
                raise
            return cls(states.FAILURE, retval=None, exc_info=sys.exc_info())


class TaskTrace(object):
    """Wraps the task in a jail, catches all exceptions, and
    saves the status and result of the task execution to the task
    meta backend.

    If the call was successful, it saves the result to the task result
    backend, and sets the task status to `"SUCCESS"`.

    If the call raises :exc:`~celery.exceptions.RetryTaskError`, it extracts
    the original exception, uses that as the result and sets the task status
    to `"RETRY"`.

    If the call results in an exception, it saves the exception as the task
    result, and sets the task status to `"FAILURE"`.

    :param name: The name of the task to execute.
    :param id: The unique id of the task.
    :param args: List of positional args to pass on to the function.
    :param kwargs: Keyword arguments mapping to pass on to the function.

    :keyword loader: Custom loader to use, if not specified the current app
      loader will be used.
    :keyword hostname: Custom hostname to use, if not specified the system
      hostname will be used.

    :returns: the evaluated functions return value on success, or
        the exception instance on failure.

    """

    def __init__(self, name, id, args, kwargs, task=None,
            request=None, propagate=None, loader=None, hostname=None):
        self.id = id
        self.name = name
        self.args = args
        self.kwargs = kwargs
        task = self.task = task or current_app.tasks[self.name]
        self.request = request or {}
        self.status = states.PENDING
        self.strtb = None
        self.propagate = propagate
        self.loader = loader or current_app.loader
        self.hostname = hostname or socket.gethostname()
        self._trace_handlers = {states.FAILURE: self.handle_failure,
                                states.RETRY: self.handle_retry,
                                states.SUCCESS: self.handle_success}

        self._store_errors = True
        if task.ignore_result:
            self._store_errors = task.store_errors_even_if_ignored

    def __call__(self):
        return self.execute()

    def process_cleanup(self):
        try:
            self.task.backend.process_cleanup()
            self.loader.on_process_cleanup()
        except (KeyboardInterrupt, SystemExit, MemoryError):
            raise
        except Exception, exc:
            logger = current_app.log.get_default_logger()
            logger.error("Process cleanup failed: %r", exc,
                            exc_info=sys.exc_info())

    def execute(self):
        self.loader.on_task_init(self.id, self.task)
        if self.task.track_started:
            if not self.task.ignore_result:
                self.task.backend.mark_as_started(self.id,
                                                  pid=os.getpid(),
                                                  hostname=self.hostname)
        self.task.request.update(self.request, args=self.args,
                                 called_directly=False, kwargs=self.kwargs)
        if _task_prerun.receivers:
            _task_prerun.send(sender=self.task, task_id=self.id,
                              task=self.task, args=self.args,
                              kwargs=self.kwargs)
        try:
            retval = self._trace()
        finally:
            self.task.request.clear()
            self.process_cleanup()
            if _task_postrun.receivers:
                _task_postrun.send(sender=self.task, id=self.id,
                                   task=self.task, args=self.args,
                                   kwargs=self.kwargs, retval=retval)
        return retval

    def execute_safe(self, *args, **kwargs):
        """Same as :meth:`execute`, but catches errors."""
        try:
            return self.execute(*args, **kwargs)
        except Exception, exc:
            _type, _value, _tb = sys.exc_info()
            _value = self.task.backend.prepare_exception(exc)
            exc_info = ExceptionInfo((_type, _value, _tb))
            warnings.warn("Exception outside body: %s: %s\n%s" % tuple(
                map(str, (exc.__class__, exc, exc_info.traceback))))
            return exc_info

    def _trace(self):
        trace = TraceInfo.trace(self.task, self.args, self.kwargs,
                                propagate=self.propagate)
        self.status = trace.status
        self.strtb = trace.strtb
        handler = self._trace_handlers[trace.status]
        r = handler(trace.retval, trace.exc_type, trace.tb, trace.strtb)
        self.handle_after_return(trace.status, trace.retval,
                                 trace.exc_type, trace.tb, trace.strtb,
                                 einfo=trace.exc_info)
        return r

    def handle_after_return(self, status, retval, type_, tb, strtb,
            einfo=None):
        if status in states.EXCEPTION_STATES:
            einfo = ExceptionInfo(einfo)
        self.task.after_return(status, retval, self.id,
                               self.args, self.kwargs, einfo)

    def handle_success(self, retval, *args):
        """Handle successful execution."""
        if not self.task.ignore_result:
            self.task.backend.mark_as_done(self.id, retval)
        try:
            on_success = self.task.on_success
        except AttributeError:
            pass
        else:
            on_success(retval, self.id, self.args, self.kwargs)
        return retval

    def handle_retry(self, exc, type_, tb, strtb):
        """Handle retry exception."""
        # Create a simpler version of the RetryTaskError that stringifies
        # the original exception instead of including the exception instance.
        # This is for reporting the retry in logs, email etc, while
        # guaranteeing pickleability.
        message, orig_exc = exc.args
        if self._store_errors:
            self.task.backend.mark_as_retry(self.id, orig_exc, strtb)
        expanded_msg = "%s: %s" % (message, str(orig_exc))
        einfo = ExceptionInfo((type_, type_(expanded_msg, None), tb))
        try:
            on_retry = self.task.on_retry
        except AttributeError:
            pass
        else:
            on_retry(exc, self.id, self.args, self.kwargs, einfo)
        return einfo

    def handle_failure(self, exc, type_, tb, strtb):
        """Handle exception."""
        if self._store_errors:
            self.task.backend.mark_as_failure(self.id, exc, strtb)
        exc = get_pickleable_exception(exc)
        einfo = ExceptionInfo((type_, exc, tb))
        try:
            on_failure = self.task.on_failure
        except AttributeError:
            pass
        else:
            on_failure(exc, self.id, self.args, self.kwargs, einfo)
        if _task_failure.receivers:
            _task_failure.send(sender=self.task, task_id=self.id,
                               exception=exc, args=self.args,
                               kwargs=self.kwargs, traceback=tb,
                               einfo=einfo)
        return einfo
