from ..datastructures import AttributeDict
from ..utils import kwdict
from ..utils.timeutils import timezone, maybe_iso8601

from . import state
from .job import WANTED_DELIVERY_INFO, InvalidTaskError


class Request(dict):
    acknowledged = False
    _already_revoked = False

    def __init__(self, task, request, on_ack=None, event_dispatcher=None):
        self.task = task
        self.task_name = request["name"]
        self.on_ack = on_ack
        self.eventer = event_dispatcher
        self._store_errors = True
        if task.ignore_result:
            self._store_errors = task.store_errors_even_if_ignored
        dict.__init__(self, request)

    def __hash__(self):
        return hash(self["id"])

    def acknowledge(self):
        """Acknowledge task."""
        if not self.acknowledged:
            self.on_ack()
            self.acknowledged = True

    def revoked(self):
        """If revoked, skip task and mark state."""
        uuid = self["id"]
        if self._already_revoked:
            return True
        if self["expires"]:
            self.maybe_expire()
        if uuid in state.revoked:
            self.logger.warn("Skipping revoked task: %s[%s]",
                             self["name"], uuid)
            self.send_event("task-revoked", uuid=uuid)
            self.acknowledge()
            self._already_revoked = True
            return True
        return False

    def maybe_expire(self):
        """If expired, mark the task as revoked."""
        expires = self["expires"]
        if expires and datetime.now(self["tzlocal"]) > expires:
            state.revoked.add(self["id"])
            if self._store_errors:
                self.task.backend.mark_as_revoked(self["id"])

    def send_event(self, type, **fields):
        if self.eventer:
            self.eventer.send(type, **fields)


class Strategy(object):

    def __init__(self, task, logger=None, loglevel=None,
            logfile=None, hostname=None, event_dispatcher=None):
        self.logger= logger
        self.loglevel = loglevel
        self.logfile = logfile
        self.hostname = hostname
        self.event_dispatcher = event_dispatcher
        self.task = task
        self.app = self.task.app

    def __call__(self):
        task = self.task
        app = self.app
        xx = 0

        while 1:
            (body, message, on_ack) = (yield)
            delivery_info = getattr(message, "delivery_info", {})
            delivery_info = dict((key, delivery_info.get(key))
                                    for key in WANTED_DELIVERY_INFO)

            kwargs = body["kwargs"]
            if not hasattr(kwargs, "items"):
                raise InvalidTaskError("Task keyword arguments is not a mapping.")

            eta = maybe_iso8601(body.get("eta"))
            expires = maybe_iso8601(body.get("expires"))
            tz = body.get("tz", None)
            tzlocal = timezone.tz_or_local(app.conf.CELERY_TIMEZONE)
            if eta is not None:
                eta = timezone.to_local(eta, tzlocal, tz)
            if expires is not None:
                expires = timezone.to_local(expires, tzlocal, tz)


            store_errors = True
            if task.ignore_result:
                store_errors = task.store_errors_even_if_ignored

            request = {"name": task.name,
                    "id": body["id"],
                    "taskset": body.get("taskset", None),
                    "args": body["args"],
                    "kwargs": kwdict(kwargs),
                    "chord": body.get("chord"),
                    "retries": body.get("retries", 0),
                    "eta": eta,
                    "expires": expires,
                    "delivery_info": delivery_info,
                    "tz": body.get("tz", None),
                    "tzlocal": tzlocal,
                    "is_eager": False,
                    "tz": tz}
            request = Request(task, request, on_ack, self.event_dispatcher)

            if not request.revoked():
                state.task_reserved(request)
                if not task.acks_late:
                    request.acknowledge()

                state.task_accepted(request)
                try:
                    task.request.update(request)
                    #t = WorkerTaskTrace(task.name, request.id,
                    #        request.args, request.kwargs,
                    #        hostname=request.hostname,
                    #        loader=app.loader,
                    #        request=request)
                    #yield t.execute()
                    task(*request["args"], **request["kwargs"])
                finally:
                    state.task_ready(request)
