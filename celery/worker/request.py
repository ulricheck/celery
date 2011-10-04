from celery import current_app
from celery.datastructures import AttributeDict
from celery.worker.job import (WANTED_DELIVERY_INFO, InvalidTaskError,
                               kwdict, maybe_iso8601, WorkerTaskTrace)
from celery.worker import state


class Req(AttributeDict):

    def __hash__(self):
        return hash(self.id)


def execute_task(body, message, on_ack=None, loglevel=None,
            logfile=None, hostname=None, app=None, **kw):
    delivery_info = getattr(message, "delivery_info", {})
    delivery_info = dict((key, delivery_info.get(key))
                            for key in WANTED_DELIVERY_INFO)

    kwargs = body["kwargs"]
    if not hasattr(kwargs, "items"):
        raise InvalidTaskError("Task keyword arguments is not a mapping.")

    request = Req(name=body["task"],
                    task_name=body["task"],
                    id=body["id"],
                    taskset=body.get("taskset", None),
                    args=body["args"],
                    kwargs=kwdict(kwargs),
                    chord=body.get("chord"),
                    retries=body.get("retries", 0),
                    eta=maybe_iso8601(body.get("eta")),
                    expires=maybe_iso8601(body.get("expires")),
                    on_ack=on_ack,
                    delivery_info=delivery_info,
                    tz=body.get("tz", None),
                    loglevel=loglevel,
                    logfile=logfile,
                    hostname=hostname)

    state.task_reserved(request)
    on_ack()
    state.task_accepted(request)
    try:
        task = app.tasks[request.name]
        t = WorkerTaskTrace(task.name, request.id,
                            request.args, request.kwargs,
                            hostname=request.hostname,
                            loader=app.loader,
                            request=request)
        t.execute()
    finally:
        state.task_ready(request)
