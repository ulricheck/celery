# -*- coding: utf-8 -*-
from __future__ import absolute_import

import atexit
import socket
import sys
import traceback

from .. import __version__, platforms
from .. import beat
from ..app import app_or_default
from ..app.abstract import configurated, from_config
from ..utils import qualname
from ..utils.log import LOG_LEVELS, mlevel
from ..utils.timeutils import humanize_seconds

STARTUP_INFO_FMT = """
Configuration ->
    . broker -> {conninfo}
    . loader -> {loader}
    . scheduler -> {scheduler}
{scheduler_info}
    . logfile -> {logfile}@{loglevel}
    . maxinterval -> {hmax_interval} ({max_interval}s)
""".strip()


class Beat(configurated):
    Service = beat.Service

    schedule = from_config("schedule_filename")
    scheduler_cls = from_config("scheduler")
    redirect_stdouts = from_config()
    redirect_stdouts_level = from_config()

    def __init__(self, max_interval=None, app=None, socket_timeout=30,
            pidfile=None, loglevel=None, logfile=None, **kwargs):
        """Starts the celerybeat task scheduler."""
        self.app = app = app_or_default(app)
        self.setup_defaults(kwargs, namespace="celerybeat")

        self.max_interval = max_interval
        self.socket_timeout = socket_timeout
        self.loglevel = mlevel(loglevel or "WARNING")
        self.logfile = logfile
        self.colored = app.log.colored(self.logfile)
        self.pidfile = pidfile

    def run(self):
        logger = self.setup_logging()
        print(str(self.colored.cyan(
            "celerybeat v{0} is starting.".format(__version__))))
        self.init_loader()
        self.set_process_title()
        self.start_scheduler(logger)

    def setup_logging(self):
        handled = self.app.log.setup_logging_subsystem(loglevel=self.loglevel,
                                                       logfile=self.logfile)
        logger = self.app.log.get_default_logger(name="celery.beat")
        if self.redirect_stdouts and not handled:
            self.app.log.redirect_stdouts_to_logger(logger,
                    loglevel=self.redirect_stdouts_level)
        return logger

    def start_scheduler(self, logger=None):
        c = self.colored
        if self.pidfile:
            pidlock = platforms.create_pidlock(self.pidfile).acquire()
            atexit.register(pidlock.release)
        beat = self.Service(app=self.app,
                            logger=logger,
                            max_interval=self.max_interval,
                            scheduler_cls=self.scheduler_cls,
                            schedule_filename=self.schedule)

        print(str(c.blue("__    ", c.magenta("-"),
                  c.blue("    ... __   "), c.magenta("-"),
                  c.blue("        _\n"),
                  c.reset(self.startup_info(beat)))))
        if self.socket_timeout:
            logger.debug("Setting default socket timeout to %r",
                         self.socket_timeout)
            socket.setdefaulttimeout(self.socket_timeout)
        try:
            self.install_sync_handler(beat)
            beat.start()
        except Exception as exc:
            logger.critical("celerybeat raised exception %s: %r\n%s",
                            exc.__class__, exc, traceback.format_exc(),
                            exc_info=sys.exc_info())

    def init_loader(self):
        # Run the worker init handler.
        # (Usually imports task modules and such.)
        self.app.loader.init_worker()

    def startup_info(self, beat):
        scheduler = beat.get_scheduler(lazy=True)
        return STARTUP_INFO_FMT.format(
            conninfo=self.app.broker_connection().as_uri(),
            logfile=self.logfile or "[stderr]",
            loglevel=LOG_LEVELS[self.loglevel],
            loader=qualname(self.app.loader),
            scheduler=qualname(scheduler),
            scheduler_info=scheduler.info,
            hmax_interval=humanize_seconds(beat.max_interval),
            max_interval=beat.max_interval,
        )

    def set_process_title(self):
        arg_start = "manage" in sys.argv[0] and 2 or 1
        platforms.set_process_title("celerybeat",
                               info=" ".join(sys.argv[arg_start:]))

    def install_sync_handler(self, beat):
        """Install a `SIGTERM` + `SIGINT` handler that saves
        the celerybeat schedule."""

        def _sync(signum, frame):
            beat.sync()
            raise SystemExit()

        platforms.signals.update(SIGTERM=_sync, SIGINT=_sync)
