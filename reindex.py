#!/usr/bin/env python3
"""Bulk reindex OpenSearch indices from a ``source:destination`` list.

* Checkpoint file so an interrupted run resumes where it left off, re-attaching to
  tasks that are still running on the cluster.
* Strict verification of every task before the (opt-in) source delete.
* N concurrent jobs, each sliced automatically by OpenSearch.
* Live dashboard on the console; text and JSON-lines logs on disk.
* Dry run that sends only read requests and prints the writes it would have sent.
"""
from __future__ import annotations

import argparse
import getpass
import json
import logging
import os
import re
import signal
import sys
import tempfile
import threading
import time
import warnings
from collections import Counter, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from opensearchpy import OpenSearch
from opensearchpy import exceptions as os_exc
from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

STATE_VERSION = 1
LOG = logging.getLogger("reindex")
COUNT_FIELDS = ("total", "created", "updated", "deleted", "noops", "version_conflicts", "batches")
# Statuses that carry a task id worth re-attaching to.
REATTACH_STATUSES = ("running", "verifying", "failed")
UNSAFE_NAME_CHARS = set('*?,<>|\\/ "')
BAR_STYLE = dict(style="grey37", complete_style="cyan", finished_style="green")


# --------------------------------------------------------------------------- helpers
def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def fmt_secs(s: float) -> str:
    s = int(max(s, 0))
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}h{m:02d}m{sec:02d}s" if h else (f"{m}m{sec:02d}s" if m else f"{sec}s")


def fmt_eta(s: float) -> str:
    """Coarser than fmt_secs so a long ETA does not flicker."""
    if s < 3600:
        return "~" + fmt_secs(s)
    step = 300 if s >= 6 * 3600 else 60
    s = round(s / step) * step
    return f"~{int(s // 3600)}h{int(s % 3600 // 60):02d}m"


def describe(e: BaseException) -> str:
    if isinstance(e, os_exc.ConnectionError):
        return f"{type(e).__name__}: {e.error}"[:600]
    if isinstance(e, os_exc.TransportError):
        info = e.info if isinstance(e.info, dict) else {}
        err = info.get("error")
        reason = err.get("reason") if isinstance(err, dict) else err
        kind = err.get("type") if isinstance(err, dict) else None
        return f"{type(e).__name__} status={e.status_code} {kind or e.error}: {reason}"[:600]
    return f"{type(e).__name__}: {e}"[:600]


def is_auth_error(e: BaseException) -> bool:
    """401/403 from the security plugin, but not a cluster block (also a 403)."""
    if not isinstance(e, (os_exc.AuthenticationException, os_exc.AuthorizationException)):
        return False
    return "cluster_block_exception" not in describe(e)


def transient(e: BaseException) -> bool:
    if isinstance(e, (os_exc.ConnectionError, os_exc.ConnectionTimeout, os_exc.SerializationError)):
        return True
    if isinstance(e, os_exc.TransportError):
        code = e.status_code
        return code == "N/A" or code in (408, 429) or (isinstance(code, int) and code >= 500)
    return False


def task_counts(task: dict[str, Any]) -> dict[str, int]:
    status = (task.get("task") or {}).get("status") or {}
    return {k: int(status.get(k) or 0) for k in COUNT_FIELDS}


def processed(counts: dict[str, int] | None) -> int:
    """Docs a task has dealt with so far (its ``total`` is the target, not the progress)."""
    c = counts or {}
    return sum(c.get(k, 0) for k in ("created", "updated", "noops", "deleted", "version_conflicts"))


def summarise_failures(failures: list[Any]) -> str:
    kinds: Counter[str] = Counter()
    first = ""
    for f in failures:
        if not isinstance(f, dict):
            kinds["unknown"] += 1
            continue
        cause = f.get("cause") if isinstance(f.get("cause"), dict) else {}
        reason = f.get("reason") if isinstance(f.get("reason"), dict) else {}
        kind = cause.get("type") or reason.get("type") or f.get("type") or "unknown"
        kinds[kind] += 1
        if not first:
            first = str(cause.get("reason") or reason.get("reason") or f.get("reason") or f)[:300]
            doc = f.get("id")
            if doc:
                first = f"doc {doc}: {first}"
    top = ", ".join(f"{k} x{n}" for k, n in kinds.most_common(3))
    return f"{len(failures)} failure(s) [{top}]; first: {first}"


def check_name(kind: str, name: str) -> str | None:
    """Reject anything that is not one concrete index or alias name."""
    if name in ("_all",) or name.startswith(("-", "+")):
        return f"{kind} {name!r} is a special name, not one index"
    if UNSAFE_NAME_CHARS & set(name):
        return f"{kind} {name!r} looks like a pattern or a list; one concrete name per line"
    if name != name.lower():
        return f"{kind} {name!r} must be lowercase"
    return None


class JobError(Exception):
    """A job-level failure: recorded in the state file, run continues."""


class Cancelled(JobError):
    """The user cancelled the running task."""


class Unreachable(JobError):
    """Lost sight of a task that may still be running; keep it re-attachable."""


class DryRunViolation(RuntimeError):
    """A write was attempted during a dry run. Never expected."""


# --------------------------------------------------------------------------- input
@dataclass(frozen=True)
class Job:
    source: str
    dest: str
    line: int

    @property
    def key(self) -> str:
        return f"{self.source}:{self.dest}"


def load_jobs(path: Path) -> list[Job]:
    jobs: list[Job] = []
    seen: dict[str, int] = {}
    problems: list[str] = []
    for n, raw in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if ":" not in line:
            problems.append(f"line {n}: expected source:destination, got {raw.strip()!r}")
            continue
        src, dst = (p.strip() for p in line.split(":", 1))
        if not src or not dst:
            problems.append(f"line {n}: empty source or destination in {raw.strip()!r}")
            continue
        bad = check_name("source", src) or check_name("destination", dst)
        if bad:
            problems.append(f"line {n}: {bad}")
        elif src == dst:
            problems.append(f"line {n}: source and destination are the same ({src!r})")
        elif src in seen:
            problems.append(f"line {n}: source {src!r} already listed on line {seen[src]}")
        else:
            seen[src] = n
            jobs.append(Job(src, dst, n))
    if problems:
        raise ValueError(f"{path} has problems:\n  " + "\n  ".join(problems))
    if not jobs:
        raise ValueError(f"{path} contains no source:destination lines")
    return jobs


# --------------------------------------------------------------------------- state
class State:
    """Checkpoint file.

    Updates mark the state dirty; a flusher thread writes it (atomically: temp file,
    fsync, rename) at most once per second, and ``flush()`` forces a write for the
    transitions that must be durable immediately. A lock file stops two processes
    from sharing one state file.
    """

    def __init__(self, path: Path):
        self.path = path
        self.lock_path = path.with_name(path.name + ".lock")
        self.lock = threading.RLock()
        self.data: dict[str, Any] = {"version": STATE_VERSION, "started": utcnow(), "jobs": {}}
        self._dirty = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._locked = False

    def load(self) -> bool:
        if not self.path.exists():
            return False
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            raise ValueError(f"{self.path} is not valid JSON ({e}); fix or remove it") from e
        if data.get("version") != STATE_VERSION or not isinstance(data.get("jobs"), dict):
            raise ValueError(f"{self.path}: unsupported or corrupt state file (version {data.get('version')})")
        self.data = data
        return True

    def acquire(self) -> None:
        """Take the lock file, or explain who holds it."""
        for _ in range(2):
            try:
                fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                try:
                    pid = int(self.lock_path.read_text().strip() or 0)
                except (OSError, ValueError):
                    pid = 0
                if pid and _pid_alive(pid):
                    raise RuntimeError(f"{self.path} is in use by pid {pid} (lock file {self.lock_path})")
                self.lock_path.unlink(missing_ok=True)   # stale lock from a dead process
                continue
            with os.fdopen(fd, "w") as fh:
                fh.write(str(os.getpid()))
            self._locked = True
            return
        raise RuntimeError(f"could not create lock file {self.lock_path}")

    def job(self, key: str) -> dict[str, Any]:
        with self.lock:
            return self.data["jobs"].setdefault(key, {"status": "pending"})

    def update(self, key: str, **fields: Any) -> None:
        with self.lock:
            self.job(key).update(fields)
            self._dirty = True

    def flush(self) -> None:
        with self.lock:
            if not self._dirty:
                return
            self.data["updated"] = utcnow()
            payload = json.dumps(self.data, indent=1)
            last: OSError | None = None
            for attempt in range(5):
                try:
                    fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=".reindex-state-", suffix=".tmp")
                    with os.fdopen(fd, "w", encoding="utf-8") as fh:
                        fh.write(payload)
                        fh.flush()
                        os.fsync(fh.fileno())
                    os.replace(tmp, self.path)
                    self._dirty = False
                    return
                except OSError as e:   # antivirus / OneDrive / DrvFs hiccups on /mnt/c
                    last = e
                    time.sleep(0.2 * (attempt + 1))
            raise last  # type: ignore[misc]

    def start_flusher(self, interval: float = 1.0) -> None:
        def loop() -> None:
            while not self._stop.wait(interval):
                try:
                    self.flush()
                except OSError as e:
                    LOG.error("could not write state file %s: %s", self.path, e,
                              extra={"event": "state_write_failed"})
        self._thread = threading.Thread(target=loop, name="state-flush", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        try:
            self.flush()
        except OSError as e:
            LOG.error("final state write failed: %s", e, extra={"event": "state_write_failed"})
        if self._locked:
            self.lock_path.unlink(missing_ok=True)
            self._locked = False

    def clean_temp(self) -> None:
        for p in self.path.parent.glob(".reindex-state-*.tmp"):
            try:
                p.unlink()
            except OSError:
                pass


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


# --------------------------------------------------------------------------- cluster
@dataclass
class Snapshot:
    """One-shot view of the cluster's indices and aliases, taken at preflight."""
    indices: dict[str, dict[str, Any]] = field(default_factory=dict)      # name -> cat row
    aliases: dict[str, dict[str, bool]] = field(default_factory=dict)     # alias -> {index: is_write}
    data_streams: dict[str, dict[str, Any]] = field(default_factory=dict) # name -> {indices, write_index, template}
    templates: list[dict[str, Any]] = field(default_factory=list)         # composable templates, priority desc


class Cluster:
    def __init__(self, *, host: str, port: int, user: str | None, password: str | None,
                 use_ssl: bool, verify_certs: bool, ca_cert: str | None, timeout: int = 30,
                 pool_size: int = 10, dry_run: bool = False):
        kwargs: dict[str, Any] = dict(
            hosts=[{"host": host, "port": port}], use_ssl=use_ssl, verify_certs=verify_certs,
            ssl_show_warn=False, timeout=timeout, max_retries=5, retry_on_timeout=True,
            http_compress=True, pool_maxsize=pool_size,
        )
        if user:
            kwargs["http_auth"] = (user, password or "")
        if ca_cert:
            kwargs["ca_certs"] = ca_cert
        self.os = OpenSearch(**kwargs)
        # Non-idempotent requests go through a client that never retries, so a dropped
        # connection cannot start the same reindex twice.
        self.os_once = OpenSearch(**{**kwargs, "max_retries": 0, "retry_on_timeout": False})
        self.dry_run = dry_run
        self.reads: Counter[str] = Counter()

    # ---- guards / bookkeeping
    def _write(self, what: str) -> None:
        if self.dry_run:
            raise DryRunViolation(f"dry run: refused to {what}")

    def _read(self, what: str) -> None:
        self.reads[what] += 1

    # ---- read-only
    def info(self) -> dict[str, Any]:
        self._read("GET /")
        return self.os.info()

    def can_list_tasks(self) -> None:
        """Raises the auth error the poll loop would otherwise hit per job."""
        self._read("GET /_tasks?actions=*reindex")
        self.os.tasks.list(actions="*reindex")

    def snapshot(self) -> Snapshot:
        snap = Snapshot()
        self._read("GET /_cat/indices")
        for row in self.os.cat.indices(h="index,docs.count,status,health", format="json",
                                       expand_wildcards="all"):
            snap.indices[row["index"]] = row
        self._read("GET /_alias")
        for index, body in self.os.indices.get_alias().items():
            for alias, cfg in (body.get("aliases") or {}).items():
                snap.aliases.setdefault(alias, {})[index] = bool((cfg or {}).get("is_write_index"))
        self._read("GET /_data_stream")
        for ds in self.os.indices.get_data_stream().get("data_streams") or []:
            backing = [i["index_name"] for i in ds.get("indices") or []]
            snap.data_streams[ds["name"]] = {"indices": backing, "write_index": backing[-1] if backing else None,
                                             "template": ds.get("template")}
        self._read("GET /_index_template")
        for t in self.os.indices.get_index_template().get("index_templates") or []:
            body = t.get("index_template") or {}
            snap.templates.append({"name": t.get("name"), "patterns": list(body.get("index_patterns") or []),
                                   "priority": int(body.get("priority") or 0), "data_stream": "data_stream" in body})
        snap.templates.sort(key=lambda t: -t["priority"])
        return snap

    @staticmethod
    def match_template(snap: Snapshot, name: str) -> dict[str, Any] | None:
        """The composable index template that would apply to ``name`` (highest priority wins)."""
        for t in snap.templates:
            for pat in t["patterns"]:
                if re.fullmatch(re.escape(pat).replace(r"\*", ".*"), name):
                    return t
        return None

    def index_exists(self, name: str) -> bool:
        self._read("HEAD /{index}")
        return bool(self.os.indices.exists(index=name))

    def count(self, index: str, refresh: bool = True) -> int:
        if refresh:
            self._write(f"refresh {index}")
            self.os.indices.refresh(index=index)
        self._read("GET /{index}/_count")
        resp = self.os.count(index=index)
        shards = resp.get("_shards") or {}
        if shards.get("failed"):
            raise JobError(f"count of {index!r} had {shards['failed']} failed shard(s); index may be red")
        return int(resp["count"])

    def get_task(self, task_id: str) -> dict[str, Any]:
        self._read("GET /_tasks/{id}")
        return self.os.tasks.get(task_id=task_id)

    def child_counts(self, task_id: str) -> dict[str, int] | None:
        """Sum the counters of a sliced reindex's running child tasks.

        The parent's status only includes slices that have finished, so live
        progress is parent (finished slices) + children (running slices).
        """
        self._read("GET /_tasks?parent_task_id={id}")
        resp = self.os.tasks.list(parent_task_id=task_id, detailed=True)
        statuses = [t.get("status") or {} for n in (resp.get("nodes") or {}).values()
                    for t in (n.get("tasks") or {}).values()]
        if not statuses:
            return None
        return {k: sum(int(st.get(k) or 0) for st in statuses) for k in COUNT_FIELDS}

    def find_reindex_task(self, job: Job) -> str | None:
        """Id of a running top-level reindex for this exact source/dest, if any."""
        self._read("GET /_tasks?actions=*reindex&detailed")
        want = f"reindex from [{job.source}] to [{job.dest}]"
        resp = self.os.tasks.list(actions="*reindex", detailed=True)
        for node in (resp.get("nodes") or {}).values():
            for tid, t in (node.get("tasks") or {}).items():
                if not t.get("parent_task_id") and str(t.get("description") or "").startswith(want):
                    return tid
        return None

    # ---- writes
    def reindex_request(self, job: Job, *, slices: str, rps: float, conflicts: str,
                        require_alias: bool, dest_kind: str | None = None) -> tuple[str, dict[str, Any]]:
        params: dict[str, Any] = {"wait_for_completion": "false", "slices": slices,
                                  "requests_per_second": rps}
        if require_alias:
            params["require_alias"] = "true"
        body: dict[str, Any] = {"source": {"index": job.source}, "dest": {"index": job.dest},
                                "conflicts": conflicts}
        if dest_kind == "data_stream":
            body["dest"]["op_type"] = "create"   # data streams are append-only
            body["conflicts"] = "proceed"        # a re-run must not abort on docs already there
        return f"POST /_reindex?{urlencode(params)}", body

    def create_destination(self, name: str, kind: str) -> None:
        """Create a missing destination; the matching index template supplies its settings."""
        self._write(f"create {kind} {name}")
        try:
            if kind == "data_stream":
                self.os.indices.create_data_stream(name=name)
            else:
                self.os.indices.create(index=name)
        except os_exc.RequestError as e:
            if "resource_already_exists_exception" in describe(e):
                return   # another worker (or auto-create) got there first
            raise

    def start_reindex(self, job: Job, **opts: Any) -> str:
        self._write(f"start reindex {job.key}")
        path, body = self.reindex_request(job, **opts)
        params = dict(p.split("=", 1) for p in path.split("?", 1)[1].split("&"))
        try:
            resp = self.os_once.reindex(body=body, params=params)
        except os_exc.ConnectionError as e:
            # The server may have accepted the request before the connection dropped.
            found = self.find_reindex_task(job)
            if found:
                LOG.warning("reindex POST for %s failed (%s) but a matching task %s is running; adopting it",
                            job.key, describe(e), found, extra={"job": job.key, "event": "adopt"})
                return found
            raise
        task = resp.get("task") if isinstance(resp, dict) else None
        if not task:
            raise JobError(f"_reindex did not return a task id: {resp}")
        return str(task)

    def cancel_task(self, task_id: str) -> None:
        self._write(f"cancel task {task_id}")
        self.os.tasks.cancel(task_id=task_id)

    def delete_index(self, index: str) -> None:
        self._write(f"delete index {index}")
        try:
            resp = self.os.indices.delete(index=index,
                                          params={"expand_wildcards": "none", "allow_no_indices": "false"})
        except os_exc.NotFoundError:
            # A retried DELETE after the first one succeeded lands here.
            if self.index_exists(index):
                raise
            return
        if not resp.get("acknowledged"):
            raise JobError(f"delete of {index!r} was not acknowledged: {resp}")


# --------------------------------------------------------------------------- logging
class JobLog(logging.LoggerAdapter):
    """LoggerAdapter that merges per-call ``extra`` with the job context."""

    def process(self, msg, kwargs):
        extra = dict(self.extra or {})
        extra.update(kwargs.get("extra") or {})
        kwargs["extra"] = extra
        return msg, kwargs


class DefaultJobFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "job"):
            record.job = "-"
        return True


class ConsoleNoiseFilter(logging.Filter):
    """Plain-console mode: keep the tool's own lines, drop library tracebacks."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name.startswith("reindex"):
            return True
        if record.levelno < logging.WARNING:
            return False
        record.exc_info = None
        record.exc_text = None
        return True


class JsonFormatter(logging.Formatter):
    EXTRA = ("event", "task", "counts", "elapsed_s", "reason")

    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "job": getattr(record, "job", None),
            "message": record.getMessage(),
        }
        for k in self.EXTRA:
            if hasattr(record, k):
                doc[k] = getattr(record, k)
        if record.exc_info:
            doc["exception"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str)


class RingHandler(logging.Handler):
    """Keeps the last few records (and the last few problems) for the dashboard."""

    def __init__(self, size: int = 6, problems: int = 6):
        super().__init__(logging.INFO)
        self.buf: deque[tuple[float, int, str, str]] = deque(maxlen=size)
        self.problems: deque[tuple[float, int, str, str]] = deque(maxlen=problems)

    def emit(self, record: logging.LogRecord) -> None:
        row = (record.created, record.levelno, getattr(record, "job", "-"), record.getMessage())
        self.buf.append(row)
        if record.levelno >= logging.WARNING:
            self.problems.append(row)


def setup_logging(args: argparse.Namespace, tui: bool, console: Console) -> RingHandler:
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if args.debug else logging.INFO)
    if args.log_file:
        fh = RotatingFileHandler(args.log_file, maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s [%(job)s] %(message)s"))
        fh.addFilter(DefaultJobFilter())
        root.addHandler(fh)
    if args.json_log:
        jh = RotatingFileHandler(args.json_log, maxBytes=50 * 1024 * 1024, backupCount=5, encoding="utf-8")
        jh.setFormatter(JsonFormatter())
        root.addHandler(jh)
    if not tui:
        ch = RichHandler(console=console, show_path=False, rich_tracebacks=False, markup=False)
        ch.setLevel(logging.DEBUG if args.debug else logging.INFO)
        ch.setFormatter(logging.Formatter("[%(job)s] %(message)s"))
        ch.addFilter(DefaultJobFilter())
        ch.addFilter(ConsoleNoiseFilter())
        root.addHandler(ch)
    ring = RingHandler()
    LOG.addHandler(ring)
    # opensearch-py logs every request at INFO and every retry at WARNING with a traceback.
    logging.getLogger("opensearch").setLevel(logging.DEBUG if args.debug else logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")
    return ring


# --------------------------------------------------------------------------- runner
@dataclass
class JobView:
    """Runtime (not persisted) view of one job for the dashboard."""
    phase: str = "queued"
    task: str | None = None
    started_at: float = 0.0
    finished_at: float = 0.0
    counts: dict[str, int] | None = None
    source_count: int = 0


class Runner:
    def __init__(self, args: argparse.Namespace, cluster: Cluster, state: State,
                 jobs: list[Job], plan: dict[str, dict[str, Any]], cluster_name: str, console: Console):
        self.args, self.cluster, self.state, self.jobs, self.plan = args, cluster, state, jobs, plan
        self.cluster_name = cluster_name
        self.console = console
        self.soft_stop = threading.Event()
        self.hard_stop = threading.Event()
        self.lock = threading.Lock()
        self.views: dict[str, JobView] = {}
        self.results: dict[str, str] = {}   # key -> done|failed|lost|held|cancelled|skipped
        self.finished_docs = 0              # docs processed by jobs completed in this run
        self.deleted = 0
        self.samples: deque[tuple[float, int]] = deque(maxlen=600)
        self.t0 = time.monotonic()

    # ---- per-job pipeline
    def run_job(self, job: Job) -> str:
        key = job.key
        log = JobLog(LOG, {"job": key})
        with self.lock:
            view = self.views.setdefault(key, JobView())
            view.source_count = self.plan[key].get("count", 0)
        if self.soft_stop.is_set():
            log.info("not started: stop requested", extra={"event": "held"})
            self._finish(key, "held")
            return "held"
        result = "failed"
        counts: dict[str, int] | None = None
        try:
            result, counts = self._run_job(job, log)
        except Cancelled as e:
            st = self.state.job(key)
            if st.get("status") == "verified":
                log.warning("%s; job stays verified, delete pending", e, extra={"event": "cancelled"})
            else:
                # Back to pending: a re-run restarts the reindex (idempotent by _id).
                self._record(key, status="pending", finished=utcnow(), note=str(e), task=None)
                log.warning("%s; job reset to pending", e, extra={"event": "cancelled"})
            result = "cancelled"
        except Unreachable as e:
            # Keep status=running and the task id so the next run re-attaches.
            self._record(key, status="running", error=str(e))
            log.error("lost contact with the task; re-run to re-attach: %s", e,
                      extra={"event": "lost", "reason": str(e)})
            result = "lost"
        except (JobError, os_exc.OpenSearchException) as e:
            msg = describe(e)
            self._record(key, status="failed", finished=utcnow(), error=msg)
            log.error("failed: %s", msg, extra={"event": "failed", "reason": msg})
            if is_auth_error(e) and not self.soft_stop.is_set():
                self.soft_stop.set()
                log.error("permission error: no further jobs will start", extra={"event": "soft_stop"})
            result = "failed"
        except Exception as e:  # noqa: BLE001 - record and keep the batch going
            msg = describe(e)
            self._record(key, status="failed", finished=utcnow(), error=msg)
            log.exception("unexpected error: %s", msg, extra={"event": "failed", "reason": msg})
            result = "failed"
        finally:
            self._finish(key, result, counts)
        return result

    def _run_job(self, job: Job, log: JobLog) -> tuple[str, dict[str, int] | None]:
        key = job.key
        a = self.args
        st = self.state.job(key)
        counts: dict[str, int] | None = None
        task_id: str | None = None

        if st.get("status") != "verified":
            if st.get("task") and st.get("status") in REATTACH_STATUSES:
                self._phase(key, "re-attaching")
                try:
                    self.cluster.get_task(st["task"])
                    task_id = st["task"]
                    log.info("re-attached to task %s from previous run", task_id,
                             extra={"event": "reattach", "task": task_id})
                except os_exc.NotFoundError:
                    log.warning("previous task %s no longer exists on the cluster; restarting reindex",
                                st["task"], extra={"event": "reattach_failed", "task": st["task"]})
            if task_id is None:
                self._phase(key, "starting")
                found = self.cluster.find_reindex_task(job)
                if found:
                    task_id = found
                    self._record(key, status="running", task=task_id, error=None, flush=True)
                    log.warning("found a running reindex task %s for this job; adopting it instead of starting another",
                                task_id, extra={"event": "adopt", "task": task_id})
            if task_id is None:
                create = self.plan[key].get("create")
                if create and not self.cluster.index_exists(job.dest):
                    self._phase(key, "creating")
                    self.cluster.create_destination(job.dest, create)
                    log.info("created %s %s (template %s)", create.replace("_", " "), job.dest,
                             self.plan[key].get("template") or "none", extra={"event": "create"})
                source_count = self.cluster.count(job.source)
                with self.lock:
                    self.views[key].source_count = source_count
                self._record(key, status="running", started=utcnow(), finished=None, error=None,
                             note=None, task=None, source_count=source_count, deleted_source=False)
                task_id = self.cluster.start_reindex(
                    job, slices=a.slices, rps=a.requests_per_second, conflicts=a.conflicts,
                    require_alias=a.require_alias, dest_kind=self.plan[key].get("dest_kind"))
                self._record(key, task=task_id, flush=True)
                log.info("started task %s for %d docs", task_id, source_count,
                         extra={"event": "start", "task": task_id, "counts": {"total": source_count}})
            self._phase(key, "reindexing", task=task_id)
            task = self._wait(job, task_id, log)
            counts = task_counts(task)
            self._phase(key, "verifying")
            self._record(key, status="verifying", **counts)
            reasons = self._verify(job, task, log)
            if reasons:
                reason = "; ".join(reasons)
                self._record(key, status="failed", finished=utcnow(), error=reason)
                log.error("verification failed: %s", reason,
                          extra={"event": "verify_failed", "task": task_id, "reason": reason, "counts": counts})
                return "failed", counts
            self._record(key, status="verified", error=None, flush=True)
        else:
            counts = {k: int(st.get(k) or 0) for k in COUNT_FIELDS}
            task_id = st.get("task")
            log.info("already verified in a previous run; finishing", extra={"event": "resume_verified"})

        deleted = bool(st.get("deleted_source"))
        if a.delete_source and not deleted:
            if self.hard_stop.is_set():
                raise Cancelled("stop requested before the source delete")
            self._phase(key, "deleting")
            self.cluster.delete_index(job.source)
            deleted = True
            with self.lock:
                self.deleted += 1
            log.info("deleted source index %s", job.source, extra={"event": "delete", "task": task_id})
        self._record(key, status="done", finished=utcnow(), deleted_source=deleted, error=None)
        with self.lock:
            elapsed = time.monotonic() - self.views[key].started_at
        log.info("done: created=%d updated=%d noops=%d total=%d in %s%s",
                 counts["created"], counts["updated"], counts["noops"], counts["total"],
                 fmt_secs(elapsed), " (source deleted)" if deleted else "",
                 extra={"event": "done", "task": task_id, "elapsed_s": round(elapsed, 1), "counts": counts})
        return "done", counts

    def _wait(self, job: Job, task_id: str, log: JobLog) -> dict[str, Any]:
        grace_until: float | None = None
        warned_children = False
        while True:
            if self.hard_stop.is_set():
                try:
                    self.cluster.cancel_task(task_id)
                except os_exc.NotFoundError:
                    task = self.cluster.get_task(task_id)
                    if task.get("completed"):
                        log.info("task %s had already completed when cancel was requested", task_id)
                        return task
                except os_exc.OpenSearchException as e:
                    raise Unreachable(f"cancel of task {task_id} failed: {describe(e)}")
                raise Cancelled(f"task {task_id} cancelled by user")
            try:
                task = self.cluster.get_task(task_id)
                grace_until = None
            except os_exc.NotFoundError:
                raise JobError(f"task {task_id} is gone from the cluster (node restart, or the .tasks "
                               "index could not be written)")
            except os_exc.OpenSearchException as e:
                now = time.monotonic()
                if not transient(e):
                    raise Unreachable(f"poll of task {task_id} rejected: {describe(e)}")
                if grace_until is None:
                    grace_until = now + self.args.poll_grace
                if now > grace_until:
                    raise Unreachable(f"could not reach the cluster for {self.args.poll_grace:g}s: {describe(e)}")
                log.warning("poll failed (%s); retrying for another %s",
                            describe(e), fmt_secs(grace_until - now), extra={"event": "poll_retry"})
                self.hard_stop.wait(self.args.poll_interval)
                continue
            counts = task_counts(task)
            if not task.get("completed") and str(self.args.slices) != "1":
                try:
                    children = self.cluster.child_counts(task_id)
                except os_exc.OpenSearchException as e:
                    children = None
                    if not warned_children:
                        warned_children = True
                        log.warning("cannot list child tasks, progress will show 0 until slices finish: %s",
                                    describe(e), extra={"event": "child_list_failed"})
                if children:
                    counts = {k: counts[k] + children[k] for k in COUNT_FIELDS}
            with self.lock:
                self.views[job.key].counts = counts
            log.debug("poll: %s", counts, extra={"event": "poll", "task": task_id, "counts": counts})
            if task.get("completed"):
                if "error" in task:
                    err = task["error"]
                    if isinstance(err, dict):
                        inner = err.get("caused_by") or {}
                        reason = f"{err.get('type')}: {err.get('reason')}"
                        if inner.get("reason"):
                            reason += f" (caused by {inner.get('type')}: {inner.get('reason')})"
                    else:
                        reason = str(err)
                    raise JobError(f"task {task_id} ended with error: {reason}")
                return task
            self.hard_stop.wait(self.args.poll_interval)

    def _verify(self, job: Job, task: dict[str, Any], log: JobLog) -> list[str]:
        c = task_counts(task)
        response = task.get("response") or {}
        st = self.state.job(job.key)
        reasons: list[str] = []
        failures = response.get("failures") or []
        if failures:
            reasons.append(summarise_failures(failures))
        if response.get("canceled"):
            reasons.append(f"task was cancelled on the cluster: {response['canceled']}")
        if response.get("timed_out"):
            reasons.append("task timed out")
        processed = c["created"] + c["updated"] + c["noops"] + c["deleted"] + c["version_conflicts"]
        if processed != c["total"]:
            reasons.append(f"created+updated+noops+deleted+conflicts={processed} != total={c['total']}")
        kind = self.plan[job.key].get("dest_kind")
        if c["version_conflicts"] and kind == "data_stream":
            # op_type=create into an append-only target: a conflict means the doc is already there.
            log.info("%d docs were already present in %s (earlier attempt); accepted for a data stream",
                     c["version_conflicts"], job.dest, extra={"event": "already_present"})
        elif c["version_conflicts"] and self.args.conflicts != "proceed":
            reasons.append(f"{c['version_conflicts']} version conflicts")
        source_count = int(st.get("source_count") or 0)
        if c["total"] != source_count:
            reasons.append(f"task total={c['total']} != source doc count={source_count} at start "
                           "(source still receiving writes?)")
        if kind in ("index", "data_stream"):
            try:
                dest_count = self.cluster.count(job.dest)
            except (JobError, os_exc.OpenSearchException) as e:
                reasons.append(f"could not count destination: {describe(e)}")
            else:
                if dest_count < source_count:
                    reasons.append(f"destination has {dest_count} docs, source had {source_count}")
        return reasons

    # ---- bookkeeping
    def _record(self, key: str, flush: bool = False, **fields: Any) -> None:
        """State update that cannot take a worker down if the disk misbehaves."""
        try:
            self.state.update(key, **fields)
            if flush:
                self.state.flush()
        except OSError as e:
            LOG.error("could not write state file: %s", e, extra={"job": key, "event": "state_write_failed"})

    def _phase(self, key: str, phase: str, task: str | None = None) -> None:
        with self.lock:
            view = self.views[key]
            view.phase = phase
            if task:
                view.task = task
            if not view.started_at:
                view.started_at = time.monotonic()

    def _finish(self, key: str, result: str, counts: dict[str, int] | None = None) -> None:
        with self.lock:
            self.results[key] = result
            view = self.views.setdefault(key, JobView())
            view.phase = result
            view.finished_at = time.monotonic()
            if counts:
                view.counts = counts
                self.finished_docs += counts["total"]

    def tally(self) -> Counter[str]:
        with self.lock:
            return Counter(self.results.values())

    def docs_done(self) -> int:
        with self.lock:
            active = sum(processed(v.counts) for k, v in self.views.items()
                         if k not in self.results and v.counts)
            return self.finished_docs + active

    def planned_docs(self) -> int:
        """Docs this run still intends to process (held jobs drop out after a soft stop)."""
        with self.lock:
            total = 0
            for j in self.jobs:
                r = self.results.get(j.key)
                if r in ("held", "skipped", "cancelled", "lost"):
                    continue
                v = self.views.get(j.key)
                started = bool(v and v.started_at)
                if r is None and self.soft_stop.is_set() and not started:
                    continue
                live_total = (v.counts or {}).get("total", 0) if v and v.counts else 0
                if r in ("done", "failed") and live_total:
                    total += live_total
                else:
                    total += max(self.plan[j.key].get("count", 0), live_total)
            return total

    def rate(self, window: float = 30.0) -> float:
        now = time.monotonic()
        self.samples.append((now, self.docs_done()))
        old = self.samples[0]
        for s in self.samples:
            if now - s[0] <= window:
                old = s
                break
        if old is self.samples[-1] and len(self.samples) > 1:
            old = self.samples[-2]
        dt = now - old[0]
        return (self.samples[-1][1] - old[1]) / dt if dt > 0 else 0.0

    # ---- rendering
    def _mode_text(self) -> Text:
        a = self.args
        if self.hard_stop.is_set():
            return Text("CANCELLING running tasks", style="bold red")
        if self.soft_stop.is_set():
            return Text("STOPPING: finishing running jobs, starting no more", style="bold yellow")
        if a.delete_source:
            return Text("DELETE SOURCE after verification", style="bold red")
        return Text("keep sources", style="green")

    def render(self, ring: RingHandler) -> Group:
        a = self.args
        t = self.tally()
        with self.lock:
            active = {k: v for k, v in self.views.items() if k not in self.results}
        finished = sum(t.values())
        planned = self.planned_docs()
        docs_done = min(self.docs_done(), planned) if planned else self.docs_done()
        rate = self.rate()
        eta_rate = self.rate(120.0)
        eta = fmt_eta((planned - docs_done) / eta_rate) if eta_rate > 0 and planned > docs_done else "--"
        pct = (docs_done / planned * 100) if planned else 0.0
        width = self.console.size.width
        height = self.console.size.height

        title = Text.assemble(
            (f"{self.cluster_name}", "bold"), f" @ {a.host}:{a.port}  ·  ",
            (Path(str(a.list)).name, "bold"), f"  ·  {a.workers} worker(s), slices={a.slices}",
            f", rps={a.requests_per_second:g}" if a.requests_per_second > 0 else "", "  ·  ", self._mode_text())
        hdr = Table.grid(padding=(0, 2))
        hdr.add_column(style="bold cyan", justify="right")
        hdr.add_column()
        hdr.add_column(style="bold cyan", justify="right")
        hdr.add_column()
        hdr.add_column(style="bold cyan", justify="right")
        hdr.add_column()
        hdr.add_row("Docs", Text.assemble((f"{pct:5.1f}%", "bold"), f"  {docs_done:,} / {planned:,}"),
                    "Rate", f"{rate:,.0f} docs/s", "ETA", eta)
        hdr.add_row("Indices", f"{finished}/{len(self.jobs)}",
                    "Elapsed", fmt_secs(time.monotonic() - self.t0),
                    "Deleted", str(self.deleted) if a.delete_source else "-")
        counters = Text.assemble(
            ("done ", "green"), f"{t['done']}   ", ("failed ", "red"), f"{t['failed'] + t['lost']}   ",
            ("held ", "yellow"), f"{t['held'] + t['cancelled']}   ", ("skipped ", "dim"), f"{t['skipped']}   ",
            ("running ", "cyan"), f"{len(active)}")
        overall = Table.grid(padding=(0, 1), expand=True)
        overall.add_column(ratio=1)
        overall.add_column(justify="right")
        overall.add_row(ProgressBar(total=max(planned, 1), completed=docs_done, width=None, **BAR_STYLE), counters)
        header = Panel(Group(hdr, overall), title=title, title_align="left", border_style="cyan")

        problems_rows = len(ring.problems)
        events_rows = len(ring.buf) or 1
        overhead = 4 + 2 + 3 + (events_rows + 2) + (problems_rows + 2 if problems_rows else 0) + 1
        max_rows = max(3, height - overhead - 2)
        jobs = Table(box=box.SIMPLE_HEAD, expand=True, show_edge=False, pad_edge=False)
        jobs.add_column("Source → Destination", ratio=3, min_width=24, no_wrap=True, overflow="ellipsis")
        if width >= 120:
            jobs.add_column("Task", no_wrap=True, style="dim")
        jobs.add_column("Phase", no_wrap=True)
        jobs.add_column("Docs", justify="right", no_wrap=True)
        jobs.add_column("Progress", ratio=2, min_width=10)
        jobs.add_column("Rate", justify="right", no_wrap=True)
        jobs.add_column("Elapsed", justify="right", no_wrap=True)
        phase_style = {"reindexing": "cyan", "verifying": "yellow", "deleting": "bold red",
                       "re-attaching": "magenta", "starting": "magenta", "creating": "magenta"}
        rows = sorted(active.items(), key=lambda kv: kv[1].started_at or 1e18)
        for key, v in rows[:max_rows]:
            c = v.counts or {}
            got = processed(c)
            tot = c.get("total") or v.source_count
            elapsed = time.monotonic() - v.started_at if v.started_at else 0
            cells: list[Any] = [Text(key.replace(":", " → ", 1), overflow="ellipsis", no_wrap=True)]
            if width >= 120:
                cells.append(Text((v.task or "-").rsplit(":", 1)[-1]))
            cells += [Text(v.phase, style=phase_style.get(v.phase, "")),
                      f"{got:,} / {tot:,}",
                      ProgressBar(total=max(tot, 1), completed=min(got, tot), width=None, **BAR_STYLE),
                      f"{got / elapsed:,.0f}/s" if elapsed > 1 else "-",
                      fmt_secs(elapsed) if v.started_at else "-"]
            jobs.add_row(*cells)
        if len(rows) > max_rows:
            jobs.add_row(Text(f"+ {len(rows) - max_rows} more running", style="dim"),
                         *[""] * (len(jobs.columns) - 1))
        if not active:
            jobs.add_row(Text("no active jobs", style="dim"), *[""] * (len(jobs.columns) - 1))
        jobs_panel = Panel(jobs, title=f"Active jobs ({len(active)})", title_align="left", border_style="blue")

        def events_table(rows_: list[tuple[float, int, str, str]], empty: str) -> Table:
            ev = Table.grid(padding=(0, 1), expand=True)
            ev.add_column(style="dim", no_wrap=True, min_width=8)
            ev.add_column(no_wrap=True, min_width=4)
            ev.add_column(style="magenta", no_wrap=True, min_width=6, max_width=32, overflow="ellipsis")
            ev.add_column(ratio=1, no_wrap=True, overflow="ellipsis")
            for ts, lvl, jobkey, msg in rows_:
                style = "red" if lvl >= logging.ERROR else ("yellow" if lvl >= logging.WARNING else "")
                ev.add_row(datetime.fromtimestamp(ts).strftime("%H:%M:%S"),
                           Text({logging.ERROR: "ERR", logging.WARNING: "WARN"}.get(lvl, "INFO"), style=style),
                           Text(jobkey.split(":", 1)[0]), Text(msg, style=style))
            if not rows_:
                ev.add_row("", "", "", Text(empty, style="dim"))
            return ev

        parts: list[Any] = [header, jobs_panel]
        if ring.problems:
            parts.append(Panel(events_table(list(ring.problems), ""), title=f"Problems ({len(ring.problems)} recent)",
                               title_align="left", border_style="red"))
        parts.append(Panel(events_table(list(ring.buf), "no events yet"), title="Recent events",
                           title_align="left", border_style="magenta"))
        if self.hard_stop.is_set():
            foot = "Cancelling running tasks; the run ends when they acknowledge."
        elif self.soft_stop.is_set():
            foot = "Ctrl+C again: cancel the running tasks (they go back to pending)."
        else:
            foot = "Ctrl+C once: finish running jobs, start no more.  Twice: cancel running tasks."
        parts.append(Text(foot, style="dim"))
        return Group(*parts)

    # ---- orchestration
    def run(self, tui: bool, ring: RingHandler) -> int:
        runnable = [j for j in self.jobs if self.plan[j.key]["action"] not in ("skip", "error")]
        for j in self.jobs:
            if j not in runnable:
                with self.lock:
                    self.results[j.key] = "skipped"
                    self.views[j.key] = JobView(phase="skipped")
        with ThreadPoolExecutor(max_workers=self.args.workers, thread_name_prefix="job") as pool:
            futures: list[Future[str]] = [pool.submit(self.run_job, j) for j in runnable]
            last_report = time.monotonic()
            live_ok = tui
            if tui:
                try:
                    with Live(self.render(ring), console=self.console, refresh_per_second=4) as live:
                        while not all(f.done() for f in futures):
                            time.sleep(0.25)
                            live.update(self.render(ring))
                        live.update(self.render(ring))
                except Exception:  # noqa: BLE001 - a dashboard bug must not take the run down
                    LOG.exception("dashboard failed; continuing without it", extra={"event": "tui_failed"})
                    live_ok = False
            if not live_ok:
                while not all(f.done() for f in futures):
                    time.sleep(0.5)
                    if time.monotonic() - last_report >= 30:
                        last_report = time.monotonic()
                        t = self.tally()
                        planned, done = self.planned_docs(), self.docs_done()
                        r = self.rate(120.0)
                        LOG.info("progress: %d/%d indices done, %d failed, %d running, %s/%s docs, %.0f docs/s, ETA %s",
                                 t["done"], len(self.jobs), t["failed"] + t["lost"],
                                 len(self.views) - len(self.results), f"{done:,}", f"{planned:,}", r,
                                 fmt_eta((planned - done) / r) if r > 0 and planned > done else "--",
                                 extra={"event": "progress"})
        for j, f in zip(runnable, futures):
            exc = f.exception()
            if exc is not None:
                LOG.error("job crashed outside its handler: %s", describe(exc),
                          extra={"job": j.key, "event": "crashed", "reason": describe(exc)}, exc_info=exc)
                self._record(j.key, status="failed", finished=utcnow(), error=describe(exc))
            if j.key not in self.results:
                self._finish(j.key, "failed")
        t = self.tally()
        if t["held"] or t["cancelled"]:
            return 130
        return 1 if (t["failed"] or t["lost"]) else 0


# --------------------------------------------------------------------------- preflight
def preflight(cluster: Cluster, jobs: list[Job], state: State, args: argparse.Namespace
              ) -> tuple[dict[str, dict[str, Any]], list[str]]:
    plan: dict[str, dict[str, Any]] = {}
    problems: list[str] = []
    try:
        cluster.can_list_tasks()
    except os_exc.OpenSearchException as e:
        problems.append(f"user cannot list tasks, which the poll loop needs "
                        f"(cluster:monitor/tasks/lists): {describe(e)}")
        for job in jobs:
            plan[job.key] = {"status": "pending", "count": 0, "dest_kind": None, "action": "error",
                             "note": "task permissions missing"}
        return plan, problems
    snap = cluster.snapshot()

    def resolve(name: str) -> tuple[str, str]:
        if name in snap.data_streams:
            return "data_stream", name
        if name in snap.aliases:
            targets = snap.aliases[name]
            if len(targets) == 1:
                return "alias", next(iter(targets))
            writers = [i for i, w in targets.items() if w]
            if len(writers) == 1:
                return "alias", writers[0]
            raise JobError(f"alias {name!r} points to {len(targets)} indices with no single write index")
        if name in snap.indices:
            return "index", name
        raise JobError(f"{name!r} does not exist")

    for job in jobs:
        st = state.job(job.key)
        status = st.get("status", "pending")
        row: dict[str, Any] = {"status": status, "count": 0, "dest_kind": None, "action": "reindex", "note": "",
                               "create": None, "template": None}
        plan[job.key] = row
        if status == "done":
            row.update(action="skip", note="done" + (" (source deleted)" if st.get("deleted_source") else ""))
            continue
        try:
            if status == "failed":
                if st.get("task"):
                    try:
                        cluster.get_task(st["task"])
                        state.update(job.key, status="running", error=None)
                        status = "running"
                    except os_exc.NotFoundError:
                        pass
                if status == "failed":
                    if not args.retry_failed:
                        row.update(action="skip",
                                   note=f"failed earlier: {str(st.get('error'))[:60]} (use --retry-failed)")
                        continue
                    state.update(job.key, status="pending", error=None, task=None)
                    row.update(status="pending", note="retrying failed job")
            if status == "verified":
                row.update(action="delete" if args.delete_source else "finish",
                           note="verified earlier" + ("" if args.delete_source else ", source kept"))
            elif status in REATTACH_STATUSES and st.get("task"):
                row.update(action="re-attach", note=f"task {st['task']}")
            src_kind, _ = resolve(job.source)
            if src_kind == "index" and snap.indices[job.source].get("status") == "close":
                raise JobError("source index is closed")
            if args.delete_source and src_kind in ("alias", "data_stream"):
                raise JobError(f"source is a {src_kind.replace('_', ' ')}; refusing with --delete-source")
            if args.delete_source and src_kind == "index":
                owner = next((n for n, d in snap.data_streams.items() if d["write_index"] == job.source), None)
                if owner:
                    raise JobError(f"source is the write index of data stream {owner!r}; "
                                   "roll it over first or drop --delete-source")
            try:
                kind, target = resolve(job.dest)
            except JobError:
                if args.no_create:
                    raise JobError(f"destination {job.dest!r} does not exist (--no-create)")
                tpl = Cluster.match_template(snap, job.dest)
                if tpl is None and not args.create_plain:
                    raise JobError(f"destination {job.dest!r} does not exist and no index template matches it; "
                                   "create it first or pass --create-plain")
                kind = "data_stream" if tpl and tpl["data_stream"] else "index"
                target = job.dest
                row["create"] = kind
                row["template"] = tpl["name"] if tpl else None
                row["note"] = (f"create {kind.replace('_', ' ')} (template {tpl['name']})" if tpl
                               else "create plain index (no template!)")
            else:
                if kind == "index" and snap.indices[job.dest].get("status") == "close":
                    raise JobError("destination index is closed")
                if kind == "data_stream":
                    row["note"] = (row["note"] + " data stream").strip()
            if target == job.source:
                raise JobError(f"destination {job.dest!r} resolves to the source index")
            if args.require_alias and kind != "alias":
                raise JobError("destination is not an alias (--require-alias)")
            row["dest_kind"] = kind
            if kind == "alias":
                row["note"] = (row["note"] + f" alias → {target}").strip()
            if src_kind == "index":
                row["count"] = int(snap.indices[job.source].get("docs.count") or 0)
            else:
                members = (snap.aliases[job.source] if src_kind == "alias"
                           else snap.data_streams[job.source]["indices"])
                row["count"] = sum(int((snap.indices.get(i) or {}).get("docs.count") or 0) for i in members)
        except JobError as e:
            row.update(action="error", note=str(e))
            problems.append(f"{job.key}: {e}")
        except os_exc.OpenSearchException as e:
            row.update(action="error", note=describe(e))
            problems.append(f"{job.key}: {describe(e)}")
    return plan, problems


ACTION_STYLES = {"reindex": "green", "re-attach": "cyan", "delete": "bold red", "finish": "cyan",
                 "skip": "dim", "error": "bold red"}


def plan_table(jobs: list[Job], plan: dict[str, dict[str, Any]], delete_source: bool, limit: int = 80) -> Group:
    t = Table(title="Plan", box=box.SIMPLE_HEAD)
    t.add_column("#", justify="right", style="dim")
    t.add_column("Source", overflow="fold")
    t.add_column("Destination", overflow="fold")
    t.add_column("Docs", justify="right")
    t.add_column("Action")
    t.add_column("Note", overflow="fold")
    shown = 0
    hidden_skips = 0
    for i, j in enumerate(jobs, 1):
        p = plan[j.key]
        if p["action"] == "skip" and len(jobs) > limit:
            hidden_skips += 1
            continue
        if shown >= limit:
            continue
        shown += 1
        action = p["action"]
        if action == "reindex" and delete_source:
            action = "reindex+delete"
        if p.get("create"):
            action = "create+" + action
        t.add_row(str(i), Text(j.source), Text(j.dest), f"{p['count']:,}" if p["count"] else "-",
                  Text(action, style=ACTION_STYLES[p["action"]]), Text(p["note"]))
    tally = Counter(p["action"] for p in plan.values())
    docs = sum(p["count"] for p in plan.values() if p["action"] not in ("skip", "error"))
    summary = Text.assemble(
        f"{len(jobs)} job(s): ", (f"{tally['reindex']} to reindex", "green"),
        f" ({docs:,} docs), {tally['re-attach']} to re-attach, {tally['delete'] + tally['finish']} to finish, ",
        (f"{tally['skip']} skipped", "dim"), ", ", (f"{tally['error']} with errors", "red" if tally["error"] else ""))
    notes = []
    if hidden_skips:
        notes.append(Text(f"{hidden_skips} already-done/skipped rows not shown", style="dim"))
    if shown >= limit and len(jobs) - hidden_skips > limit:
        notes.append(Text(f"only the first {limit} rows shown", style="dim"))
    return Group(t, summary, *notes)


def writes_table(jobs: list[Job], plan: dict[str, dict[str, Any]], args: argparse.Namespace,
                 cluster: Cluster) -> Group:
    """Dry run: every write request a real run would send, per job, in order."""
    t = Table(title="Write requests a real run would send (none were sent)", box=box.SIMPLE_HEAD)
    t.add_column("Job", overflow="fold")
    t.add_column("Request", overflow="fold")
    t.add_column("Kind", no_wrap=True)
    harmless = Text("write (refresh)", style="dim")
    write = Text("write", style="yellow")
    create_kind = Text("write (create)", style="yellow")
    destructive = Text("DESTRUCTIVE", style="bold red")
    n_writes = n_destructive = n_create = 0
    for j in jobs:
        p = plan[j.key]
        if p["action"] in ("skip", "error"):
            continue
        first = True

        def row(req: str, kind: Text) -> None:
            nonlocal first
            t.add_row(Text(j.key.replace(":", " → ", 1)) if first else "", Text(req), kind)
            first = False

        if p["action"] == "reindex":
            if p.get("create"):
                tpl = f"  (template {p['template']})" if p.get("template") else "  (no template: dynamic mapping)"
                row(f"PUT /_data_stream/{j.dest}{tpl}" if p["create"] == "data_stream" else f"PUT /{j.dest}{tpl}",
                    create_kind)
                n_create += 1
            row(f"POST /{j.source}/_refresh  (exact doc count before starting)", harmless)
            path, body = cluster.reindex_request(j, slices=args.slices, rps=args.requests_per_second,
                                                 conflicts=args.conflicts, require_alias=args.require_alias,
                                                 dest_kind=p.get("dest_kind"))
            row(f"{path}\n{json.dumps(body)}", write)
            n_writes += 1
        if p["action"] in ("reindex", "re-attach") and p.get("dest_kind") in ("index", "data_stream"):
            row(f"POST /{j.dest}/_refresh  (verification count)", harmless)
        if args.delete_source and p["action"] in ("reindex", "re-attach", "delete"):
            row(f"DELETE /{j.source}?expand_wildcards=none&allow_no_indices=false  (only after verification passes)",
                destructive)
            n_destructive += 1
    if n_writes == 0 and n_destructive == 0:
        t.add_row(Text("nothing to do", style="dim"), "", "")
    lines = [t,
             Text.assemble("Conditional: ", ("POST /_tasks/<id>/_cancel", "yellow"),
                           " for each running task on a second Ctrl+C."),
             Text.assemble(f"Totals: {n_create} destination(s) to create, {n_writes} reindex request(s), ",
                           (f"{n_destructive} index delete(s)", "bold red" if n_destructive else ""), ".")]
    reads = ", ".join(f"{k} x{v}" for k, v in sorted(cluster.reads.items()))
    lines.append(Text(f"Read-only requests this dry run sent: {reads}", style="dim"))
    return Group(*lines)


def summary_table(jobs: list[Job], state: State, runner: Runner, limit: int = 80) -> Group:
    t = Table(title="Summary", box=box.SIMPLE_HEAD)
    t.add_column("Source", overflow="fold")
    t.add_column("Destination", overflow="fold")
    t.add_column("Result")
    t.add_column("Docs", justify="right")
    t.add_column("Time", justify="right")
    t.add_column("Detail", overflow="fold")
    styles = {"done": "green", "failed": "bold red", "lost": "bold yellow", "held": "yellow",
              "cancelled": "yellow", "skipped": "dim"}
    shown = 0
    for j in jobs:
        res = runner.results.get(j.key, "-")
        if res == "skipped" and len(jobs) > limit:
            continue
        if shown >= limit:
            break
        shown += 1
        st = state.job(j.key)
        v = runner.views.get(j.key)
        c = (v.counts if v and v.counts else None) or {k: st.get(k, 0) for k in COUNT_FIELDS}
        docs = f"{int(c.get('created') or 0) + int(c.get('updated') or 0):,}" if c.get("total") else "-"
        elapsed = fmt_secs((v.finished_at or time.monotonic()) - v.started_at) if v and v.started_at else "-"
        if res == "skipped":
            note = runner.plan[j.key]["note"]
        elif res == "held":
            note = "not started (stop requested)"
        elif res == "done" and st.get("deleted_source"):
            note = "source deleted"
        else:
            note = st.get("error") or st.get("note") or ""
            if res in ("failed", "lost") and st.get("task"):
                note = f"task {st['task']}: {note}"
        t.add_row(Text(j.source), Text(j.dest), Text(res, style=styles.get(res, "")), docs, elapsed,
                  Text(str(note)[:200]))
    tl = runner.tally()
    line = Text.assemble(
        ("done ", "green"), f"{tl['done']}  ", ("failed ", "red"), f"{tl['failed']}  ",
        ("lost ", "yellow"), f"{tl['lost']}  ", ("held ", "yellow"), f"{tl['held'] + tl['cancelled']}  ",
        ("skipped ", "dim"), f"{tl['skipped']}  ", f"deleted {runner.deleted}  ",
        f"elapsed {fmt_secs(time.monotonic() - runner.t0)}")
    return Group(t, line)


# --------------------------------------------------------------------------- cli
def env_default(name: str, default: Any) -> Any:
    return os.environ.get(name, default)


class HelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    def _get_help_string(self, action):  # hide "(default: False/None)" noise
        if action.default in (False, None, argparse.SUPPRESS):
            return action.help
        return super()._get_help_string(action)


EPILOG = """\
examples:
  export OS_PASSWORD='...'
  reindex --host os-client --user admin --dry-run              # read-only: plan + the writes it would send
  reindex --host os-client --user admin --workers 2            # reindex, keep sources
  reindex --host os-client --user admin --workers 2 --delete-source --yes
  reindex --host os-client --user admin --retry-failed         # after fixing a mapping

stopping: Ctrl+C (or SIGTERM) once finishes running jobs and starts no more; twice cancels
the running tasks (they go back to pending); a third time exits immediately.
exit codes: 0 all done/skipped, 1 a job failed, 2 configuration/preflight error, 130 interrupted.
"""


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="reindex",
        description="Bulk reindex OpenSearch indices from a source:destination list, with checkpointing.",
        epilog=EPILOG, formatter_class=HelpFormatter)
    g = p.add_argument_group("connection")
    g.add_argument("--host", default=env_default("OS_HOST", "localhost"), help="OpenSearch host [env OS_HOST]")
    g.add_argument("--port", type=int, default=env_default("OS_PORT", 9200), help="OpenSearch port [env OS_PORT]")
    g.add_argument("--user", default=env_default("OS_USER", "admin"),
                   help="basic-auth user, '' for no authentication [env OS_USER]")
    g.add_argument("--password-env", default="OS_PASSWORD", metavar="VAR",
                   help="name of the env var holding the password; prompts if unset on a TTY")
    g.add_argument("--no-ssl", action="store_true", help="use http:// instead of https://")
    g.add_argument("--verify-certs", action=argparse.BooleanOptionalAction,
                   default=str(env_default("OS_VERIFY_CERTS", "")).lower() in ("1", "true", "yes"),
                   help="verify TLS certificates [env OS_VERIFY_CERTS]")
    g.add_argument("--ca-cert", default=env_default("OS_CA_CERT", None), help="CA bundle path [env OS_CA_CERT]")
    g.add_argument("--timeout", type=int, default=30, help="per-request timeout in seconds")

    r = p.add_argument_group("reindex")
    r.add_argument("--list", type=Path, default=Path("list.txt"), help="source:destination list file")
    r.add_argument("--workers", type=int, default=1, help="indices to reindex concurrently")
    r.add_argument("--slices", default="auto", help="_reindex slices parameter (auto or an integer)")
    r.add_argument("--requests-per-second", type=float, default=-1,
                   help="throttle passed to _reindex; effectively documents per second per task "
                        "(a batch of 1000 docs waits 1000/N seconds); -1 = unlimited")
    r.add_argument("--conflicts", choices=("abort", "proceed"), default="abort",
                   help="what _reindex does on a version conflict (only reachable with external versioning)")
    r.add_argument("--require-alias", action="store_true", help="fail if a destination is not an alias")
    r.add_argument("--delete-source", action="store_true",
                   help="delete each source index after its reindex passes verification")
    r.add_argument("--skip-missing", action="store_true",
                   help="skip jobs that fail preflight instead of aborting the run")
    r.add_argument("--no-create", action="store_true",
                   help="do not create missing destinations (default: create a data stream or index from the "
                        "matching index template before the reindex)")
    r.add_argument("--create-plain", action="store_true",
                   help="allow creating a missing destination that matches no index template, as a plain "
                        "dynamically-mapped index")
    r.add_argument("--poll-interval", type=float, default=5, help="seconds between task polls")
    r.add_argument("--poll-grace", type=float, default=300,
                   help="seconds to keep retrying an unreachable cluster before giving up on a job "
                        "(the job stays re-attachable)")

    s = p.add_argument_group("run control")
    s.add_argument("--dry-run", action="store_true",
                   help="send only read requests; print the plan and every write a real run would send")
    s.add_argument("-y", "--yes", action="store_true", help="do not ask for confirmation")
    s.add_argument("--state-file", type=Path, default=Path("reindex-state.json"),
                   help="checkpoint file; one per batch, never shared between concurrent runs")
    s.add_argument("--reset-state", action="store_true", help="delete the state file before starting")
    s.add_argument("--retry-failed", action="store_true", help="re-run jobs recorded as failed")
    s.add_argument("--log-file", type=Path, default=Path("reindex.log"), help="text log ('' to disable)")
    s.add_argument("--json-log", type=Path, default=Path("reindex.jsonl"), help="JSON-lines log ('' to disable)")
    s.add_argument("--no-tui", action="store_true", help="plain log lines instead of the live dashboard")
    s.add_argument("--debug", action="store_true", help="log every poll and HTTP request to the log files")
    a = p.parse_args(argv)
    try:
        a.port = int(a.port)
    except (TypeError, ValueError):
        p.error(f"--port / OS_PORT must be an integer, got {a.port!r}")
    if a.workers < 1:
        p.error("--workers must be >= 1")
    if a.poll_interval <= 0:
        p.error("--poll-interval must be > 0")
    if str(a.log_file) in ("", "."):
        a.log_file = None
    if str(a.json_log) in ("", "."):
        a.json_log = None
    return a


def confirm(console: Console, prompt: str, yes: bool, word: str = "y") -> bool:
    if yes:
        return True
    if not sys.stdin.isatty():
        console.print(Text(f"{prompt} needs confirmation but stdin is not a terminal; pass --yes.", style="red"))
        return False
    hint = "[y/N]" if word == "y" else f"(type {word!r} to continue)"
    answer = console.input(Text(f"{prompt} {hint} ")).strip().lower()
    return answer in ("y", "yes") if word == "y" else answer == word


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    console = Console()
    if not console.is_terminal:
        console = Console(width=max(console.size.width, 120))
    tui = not args.no_tui and console.is_terminal
    ring = setup_logging(args, tui, console)
    err = lambda msg: console.print(Text(msg, style="red"))  # noqa: E731

    password = os.environ.get(args.password_env) if args.user else None
    if args.user and password is None:
        if not sys.stdin.isatty():
            err(f"set ${args.password_env} (or --password-env) when not running interactively")
            return 2
        password = getpass.getpass(f"Password for {args.user} (env {args.password_env} is unset): ")

    try:
        jobs = load_jobs(args.list)
    except (OSError, ValueError) as e:
        err(str(e))
        return 2

    state = State(args.state_file)
    state.clean_temp()
    if args.reset_state and args.state_file.exists():
        if args.dry_run:
            console.print(Text(f"dry run: would delete state file {args.state_file}", style="yellow"))
        else:
            if not confirm(console, f"Delete state file {args.state_file}?", args.yes):
                return 2
            args.state_file.unlink()
            LOG.info("state file %s deleted", args.state_file)
    if not (args.reset_state and args.dry_run):
        try:
            if state.load():
                LOG.info("loaded state from %s (%d jobs recorded)", args.state_file, len(state.data["jobs"]))
        except ValueError as e:
            err(str(e))
            return 2

    # Stop signals are honoured from here on; a third one falls back to the default action.
    soft_stop = threading.Event()
    hard_stop = threading.Event()
    signals_seen = 0

    def on_signal(signum, _frame):
        nonlocal signals_seen
        signals_seen += 1
        name = signal.Signals(signum).name
        if signals_seen == 1:
            soft_stop.set()
            LOG.warning("%s received: no new jobs will start; running jobs continue. Again cancels them.",
                        name, extra={"event": "soft_stop"})
        elif signals_seen == 2:
            hard_stop.set()
            LOG.warning("second %s: cancelling running tasks", name, extra={"event": "hard_stop"})
        else:
            LOG.warning("third %s: exiting now; state may be re-attachable", name, extra={"event": "force_exit"})
            state.close()
            signal.signal(signum, signal.SIG_DFL)
            os.kill(os.getpid(), signum)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    cluster = Cluster(host=args.host, port=args.port, user=args.user or None, password=password,
                      use_ssl=not args.no_ssl, verify_certs=args.verify_certs, ca_cert=args.ca_cert,
                      timeout=args.timeout, pool_size=max(10, args.workers * 2 + 2), dry_run=args.dry_run)
    try:
        info = cluster.info()
    except os_exc.OpenSearchException as e:
        err(f"cannot connect to {args.host}:{args.port}: {describe(e)}")
        LOG.error("cannot connect: %s", describe(e))
        return 2
    cluster_name = info.get("cluster_name", "?")
    version = (info.get("version") or {}).get("number", "?")
    dist = (info.get("version") or {}).get("distribution", "elasticsearch")
    LOG.info("connected to %s (%s %s)", cluster_name, dist, version)
    console.print(Text.assemble("Connected to ", (str(cluster_name), "bold"), f" ({dist} {version}); ",
                                f"{len(jobs)} job(s) in {args.list}",
                                ("   DRY RUN: read-only", "bold yellow") if args.dry_run else ""))

    plan, problems = preflight(cluster, jobs, state, args)
    console.print(plan_table(jobs, plan, args.delete_source))
    if soft_stop.is_set():
        console.print(Text("interrupted during preflight; nothing was started.", style="yellow"))
        return 130
    if problems:
        for pr in problems:
            LOG.error("preflight: %s", pr)
        if not args.skip_missing:
            err(f"{len(problems)} job(s) failed preflight. Fix them or pass --skip-missing.")
            if args.dry_run:
                console.print(writes_table(jobs, plan, args, cluster))
            return 2
        console.print(Text(f"{len(problems)} job(s) failed preflight and will be skipped.", style="yellow"))
    runnable = [j for j in jobs if plan[j.key]["action"] not in ("skip", "error")]
    if args.dry_run:
        console.print(writes_table(jobs, plan, args, cluster))
        console.print(Text(f"dry run: {len(runnable)} job(s) would run; nothing was written.", style="yellow"))
        return 0
    if not runnable:
        console.print(Text("nothing to do: every job is already done or skipped.", style="green"))
        return 0
    docs = sum(plan[j.key]["count"] for j in runnable)
    if args.delete_source:
        prompt = (f"On {cluster_name}: reindex {len(runnable)} job(s) ({docs:,} docs) with {args.workers} worker(s), "
                  f"then DELETE each source after verification. This cannot be undone.")
        console.print(Text(prompt, style="bold red"))
        if not confirm(console, "Continue?", args.yes, word="delete"):
            console.print("aborted")
            return 2
    elif not confirm(console, f"On {cluster_name}: reindex {len(runnable)} job(s) ({docs:,} docs) "
                              f"with {args.workers} worker(s)?", args.yes):
        console.print("aborted")
        return 2

    try:
        state.acquire()
    except RuntimeError as e:
        err(str(e))
        return 2
    try:
        state._dirty = True     # persist the plan (pending entries) now that the user confirmed
        state.flush()
        state.start_flusher()
        runner = Runner(args, cluster, state, jobs, plan, cluster_name, console)
        runner.soft_stop, runner.hard_stop = soft_stop, hard_stop
        LOG.info("run started: %d job(s), workers=%d, delete_source=%s", len(runnable), args.workers,
                 args.delete_source, extra={"event": "run_start"})
        code = runner.run(tui, ring)
    finally:
        state.close()
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    console.print(summary_table(jobs, state, runner))
    t = runner.tally()
    LOG.info("run finished: %d done, %d failed, %d lost, exit=%d", t["done"], t["failed"], t["lost"], code,
             extra={"event": "run_end", "elapsed_s": round(time.monotonic() - runner.t0, 1)})
    if t["failed"]:
        console.print(Text(f"{t['failed']} job(s) failed. Inspect with GET _tasks/<task id> in Dev Tools, "
                           "fix the cause, then re-run with --retry-failed.", style="red"))
    if t["lost"]:
        console.print(Text(f"{t['lost']} job(s) lost contact with their task; re-run to re-attach.", style="yellow"))
    if code == 130:
        console.print(Text("run interrupted. Re-run the same command to resume; running tasks are re-attached.",
                           style="yellow"))
    for label, p in (("text log", args.log_file), ("json log", args.json_log), ("state", args.state_file)):
        if p:
            console.print(Text(f"{label}: {p}", style="dim"))
    return code


if __name__ == "__main__":
    sys.exit(main())
