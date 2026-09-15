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
import base64
import getpass
import json
import logging
import math
import os
import queue
import re
import select
import signal
import ssl
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import warnings
try:
    import termios
    import tty
except ImportError:  # Windows
    termios = None  # type: ignore[assignment]
    tty = None  # type: ignore[assignment]
from collections import Counter, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

from opensearchpy import OpenSearch
from opensearchpy import exceptions as os_exc
from rich import box
from rich.console import Console, Group
from rich.layout import Layout
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
    """Two largest units, spilling into days and weeks: ~3w 2d, ~4d 6h, ~5h 20m, ~12m 05s."""
    s = int(max(s, 0))
    if s >= 7 * 86400:
        w, rem = divmod(s, 7 * 86400)
        return f"~{w}w {rem // 86400}d"
    if s >= 86400:
        d, rem = divmod(s, 86400)
        return f"~{d}d {rem // 3600}h"
    if s >= 3600:
        return f"~{s // 3600}h {s % 3600 // 60:02d}m"
    return f"~{s // 60}m {s % 60:02d}s"


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
        for row in self.os.cat.indices(h="index,docs.count,store.size,status,health", format="json",
                                       expand_wildcards="all", bytes="b"):
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
            settings = (body.get("template") or {}).get("settings") or {}
            shards = (settings.get("index.number_of_shards")
                      or (settings.get("index") or {}).get("number_of_shards")
                      or settings.get("number_of_shards"))
            snap.templates.append({"name": t.get("name"), "patterns": list(body.get("index_patterns") or []),
                                   "priority": int(body.get("priority") or 0), "data_stream": "data_stream" in body,
                                   "shards": int(shards) if shards else None})
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

    def list_reindex_tasks(self) -> dict[str, dict[str, int]]:
        """Counters of every running top-level reindex task, with running slices summed in."""
        self._read("GET /_tasks?actions=*reindex&detailed")
        resp = self.os.tasks.list(actions="*reindex", detailed=True)
        parents: dict[str, dict[str, int]] = {}
        children: dict[str, dict[str, int]] = {}
        for node in (resp.get("nodes") or {}).values():
            for tid, t in (node.get("tasks") or {}).items():
                counts = {k: int((t.get("status") or {}).get(k) or 0) for k in COUNT_FIELDS}
                parent = t.get("parent_task_id")
                if parent:
                    acc = children.setdefault(parent, dict.fromkeys(COUNT_FIELDS, 0))
                    for k in COUNT_FIELDS:
                        acc[k] += counts[k]
                else:
                    parents[tid] = counts
        for tid, acc in children.items():
            if tid in parents:
                parents[tid] = {k: parents[tid][k] + acc[k] for k in COUNT_FIELDS}
        return parents

    def health(self) -> dict[str, Any]:
        self._read("GET /_cluster/health")
        return self.os.cluster.health()

    def write_pool(self) -> list[dict[str, Any]]:
        self._read("GET /_cat/thread_pool/write")
        return list(self.os.cat.thread_pool(thread_pool_patterns="write", h="node_name,active,queue,rejected",
                                            format="json"))

    def write_index_of(self, name: str, kind: str) -> str:
        if kind != "data_stream":
            return name
        self._read("GET /_data_stream/{name}")
        ds = self.os.indices.get_data_stream(name=name)["data_streams"][0]
        return ds["indices"][-1]["index_name"]

    def get_index_settings(self, index: str) -> dict[str, Any]:
        self._read("GET /{index}/_settings")
        resp = self.os.indices.get_settings(index=index, name="index.number_of_replicas,index.refresh_interval")
        idx = ((next(iter(resp.values())) or {}).get("settings") or {}).get("index") or {}
        return {"number_of_replicas": idx.get("number_of_replicas"), "refresh_interval": idx.get("refresh_interval")}

    def put_index_settings(self, index: str, settings: dict[str, Any]) -> None:
        self._write(f"change settings of {index}")
        self.os.indices.put_settings(index=index, body={"index": settings})

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
                        require_alias: bool, dest_kind: str | None = None,
                        batch_size: int = 1000) -> tuple[str, dict[str, Any]]:
        params: dict[str, Any] = {"wait_for_completion": "false", "slices": slices,
                                  "requests_per_second": int(rps) if float(rps).is_integer() else rps}
        if require_alias:
            params["require_alias"] = "true"
        body: dict[str, Any] = {"source": {"index": job.source}, "dest": {"index": job.dest},
                                "conflicts": conflicts}
        if batch_size and batch_size != 1000:
            body["source"]["size"] = int(batch_size)
        if dest_kind == "data_stream":
            body["dest"]["op_type"] = "create"   # data streams are append-only
            body["conflicts"] = "proceed"        # a re-run must not abort on docs already there
        return f"POST /_reindex?{urlencode(params)}", body

    def create_destination(self, name: str, kind: str, shards: int | None = None) -> None:
        """Create a missing destination; the matching index template supplies its settings."""
        self._write(f"create {kind} {name}")
        try:
            if kind == "data_stream":
                self.os.indices.create_data_stream(name=name)
            elif shards:
                self.os.indices.create(index=name, body={"settings": {"index.number_of_shards": int(shards)}})
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


class DashboardsError(RuntimeError):
    """An OpenSearch Dashboards request failed: bad URL, credentials, tenant, or index-pattern id."""


class Dashboards:
    """Saved-objects client for OpenSearch Dashboards, used to refresh index patterns.

    Standard library HTTP only; the Dashboards API is plain JSON over ``/api``. Reads
    are counted in ``reads`` and every write goes through ``_write()``, like ``Cluster``.
    """
    META_FIELDS = ("_source", "_id", "_type", "_index", "_score")

    def __init__(self, *, url: str, user: str | None, password: str | None, verify_certs: bool,
                 ca_cert: str | None, tenant: str | None = None, timeout: int = 30, dry_run: bool = False):
        self.url = url.rstrip("/")
        self.tenant = tenant
        self.timeout = timeout
        self.dry_run = dry_run
        self.reads: Counter[str] = Counter()
        self.headers = {"osd-xsrf": "true", "Accept": "application/json"}
        if user:
            token = base64.b64encode(f"{user}:{password or ''}".encode()).decode()
            self.headers["Authorization"] = f"Basic {token}"
        if tenant:
            self.headers["securitytenant"] = tenant
        ctx = ssl.create_default_context(cafile=ca_cert) if ca_cert else ssl.create_default_context()
        if not verify_certs:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        self.opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))

    # ---- guards / bookkeeping
    def _write(self, what: str) -> None:
        if self.dry_run:
            raise DryRunViolation(f"dry run: refused to {what}")

    def _read(self, what: str) -> None:
        self.reads[what] += 1

    def _request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        data = json.dumps(body).encode() if body is not None else None
        headers = dict(self.headers)
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self.url + path, data=data, headers=headers, method=method)
        try:
            with self.opener.open(req, timeout=self.timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace").strip()[:300]
            hint = {401: "check --dashboards-user and its password",
                    403: "user has no access to this saved object or tenant",
                    404: "no such index pattern in this tenant, or not a Dashboards URL"}.get(e.code)
            raise DashboardsError(f"{method} {path}: HTTP {e.code}" + (f" ({hint})" if hint else "")
                                  + (f": {detail}" if detail else "")) from e
        except urllib.error.URLError as e:
            raise DashboardsError(f"{method} {self.url}{path}: {e.reason}") from e
        except OSError as e:   # timeouts, TLS handshake failures
            raise DashboardsError(f"{method} {self.url}{path}: {e}") from e
        try:
            return json.loads(raw) if raw else {}
        except json.JSONDecodeError as e:
            raise DashboardsError(f"{method} {path}: response is not JSON; is {self.url} the Dashboards URL "
                                  "(port 5601), and is the login not redirected?") from e

    # ---- read-only
    def get_index_pattern(self, pattern_id: str) -> dict[str, Any]:
        """The index-pattern saved object; 401/403/404 become a DashboardsError with a hint."""
        self._read("GET /api/saved_objects/index-pattern/{id}")
        return self._request("GET", f"/api/saved_objects/index-pattern/{quote(pattern_id, safe='')}")

    def fields_for(self, title: str) -> list[dict[str, Any]]:
        """Field list for a pattern title: what the UI's 'refresh field list' button fetches."""
        self._read("GET /api/index_patterns/_fields_for_wildcard")
        query = urlencode([("pattern", title)] + [("meta_fields", m) for m in self.META_FIELDS])
        resp = self._request("GET", f"/api/index_patterns/_fields_for_wildcard?{query}")
        fields = resp.get("fields")
        if not isinstance(fields, list):
            raise DashboardsError(f"_fields_for_wildcard for {title!r} returned no field list: {str(resp)[:200]}")
        return fields

    # ---- writes
    def refresh(self, pattern_id: str) -> tuple[str, int]:
        """Store a freshly fetched field list on the pattern. Returns (title, number of fields)."""
        obj = self.get_index_pattern(pattern_id)
        title = str((obj.get("attributes") or {}).get("title") or "")
        if not title:
            raise DashboardsError(f"index pattern {pattern_id!r} has no title: {str(obj)[:200]}")
        fields = self.fields_for(title)
        self._write(f"update index pattern {pattern_id}")
        self._request("PUT", f"/api/saved_objects/index-pattern/{quote(pattern_id, safe='')}",
                      {"attributes": {"fields": json.dumps(fields)}})
        return title, len(fields)


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

    def __init__(self, size: int = 40, problems: int = 12):
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


def sparkline(values: list[float], rows: int = 2) -> list[Text]:
    """btop-style block graph: ``rows`` lines of block characters, oldest value first."""
    blocks = " ▁▂▃▄▅▆▇█"
    peak = max(values) if values and max(values) > 0 else 1.0
    lines: list[Text] = []
    for r in range(rows):
        t = Text()
        for v in values:
            level = v / peak * 8 * rows
            row_level = level - 8 * (rows - 1 - r)
            idx = int(min(8, max(0, round(row_level))))
            style = "bright_cyan" if v >= 0.66 * peak else ("cyan" if v >= 0.33 * peak else "blue")
            t.append(blocks[idx], style=style)
        lines.append(t)
    return lines


DEFAULT_TZ = "America/Chicago"


def resolve_tz(name: str) -> tuple[str, ZoneInfo | None]:
    """Map a zone name or alias to (resolved name, ZoneInfo); 'local' is the system zone (None).

    Raises KeyError (zoneinfo's not-found error) or ValueError for names that do not resolve.
    """
    key = name.strip()
    if key.lower() == "local":
        return "local", None
    if key.lower() == "utc":
        key = "UTC"
    if not key:
        raise ValueError("empty zone name")
    return key, ZoneInfo(key)


def now_in(tz: ZoneInfo | None) -> datetime:
    """Aware current time in ``tz``, or in the system zone when None."""
    return datetime.now(tz) if tz is not None else datetime.now().astimezone()


def fmt_finish(dt: datetime, tz: ZoneInfo | None = None) -> str:
    """Local (or given-zone) completion time: 'today 14:32 CDT', 'tomorrow ...', 'Thu 18 Sep ...'."""
    if tz is not None:
        dt = dt.astimezone(tz)
    now = datetime.now(dt.tzinfo)
    if dt.date() == now.date():
        return dt.strftime("today %H:%M %Z").strip()
    if (dt.date() - now.date()).days == 1:
        return dt.strftime("tomorrow %H:%M %Z").strip()
    return dt.strftime("%a %d %b %H:%M %Z").strip()


class Dialog:
    """A modal box shown in place of the jobs table while it is open.

    Subclasses render themselves with ``render()`` and consume keys with
    ``handle(ch)``; returning True closes the dialog.
    """
    title = "dialog"

    def render(self, width: int, height: int) -> Panel:
        return Panel(Text("press Esc to close", style="dim"), title=self.title, border_style="yellow")

    def handle(self, ch: str) -> bool:
        return ch in ("\x1b", "q")


class ConfirmDialog(Dialog):
    """Yes/no question: ``y`` runs ``on_yes`` and closes; any other key just closes."""

    def __init__(self, title: str, message: str, on_yes: Callable[[], None]):
        self.title, self.message, self.on_yes = title, message, on_yes

    def render(self, width: int, height: int) -> Panel:
        return Panel(Text(self.message), title=self.title, border_style="red")

    def handle(self, ch: str) -> bool:
        if ch == "y":
            self.on_yes()
        return True

class TimezoneDialog(Dialog):
    """Pick the zone used by the header clock and the finish times (key z)."""
    title = "timezone"
    CHOICES = (("America/Chicago", "US Central"), ("America/New_York", "US Eastern"),
               ("America/Denver", "US Mountain"), ("America/Los_Angeles", "US Pacific"),
               ("America/Anchorage", "US Alaska"), ("Pacific/Honolulu", "US Hawaii"),
               ("UTC", ""), ("local", "system"))

    def __init__(self, runner: Runner):
        self.runner = runner
        self.typing = False      # True while the "type a zone name" line is active
        self.buffer = ""
        self.error = ""

    def current(self) -> str:
        return self.runner.tz.key if self.runner.tz is not None else "local"

    def apply(self, name: str) -> bool:
        try:
            resolved, tz = resolve_tz(name)
        except (KeyError, ValueError):
            self.error = f"unknown zone {name.strip()!r} (IANA name, 'utc' or 'local')"
            return False
        self.runner.tz = tz
        self.error = ""
        LOG.info("timezone set to %s", resolved, extra={"event": "timezone"})
        return True

    def render(self, width: int, height: int) -> Panel:
        cur = self.current()
        rows = Table.grid(padding=(0, 2))
        rows.add_column(no_wrap=True)
        rows.add_column(no_wrap=True)
        rows.add_column(no_wrap=True)
        for n, (name, desc) in enumerate(self.CHOICES, 1):
            try:
                clock = now_in(resolve_tz(name)[1]).strftime("%H:%M %Z").strip()
            except (KeyError, ValueError):
                clock = "unavailable"
            style = "bold green" if name == cur else ""
            rows.add_row(Text(str(n), style="bold"), Text(f"{name} ({desc})" if desc else name, style=style),
                         Text(clock, style=style or "dim"))
        rows.add_row(Text("9", style="bold"), Text("type a zone name", style="bold green" if self.typing else ""),
                     Text(""))
        lines: list[Any] = [rows]
        if self.typing:
            lines.append(Text.assemble(("zone: ", "dim"), self.buffer, ("_", "blink")))
            hint = "Enter applies, Backspace deletes, Esc cancels"
        else:
            hint = "press a number; Esc or q closes"
        lines.append(Text(self.error, style="bold red") if self.error else Text(hint, style="dim"))
        return Panel(Group(*lines), title=f"{self.title}: {cur}", title_align="left", border_style="yellow",
                     padding=(0, 1))

    def handle(self, ch: str) -> bool:
        if self.typing:
            if ch == "\x1b":
                self.typing, self.buffer, self.error = False, "", ""
            elif ch in ("\r", "\n"):
                return self.apply(self.buffer)
            elif ch in ("\x7f", "\x08"):
                self.buffer = self.buffer[:-1]
            elif ch.isprintable():
                self.buffer += ch
            return False
        if ch in ("\x1b", "q"):
            return True
        if ch == "9":
            self.typing, self.buffer, self.error = True, "", ""
            return False
        if ch.isdigit() and 1 <= int(ch) <= len(self.CHOICES):
            return self.apply(self.CHOICES[int(ch) - 1][0])
        return False


class Runner:
    POOL_CAP = 64

    def __init__(self, args: argparse.Namespace, cluster: Cluster, state: State,
                 jobs: list[Job], plan: dict[str, dict[str, Any]], cluster_name: str, console: Console,
                 cluster_stats: dict[str, Any] | None = None):
        self.args, self.cluster, self.state, self.jobs, self.plan = args, cluster, state, jobs, plan
        self.cluster_name = cluster_name
        self.console = console
        self.soft_stop = threading.Event()
        self.hard_stop = threading.Event()
        self.paused = False
        self.max_active = args.workers
        self.lock = threading.Lock()
        self.views: dict[str, JobView] = {}
        self.results: dict[str, str] = {}   # key -> done|failed|lost|held|cancelled|skipped
        self.finished_docs = 0              # docs processed by jobs completed in this run
        self.deleted = 0
        self.samples: deque[tuple[float, int]] = deque(maxlen=2400)
        self.rate_smooth = 0.0
        self.rate_ts = 0.0
        self.t0 = time.monotonic()
        # shared poller
        self.task_jobs: dict[str, str] = {}
        self.poll_cv = threading.Condition()
        self.poll_gen = 0
        self.poll_tasks: dict[str, dict[str, int]] = {}
        self.poll_error: BaseException | None = None
        self.poll_stop = threading.Event()
        self.cluster_stats: dict[str, Any] = dict(cluster_stats or {})
        self.rejected_base: int | None = None
        # keyboard: key -> (help label, action). Extend via add_key(); the footer is built from it.
        self.keys: queue.Queue[str] = queue.Queue()
        self.keys_stop = threading.Event()
        self.key_bindings: dict[str, tuple[str, Callable[[], None]]] = {}
        self.dialog: Dialog | None = None
        self.graph_window = 900.0            # seconds shown by the throughput graph
        self.tz: ZoneInfo | None = getattr(args, "tzinfo", None)
        self._default_keys()

    # ---- key bindings (shared hook: every feature registers here)
    def add_key(self, ch: str, label: str, action: Callable[[], None], aliases: tuple[str, ...] = ()) -> None:
        self.key_bindings[ch] = (label, action)
        for a in aliases:
            self.key_bindings[a] = ("", action)

    def _default_keys(self) -> None:
        def soft() -> None:
            if not self.soft_stop.is_set():
                self.soft_stop.set()
                LOG.warning("q pressed: no new jobs will start; running jobs continue", extra={"event": "soft_stop"})

        def hard() -> None:
            if not self.hard_stop.is_set():
                self.soft_stop.set()
                self.hard_stop.set()
                LOG.warning("Q pressed: cancelling running tasks", extra={"event": "hard_stop"})

        def fewer() -> None:
            if self.max_active > 1:
                self.max_active -= 1
                LOG.info("concurrency lowered to %d job(s)", self.max_active, extra={"event": "slots"})

        def more() -> None:
            if self.max_active < self.POOL_CAP:
                self.max_active += 1
                LOG.info("concurrency raised to %d job(s)", self.max_active, extra={"event": "slots"})

        def pause() -> None:
            self.paused = not self.paused
            LOG.warning("launching %s", "paused" if self.paused else "resumed", extra={"event": "pause"})

        self.add_key("q", "q stop", soft)
        self.add_key("Q", "Q cancel", hard)
        self.add_key("-", "-/+ slots", fewer, aliases=("_",))
        self.add_key("+", "", more, aliases=("=",))
        self.add_key("p", "p pause", pause)

        def label(ch: str, name: str, on: bool) -> str:
            return f"{ch} {name}:{'on' if on else 'off'}"

        def set_toggle(ch: str, name: str, attr: str, value: bool, on: bool, level: int = logging.INFO) -> None:
            """Store ``value`` in args.<attr>; ``on`` is what the footer and log call it."""
            setattr(self.args, attr, value)
            self.key_bindings[ch] = (label(ch, name, on), self.key_bindings[ch][1])
            LOG.log(level, "%s turned %s (%s key)", name, "on" if on else "off", ch,
                    extra={"event": "toggle", "setting": attr, "value": value})

        def toggle_delete() -> None:
            if self.args.delete_source:
                set_toggle("d", "delete", "delete_source", False, on=False)
                return
            self.dialog = ConfirmDialog(
                "delete source?",
                "Delete each source index after its reindex verifies. Applies to jobs that have not "
                "reached the delete step yet. Press y to confirm, any other key to cancel.",
                lambda: set_toggle("d", "delete", "delete_source", True, on=True, level=logging.WARNING))

        def toggle_tune() -> None:
            on = not self.args.tune_dest
            set_toggle("t", "tune", "tune_dest", on, on=on)

        def toggle_create() -> None:
            # args.no_create is the inverse of the label; jobs planned to create check it when they start.
            on = bool(self.args.no_create)
            set_toggle("c", "create", "no_create", not on, on=on)

        self.add_key("d", label("d", "delete", bool(self.args.delete_source)), toggle_delete)
        self.add_key("t", label("t", "tune", bool(self.args.tune_dest)), toggle_tune)
        self.add_key("c", label("c", "create", not self.args.no_create), toggle_create)
        def zone() -> None:
            self.dialog = TimezoneDialog(self)

        self.add_key("z", "z timezone", zone)

    def key_help(self) -> Text:
        """Footer text built from the bindings: labels are 'KEY description'."""
        t = Text()
        for label, _ in self.key_bindings.values():
            if label:
                key, _, desc = label.partition(" ")
                t.append(key, style="bold")
                t.append(f" {desc}  ")
        return t

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
        dest_kind = self.plan[key].get("dest_kind")

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
                    if a.no_create:
                        raise JobError("destination missing and creation is turned off (c key)")
                    self._phase(key, "creating")
                    shards = self.plan[key].get("shards")
                    self.cluster.create_destination(job.dest, create, shards=shards)
                    log.info("created %s %s (template %s%s)", create.replace("_", " "), job.dest,
                             self.plan[key].get("template") or "none",
                             f", {shards} primary shards" if shards else "", extra={"event": "create"})
                if a.tune_dest and dest_kind in ("index", "data_stream") and not st.get("tune"):
                    self._phase(key, "tuning")
                    idx = self.cluster.write_index_of(job.dest, dest_kind)
                    orig = self.cluster.get_index_settings(idx)
                    self._record(key, tune={"index": idx, **orig}, flush=True)
                    self.cluster.put_index_settings(idx, {"number_of_replicas": 0, "refresh_interval": "-1"})
                    log.info("tuned %s for bulk load: replicas 0, refresh off (was replicas=%s, refresh=%s)",
                             idx, orig.get("number_of_replicas"), orig.get("refresh_interval") or "default",
                             extra={"event": "tune"})
                source_count = self.cluster.count(job.source)
                with self.lock:
                    self.views[key].source_count = source_count
                self._record(key, status="running", started=utcnow(), finished=None, error=None,
                             note=None, task=None, source_count=source_count, deleted_source=False)
                task_id = self.cluster.start_reindex(
                    job, slices=a.slices, rps=a.requests_per_second, conflicts=a.conflicts,
                    require_alias=a.require_alias, dest_kind=dest_kind, batch_size=a.batch_size)
                self._record(key, task=task_id, flush=True)
                log.info("started task %s for %d docs", task_id, source_count,
                         extra={"event": "start", "task": task_id, "counts": {"total": source_count}})
            self._phase(key, "reindexing", task=task_id)
            task = self._wait(job, task_id, log)
            counts = task_counts(task)
            self._phase(key, "verifying")
            self._record(key, status="verifying", **counts)
            reasons = self._verify(job, task, log)
            self._untune(key, log)
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
            self._untune(key, log)

        deleted = bool(st.get("deleted_source"))
        if a.delete_source and not deleted:
            reason = self._delete_guard(key)
            if reason:
                raise JobError(f"reindex verified but source kept: {reason}")
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

    def _delete_guard(self, key: str) -> str | None:
        """Why the source of ``key`` must not be deleted, or None when it may be.

        Preflight only checks this when --delete-source is on at start; the d key can turn
        it on later, so the delete step asks again using the plan row.
        """
        row = self.plan.get(key) or {}
        kind = row.get("src_kind")
        if kind in ("alias", "data_stream"):
            return f"source is a {kind.replace('_', ' ')}, not an index"
        owner = row.get("src_write_index_of")
        if owner:
            return f"source is the write index of data stream {owner!r}; roll it over first"
        return None

    def _untune(self, key: str, log: JobLog) -> None:
        """Restore replicas/refresh recorded by --tune-dest (also on resume)."""
        tune = self.state.job(key).get("tune")
        if not tune:
            return
        try:
            self.cluster.put_index_settings(tune["index"], {
                "number_of_replicas": tune.get("number_of_replicas"),
                "refresh_interval": tune.get("refresh_interval")})
            log.info("restored %s: replicas=%s, refresh=%s", tune["index"], tune.get("number_of_replicas"),
                     tune.get("refresh_interval") or "default", extra={"event": "untune"})
            self._record(key, tune=None, flush=True)
        except os_exc.OpenSearchException as e:
            log.error("could not restore settings on %s (replicas=%s refresh=%s): %s", tune["index"],
                      tune.get("number_of_replicas"), tune.get("refresh_interval"), describe(e),
                      extra={"event": "untune_failed"})

    def _wait(self, job: Job, task_id: str, log: JobLog) -> dict[str, Any]:
        """Follow a task through the shared poller until it leaves the task list."""
        with self.lock:
            self.task_jobs[task_id] = job.key
        with self.poll_cv:
            seen_gen = self.poll_gen
        grace_until: float | None = None
        try:
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
                with self.poll_cv:
                    self.poll_cv.wait_for(lambda: self.poll_gen > seen_gen or self.hard_stop.is_set(), timeout=1.0)
                    if self.poll_gen <= seen_gen:
                        continue
                    seen_gen = self.poll_gen
                    err = self.poll_error
                    listed = task_id in self.poll_tasks
                    counts = self.poll_tasks.get(task_id)
                if err is not None:
                    now = time.monotonic()
                    if not transient(err):
                        raise Unreachable(f"poll of tasks rejected: {describe(err)}")
                    if grace_until is None:
                        grace_until = now + self.args.poll_grace
                    if now > grace_until:
                        raise Unreachable(f"could not reach the cluster for {self.args.poll_grace:g}s: {describe(err)}")
                    log.warning("poll failed (%s); retrying for another %s",
                                describe(err), fmt_secs(grace_until - now), extra={"event": "poll_retry"})
                    continue
                grace_until = None
                if listed:
                    log.debug("poll: %s", counts, extra={"event": "poll", "task": task_id, "counts": counts})
                    continue
                # Not in the list: finished (result stored in .tasks), or not yet registered.
                try:
                    task = self.cluster.get_task(task_id)
                except os_exc.NotFoundError:
                    raise JobError(f"task {task_id} is gone from the cluster (node restart, or the .tasks "
                                   "index could not be written)")
                if not task.get("completed"):
                    continue
                with self.lock:
                    self.views[job.key].counts = task_counts(task)
                if "error" in task:
                    err_ = task["error"]
                    if isinstance(err_, dict):
                        inner = err_.get("caused_by") or {}
                        reason = f"{err_.get('type')}: {err_.get('reason')}"
                        if inner.get("reason"):
                            reason += f" (caused by {inner.get('type')}: {inner.get('reason')})"
                    else:
                        reason = str(err_)
                    raise JobError(f"task {task_id} ended with error: {reason}")
                return task
        finally:
            with self.lock:
                self.task_jobs.pop(task_id, None)

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
        processed_ = c["created"] + c["updated"] + c["noops"] + c["deleted"] + c["version_conflicts"]
        if processed_ != c["total"]:
            reasons.append(f"created+updated+noops+deleted+conflicts={processed_} != total={c['total']}")
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

    # ---- shared poller (one task listing per interval for every running job)
    def _poller(self) -> None:
        last_stats = 0.0
        while True:
            try:
                tasks: dict[str, dict[str, int]] | None = self.cluster.list_reindex_tasks()
                err: BaseException | None = None
            except Exception as e:  # noqa: BLE001 - reported to the waiting jobs
                tasks, err = None, e
            with self.poll_cv:
                if tasks is not None:
                    self.poll_tasks = tasks
                self.poll_error = err
                self.poll_gen += 1
                self.poll_cv.notify_all()
            if tasks is not None:
                with self.lock:
                    for tid, key in self.task_jobs.items():
                        if tid in tasks and key in self.views:
                            self.views[key].counts = tasks[tid]
            if not self.args.no_cluster_stats and time.monotonic() - last_stats >= 30:
                last_stats = time.monotonic()
                self._fetch_cluster_stats()
            if self.poll_stop.wait(self.args.poll_interval):
                return

    def _fetch_cluster_stats(self) -> None:
        try:
            health = self.cluster.health()
            pool = self.cluster.write_pool()
        except Exception as e:  # noqa: BLE001 - display only
            with self.lock:
                self.cluster_stats["error"] = describe(e)
            return
        rejected = sum(int(r.get("rejected") or 0) for r in pool)
        if self.rejected_base is None:
            self.rejected_base = rejected
        with self.lock:
            self.cluster_stats.update(
                status=health.get("status"), data_nodes=health.get("number_of_data_nodes"),
                pending=health.get("number_of_pending_tasks"),
                shards_pct=health.get("active_shards_percent_as_number"),
                write_active=sum(int(r.get("active") or 0) for r in pool),
                write_queue=sum(int(r.get("queue") or 0) for r in pool),
                write_rejected=rejected - self.rejected_base, nodes=len(pool),
                ts=time.monotonic(), error=None)

    # ---- keyboard (dashboard only)
    def _key_reader(self) -> None:
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            while not self.keys_stop.is_set():
                ready, _, _ = select.select([fd], [], [], 0.25)
                if ready:
                    ch = os.read(fd, 1).decode(errors="ignore")
                    if ch:
                        self.keys.put(ch)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)

    def _handle_keys(self) -> None:
        while True:
            try:
                ch = self.keys.get_nowait()
            except queue.Empty:
                return
            if self.dialog is not None:
                try:
                    if self.dialog.handle(ch):
                        self.dialog = None
                except Exception:  # noqa: BLE001 - a dialog bug must not take the run down
                    LOG.exception("dialog failed", extra={"event": "dialog_failed"})
                    self.dialog = None
                continue
            binding = self.key_bindings.get(ch)
            if binding:
                binding[1]()

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
            # _cat docs.count includes nested docs; once enough jobs finished, scale the
            # remaining estimates by the observed ratio of real count to _cat count.
            est = real = 0
            n = 0
            for j in self.jobs:
                if self.results.get(j.key) == "done":
                    sc = int(self.state.job(j.key).get("source_count") or 0)
                    pc = self.plan[j.key].get("count", 0)
                    if sc and pc:
                        est, real, n = est + pc, real + sc, n + 1
            ratio = (real / est) if n >= 3 and est else 1.0
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
                elif started and (live_total or (v and v.source_count)):
                    total += max(live_total, v.source_count if v else 0)
                else:
                    total += int(self.plan[j.key].get("count", 0) * ratio)
            return total

    def _window_rate(self, now: float, window: float) -> float:
        if not self.samples:
            return 0.0
        old = self.samples[0]
        for s in self.samples:
            if now - s[0] <= window:
                old = s
                break
        if old is self.samples[-1] and len(self.samples) > 1:
            old = self.samples[-2]
        dt = now - old[0]
        return (self.samples[-1][1] - old[1]) / dt if dt > 0 else 0.0

    def history(self, cols: int, window: float | None = None) -> list[float]:
        """docs/s per bucket for the last ``window`` seconds in ``cols`` buckets, oldest first."""
        bucket = max(1.0, (window or self.graph_window) / cols)
        if len(self.samples) < 2:
            return [0.0] * cols
        now = self.samples[-1][0]
        pts = list(self.samples)

        def docs_at(t: float) -> int | None:
            best = None
            for s in pts:
                if s[0] <= t:
                    best = s
                else:
                    break
            return best[1] if best else None

        out: list[float] = []
        for i in range(cols - 1, -1, -1):
            end = now - i * bucket
            a, b = docs_at(end - bucket), docs_at(end)
            out.append(max(0.0, (b - a) / bucket) if a is not None and b is not None else 0.0)
        return out

    def stats(self) -> dict[str, Any]:
        """One consistent set of progress numbers for the dashboard and the plain progress line."""
        now = time.monotonic()
        planned = self.planned_docs()
        done = self.docs_done()
        if planned:
            done = min(done, planned)
        if not self.samples or now - self.samples[-1][0] >= 0.5:
            self.samples.append((now, done))
        recent = self._window_rate(now, 60.0)
        with self.lock:
            starts = [v.started_at for v in self.views.values() if v.started_at]
        active_secs = now - min(starts) if starts else 0.0
        avg = done / active_secs if active_secs > 1 else 0.0
        warm = active_secs >= 300 or (planned > 0 and done / planned >= 0.05)
        eta_rate = avg if (warm and avg > 0) else recent
        if eta_rate > 0:
            if self.rate_smooth <= 0:
                self.rate_smooth = eta_rate
            else:
                dt = max(now - self.rate_ts, 0.0)
                alpha = 1 - math.exp(-dt / 60.0)   # one-minute time constant
                self.rate_smooth += alpha * (eta_rate - self.rate_smooth)
            self.rate_ts = now
        remaining = max(planned - done, 0)
        eta_s: float | None
        if planned and not remaining:
            eta_s = 0.0
        elif self.rate_smooth > 0 and remaining:
            eta_s = remaining / self.rate_smooth
        else:
            eta_s = None
        finish = (datetime.now(timezone.utc) + timedelta(seconds=eta_s)).astimezone(self.tz) if eta_s is not None else None
        return {"planned": planned, "done": done, "pct": (done / planned * 100) if planned else 0.0,
                "recent": recent, "avg": avg, "eta_s": eta_s, "finish": finish, "warm": warm,
                "elapsed": now - self.t0}

    # ---- rendering
    def _mode_text(self) -> Text:
        a = self.args
        if self.hard_stop.is_set():
            return Text("CANCELLING", style="bold red")
        if self.soft_stop.is_set():
            return Text("STOPPING", style="bold yellow")
        if self.paused:
            return Text("PAUSED", style="bold yellow")
        mode = Text("DELETE SOURCE", style="bold red") if a.delete_source else Text("keep sources", style="green")
        if a.tune_dest:
            mode.append(" · tune", style="cyan")
        return mode

    def render(self, ring: RingHandler) -> Layout | Group:
        a = self.args
        st = self.stats()
        t = self.tally()
        with self.lock:
            active = {k: v for k, v in self.views.items() if k not in self.results}
            cs = dict(self.cluster_stats)
        finished = sum(t.values())
        width, height = self.console.size.width, self.console.size.height
        eta = fmt_eta(st["eta_s"]) if st["eta_s"] is not None else ("warming up" if st["done"] == 0 else "--")
        finish = fmt_finish(st["finish"], self.tz) if st["finish"] else "--"

        # header
        hdr = Table.grid(expand=True)
        hdr.add_column(ratio=1, no_wrap=True, overflow="ellipsis")
        hdr.add_column(justify="right", no_wrap=True)
        left = Text.assemble((str(self.cluster_name), "bold"), f" @ {a.host}:{a.port}  ·  ",
                             (Path(str(a.list)).name, "bold"), f"  ·  {len(self.jobs)} jobs  ·  ", self._mode_text())
        right = Text.assemble(now_in(self.tz).strftime("%H:%M:%S %Z").strip(), ("   up ", "dim"),
                              fmt_secs(st["elapsed"]))
        hdr.add_row(left, right)
        header = Panel(hdr, border_style="cyan", padding=(0, 1))

        # progress box
        compact = height < 30
        prog = Table.grid(expand=True, padding=(0, 1))
        prog.add_column(no_wrap=True)
        prog.add_column(ratio=1)
        prog.add_column(justify="right", no_wrap=True)
        prog.add_row(Text(f"{st['pct']:5.1f}%", style="bold"),
                     ProgressBar(total=max(st["planned"], 1), completed=st["done"], width=None, **BAR_STYLE),
                     f"{st['done']:,} / {st['planned']:,} docs")
        line2 = Text.assemble(("now ", "dim"), f"{st['recent']:,.0f}/s   ", ("avg ", "dim"), f"{st['avg']:,.0f}/s   ",
                              ("ETA ", "dim"), (eta, "bold"), ("   finishes ", "dim"), (finish, "bold"),
                              no_wrap=True, overflow="ellipsis")
        graph_cols = max(20, int(width * 0.6) - 16)
        hist = self.history(graph_cols)
        rows = 1 if compact else 2
        graph = sparkline(hist, rows=rows)
        label = Text.assemble(("docs/s · last ", "dim"), fmt_secs(self.graph_window).rstrip("0s") or "0s",
                              ("   peak ", "dim"), f"{max(hist):,.0f}/s")
        counters = Text.assemble(
            ("indices ", "dim"), f"{finished}/{len(self.jobs)}  ", ("done ", "green"), f"{t['done']} ",
            ("fail ", "red"), f"{t['failed'] + t['lost']} ", ("held ", "yellow"), f"{t['held'] + t['cancelled']} ",
            ("skip ", "dim"), f"{t['skipped']} ", ("run ", "cyan"), f"{len(active)}",
            (f" del {self.deleted}" if a.delete_source else ""), ("  slots ", "dim"),
            f"{len(active)}/{self.max_active}", overflow="ellipsis", no_wrap=True)
        parts: list[Any] = [prog, line2] + ([] if compact else [Text("")]) + graph + [label, counters]
        progress = Panel(Group(*parts), title="Progress", title_align="left", border_style="blue", padding=(0, 1))

        # cluster box
        cl = Table.grid(padding=(0, 1), expand=True)
        cl.add_column(style="dim", no_wrap=True, min_width=10, max_width=10)
        cl.add_column(ratio=1, no_wrap=True, overflow="ellipsis")
        if a.no_cluster_stats:
            cl.add_row("stats", Text("off (--no-cluster-stats)", style="dim"))
        elif cs.get("error"):
            cl.add_row("stats", Text(str(cs["error"])[:60], style="red"))
        elif cs.get("status"):
            status = str(cs["status"]).upper()
            cl.add_row("health", Text.assemble((status, {"GREEN": "green", "YELLOW": "yellow"}.get(status, "red")),
                                                f"  nodes {cs.get('data_nodes', '?')}  pending {cs.get('pending', '?')}"))
            rej = int(cs.get("write_rejected") or 0)
            cl.add_row("write pool", Text.assemble(f"active {cs.get('write_active', 0)}  queue {cs.get('write_queue', 0)}  ",
                                                    ("rej ", "dim"), (f"+{rej}", "bold red" if rej else "green"),
                                                    (f"  {cs.get('nodes', 0)}n", "dim")))
            age = time.monotonic() - float(cs.get("ts") or time.monotonic())
            cl.add_row("shards", f"{cs.get('shards_pct', '?')}% active  {int(age)}s ago")
        else:
            cl.add_row("stats", Text("waiting for first sample", style="dim"))
        if not compact:
            cl.add_row("", "")
        cl.add_row("keys", Text.assemble(("q", "bold"), " stop ", ("Q", "bold"), " cancel ", ("-/+", "bold"),
                                          " slots ", ("p", "bold"), " pause"))
        cluster_panel = Panel(cl, title="Cluster", title_align="left", border_style="green", padding=(0, 1))

        # jobs box: as tall as the active jobs need (at least 3 rows, plus 4 for border and
        # header), the problems box only when there are problems, everything left to events.
        top_size = 7 if compact else 9
        avail = height - 3 - top_size - 1
        problems_rows = min(len(ring.problems), 6, avail - 16) if height >= 34 else 0
        show_problems = problems_rows > 0
        problems_size = problems_rows + 2 if show_problems else 0
        jobs_rows = max(1, min(max(3, len(active)), avail - problems_size - 11))
        events_size = max(3, avail - problems_size - jobs_rows - 4)
        jobs = Table(box=box.SIMPLE_HEAD, expand=True, show_edge=False, pad_edge=False)
        jobs.add_column("Source → Destination", ratio=3, min_width=24, no_wrap=True, overflow="ellipsis")
        if width >= 120:
            jobs.add_column("Task", no_wrap=True, style="dim")
        jobs.add_column("Phase", no_wrap=True)
        jobs.add_column("Docs", justify="right", no_wrap=True)
        jobs.add_column("Progress", ratio=2, min_width=10)
        jobs.add_column("Rate", justify="right", no_wrap=True)
        jobs.add_column("ETA", justify="right", no_wrap=True)
        jobs.add_column("Elapsed", justify="right", no_wrap=True)
        phase_style = {"reindexing": "cyan", "verifying": "yellow", "deleting": "bold red",
                       "re-attaching": "magenta", "starting": "magenta", "creating": "magenta", "tuning": "magenta"}
        rows_ = sorted(active.items(), key=lambda kv: kv[1].started_at or 1e18)
        shown_rows = rows_[:jobs_rows - 1] if len(rows_) > jobs_rows else rows_
        for key, v in shown_rows:
            c = v.counts or {}
            got = processed(c)
            tot = c.get("total") or v.source_count
            elapsed = time.monotonic() - v.started_at if v.started_at else 0
            rate = got / elapsed if elapsed > 1 else 0.0
            job_eta = fmt_eta((tot - got) / rate) if rate > 0 and tot > got else "-"
            cells: list[Any] = [Text(key.replace(":", " → ", 1), overflow="ellipsis", no_wrap=True)]
            if width >= 120:
                cells.append(Text((v.task or "-").rsplit(":", 1)[-1]))
            cells += [Text(v.phase, style=phase_style.get(v.phase, "")),
                      f"{got:,} / {tot:,}",
                      ProgressBar(total=max(tot, 1), completed=min(got, tot), width=None, **BAR_STYLE),
                      f"{rate:,.0f}/s" if rate else "-", job_eta,
                      fmt_secs(elapsed) if v.started_at else "-"]
            jobs.add_row(*cells)
        if len(rows_) > len(shown_rows):
            jobs.add_row(Text(f"+ {len(rows_) - len(shown_rows)} more running", style="dim"),
                         *[""] * (len(jobs.columns) - 1))
        if not active:
            jobs.add_row(Text("no active jobs" + (" (paused)" if self.paused else ""), style="dim"),
                         *[""] * (len(jobs.columns) - 1))
        jobs_panel = Panel(jobs, title=f"Active jobs ({len(active)})", title_align="left", border_style="blue",
                           padding=(0, 1))

        def events_table(rows__: list[tuple[float, int, str, str]], empty: str) -> Table:
            ev = Table.grid(padding=(0, 1), expand=True)
            ev.add_column(style="dim", no_wrap=True, min_width=8)
            ev.add_column(no_wrap=True, min_width=4)
            ev.add_column(style="magenta", no_wrap=True, min_width=6, max_width=32, overflow="ellipsis")
            ev.add_column(ratio=1, no_wrap=True, overflow="ellipsis")
            for ts, lvl, jobkey, msg in rows__:
                style = "red" if lvl >= logging.ERROR else ("yellow" if lvl >= logging.WARNING else "")
                ev.add_row(datetime.fromtimestamp(ts).strftime("%H:%M:%S"),
                           Text({logging.ERROR: "ERR", logging.WARNING: "WARN"}.get(lvl, "INFO"), style=style),
                           Text(jobkey.split(":", 1)[0]), Text(msg, style=style))
            if not rows__:
                ev.add_row("", "", "", Text(empty, style="dim"))
            return ev

        problems_panel = Panel(events_table(list(ring.problems)[-problems_rows:], "none so far"),
                               title=f"Problems ({len(ring.problems)} recent)", title_align="left",
                               border_style="red" if ring.problems else "dim", padding=(0, 1))
        events_panel = Panel(events_table(list(ring.buf)[-(events_size - 2):], "no events yet"),
                             title="Recent events", title_align="left", border_style="magenta", padding=(0, 1))
        if self.hard_stop.is_set():
            foot = "Cancelling running tasks; the run ends when they acknowledge."
        elif self.soft_stop.is_set():
            foot = "Stopping: running jobs finish, nothing new starts.  Q or Ctrl+C again cancels them."
        else:
            foot = None
        footer = Text(foot, style="dim") if foot else Text.assemble(self.key_help(), ("· Ctrl+C once/twice = q/Q", "dim"))

        if height < 24:
            return Group(header, jobs_panel, footer)
        layout = Layout()
        sections = [Layout(name="header", size=3), Layout(name="top", size=top_size),
                    Layout(name="jobs", size=jobs_rows + 4)]
        if show_problems:
            sections.append(Layout(name="problems", size=problems_size))
        sections += [Layout(name="events", ratio=1), Layout(name="footer", size=1)]
        layout.split_column(*sections)
        layout["top"].split_row(Layout(name="progress", ratio=3), Layout(name="cluster", ratio=2, minimum_size=36))
        layout["header"].update(header)
        layout["progress"].update(progress)
        layout["cluster"].update(cluster_panel)
        layout["jobs"].update(self.dialog.render(width, jobs_rows + 4) if self.dialog else jobs_panel)
        if show_problems:
            layout["problems"].update(problems_panel)
        layout["events"].update(events_panel)
        layout["footer"].update(footer)
        return layout

    # ---- orchestration
    def _dispatch(self, pool: ThreadPoolExecutor, pending: deque[Job], futures: dict[Future[str], Job]) -> None:
        if self.soft_stop.is_set():
            while pending:
                j = pending.popleft()
                LOG.info("not started: stop requested", extra={"job": j.key, "event": "held"})
                self._finish(j.key, "held")
            return
        if self.paused:
            return
        running = sum(1 for f in futures if not f.done())
        while pending and running < self.max_active:
            j = pending.popleft()
            futures[pool.submit(self.run_job, j)] = j
            running += 1

    def run(self, tui: bool, ring: RingHandler) -> int:
        runnable = [j for j in self.jobs if self.plan[j.key]["action"] not in ("skip", "error")]
        for j in self.jobs:
            if j not in runnable:
                with self.lock:
                    self.results[j.key] = "skipped"
                    self.views[j.key] = JobView(phase="skipped")
        pending: deque[Job] = deque(runnable)
        futures: dict[Future[str], Job] = {}
        poller = threading.Thread(target=self._poller, name="poller", daemon=True)
        poller.start()
        keys_thread: threading.Thread | None = None
        if tui and sys.stdin.isatty() and termios is not None:
            keys_thread = threading.Thread(target=self._key_reader, name="keys", daemon=True)
            keys_thread.start()

        def busy() -> bool:
            return bool(pending) or any(not f.done() for f in futures)

        try:
            with ThreadPoolExecutor(max_workers=self.POOL_CAP, thread_name_prefix="job") as pool:
                last_report = time.monotonic()
                live_ok = tui
                if tui:
                    try:
                        with Live(self.render(ring), console=self.console, screen=True, refresh_per_second=2,
                                  vertical_overflow="crop") as live:
                            while busy():
                                self._handle_keys()
                                self._dispatch(pool, pending, futures)
                                time.sleep(0.5)
                                live.update(self.render(ring))
                            live.update(self.render(ring))
                    except Exception:  # noqa: BLE001 - a dashboard bug must not take the run down
                        LOG.exception("dashboard failed; continuing without it", extra={"event": "tui_failed"})
                        live_ok = False
                if not live_ok:
                    while busy():
                        self._dispatch(pool, pending, futures)
                        time.sleep(0.5)
                        if time.monotonic() - last_report >= 30:
                            last_report = time.monotonic()
                            st = self.stats()
                            t = self.tally()
                            LOG.info("progress: %d/%d indices done, %d failed, %d running, %s/%s docs (%.1f%%), "
                                     "now %.0f docs/s, avg %.0f docs/s, ETA %s, finishes %s",
                                     t["done"], len(self.jobs), t["failed"] + t["lost"],
                                     sum(1 for f in futures if not f.done()), f"{st['done']:,}", f"{st['planned']:,}",
                                     st["pct"], st["recent"], st["avg"],
                                     fmt_eta(st["eta_s"]) if st["eta_s"] is not None else "--",
                                     fmt_finish(st["finish"], self.tz) if st["finish"] else "--",
                                     extra={"event": "progress", "counts": {"total": st["planned"]},
                                            "elapsed_s": round(st["elapsed"], 1),
                                            "eta_s": round(st["eta_s"], 1) if st["eta_s"] is not None else None,
                                            "finish_at": st["finish"].isoformat(timespec="minutes") if st["finish"] else None,
                                            "rate_recent": round(st["recent"]), "rate_avg": round(st["avg"])})
        finally:
            self.poll_stop.set()
            self.keys_stop.set()
            with self.poll_cv:
                self.poll_cv.notify_all()
            if keys_thread:
                keys_thread.join(timeout=2)
        for f, j in futures.items():
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
    try:
        health = cluster.health()
    except os_exc.OpenSearchException:
        health = {}
    data_nodes = int(health.get("number_of_data_nodes") or 1)
    args.cluster_health = health

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
                               "create": None, "template": None, "shards": None}
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
            row["src_kind"] = src_kind
            # Always recorded: the d key can turn on --delete-source later and the delete step re-checks it.
            row["src_write_index_of"] = next((n for n, d in snap.data_streams.items()
                                              if d["write_index"] == job.source), None)
            if src_kind == "index" and snap.indices[job.source].get("status") == "close":
                raise JobError("source index is closed")
            if args.delete_source and src_kind in ("alias", "data_stream"):
                raise JobError(f"source is a {src_kind.replace('_', ' ')}; refusing with --delete-source")
            if args.delete_source and src_kind == "index":
                owner = row["src_write_index_of"]
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
                src_row = snap.indices.get(job.source) or {}
                src_bytes = int(src_row.get("store.size") or 0)
                if kind == "index" and args.dest_shards:
                    if args.dest_shards == "auto":
                        if src_bytes >= 5 * 1024 ** 3:
                            row["shards"] = max(1, min(data_nodes, 8))
                    else:
                        row["shards"] = int(args.dest_shards)
                    if row["shards"]:
                        row["note"] += f", {row['shards']} primary shards"
                elif kind == "data_stream" and data_nodes > 1 and (tpl or {}).get("shards", 1) < data_nodes:
                    row["note"] += (f"; template gives {(tpl or {}).get('shards') or 1} primary, cluster has "
                                    f"{data_nodes} data nodes: set index.number_of_shards in the template")
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
                 cluster: Cluster, dashboards: Dashboards | None = None) -> Group:
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
                if p["create"] == "data_stream":
                    row(f"PUT /_data_stream/{j.dest}{tpl}", create_kind)
                elif p.get("shards"):
                    row(f"PUT /{j.dest}{tpl}\n{json.dumps({'settings': {'index.number_of_shards': p['shards']}})}",
                        create_kind)
                else:
                    row(f"PUT /{j.dest}{tpl}", create_kind)
                n_create += 1
            if args.tune_dest and p.get("dest_kind") in ("index", "data_stream"):
                idx = j.dest if p.get("dest_kind") == "index" else f"<write index of {j.dest}>"
                row(f"PUT /{idx}/_settings  {json.dumps({'index': {'number_of_replicas': 0, 'refresh_interval': '-1'}})}"
                    "  (--tune-dest, before the reindex)", write)
                row(f"PUT /{idx}/_settings  (restore the previous replicas and refresh_interval after verification)",
                    write)
            row(f"POST /{j.source}/_refresh  (exact doc count before starting)", harmless)
            path, body = cluster.reindex_request(j, slices=args.slices, rps=args.requests_per_second,
                                                 conflicts=args.conflicts, require_alias=args.require_alias,
                                                 dest_kind=p.get("dest_kind"), batch_size=args.batch_size)
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
    # Dashboards index patterns are refreshed only after a run that actually ran something.
    runnable = any(plan[j.key]["action"] not in ("skip", "error") for j in jobs)
    patterns = list(args.index_pattern or []) if dashboards is not None and runnable else []
    for i, pid in enumerate(patterns):
        t.add_row(Text("after the run") if i == 0 else "",
                  Text(f"PUT /api/saved_objects/index-pattern/{pid}  (refresh field list via GET _fields_for_wildcard)"),
                  write)
    lines = [t,
             Text.assemble("Conditional: ", ("POST /_tasks/<id>/_cancel", "yellow"),
                           " for each running task on a second Ctrl+C."),
             Text.assemble(f"Totals: {n_create} destination(s) to create, {n_writes} reindex request(s), ",
                           (f"{n_destructive} index delete(s)", "bold red" if n_destructive else ""),
                           f", {len(patterns)} index pattern(s) to refresh" if dashboards is not None else "", ".")]
    all_reads = cluster.reads + dashboards.reads if dashboards is not None else cluster.reads
    reads = ", ".join(f"{k} x{v}" for k, v in sorted(all_reads.items()))
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
dashboard keys: q stop, Q cancel, - / + lower or raise the number of concurrent jobs, p pause.
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
    r.add_argument("--max-total-rate", type=float, default=None, metavar="DOCS_PER_SEC",
                   help="cap for the whole run: each task is throttled to DOCS_PER_SEC / workers "
                        "(cannot be combined with --requests-per-second)")
    r.add_argument("--batch-size", type=int, default=1000,
                   help="documents per scroll batch (source.size); larger for small docs, smaller for multi-KB docs")
    r.add_argument("--tune-dest", action="store_true",
                   help="set replicas 0 and refresh_interval -1 on each destination during its reindex and "
                        "restore the previous values after verification")
    r.add_argument("--dest-shards", default=None, metavar="N|auto",
                   help="primary shards for plain indices the tool creates; auto = number of data nodes "
                        "(max 8, only for sources of 5 GB or more). Data streams take shards from their template")
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
    r.add_argument("--poll-interval", type=float, default=10,
                   help="seconds between task-list polls (one request per interval for all jobs)")
    r.add_argument("--no-cluster-stats", action="store_true",
                   help="do not fetch cluster health and write thread-pool stats every 30 s for the dashboard")
    r.add_argument("--poll-grace", type=float, default=300,
                   help="seconds to keep retrying an unreachable cluster before giving up on a job "
                        "(the job stays re-attachable)")

    d = p.add_argument_group("dashboards")
    d.add_argument("--dashboards-url", default=env_default("OSD_URL", None), metavar="URL",
                   help="OpenSearch Dashboards base URL, e.g. https://host:5601 [env OSD_URL]")
    d.add_argument("--index-pattern", action="append", default=None, metavar="ID",
                   help="id of an index-pattern saved object whose field list is refreshed after the run "
                        "(repeatable, or comma-separated); needs --dashboards-url")
    d.add_argument("--dashboards-user", default=None, metavar="USER",
                   help="basic-auth user for Dashboards (default: same as --user; '' for none)")
    d.add_argument("--dashboards-password-env", default=None, metavar="VAR",
                   help="name of the env var holding the Dashboards password (default: same as --password-env)")
    d.add_argument("--tenant", default=None,
                   help="securitytenant header for the security plugin's multi-tenancy "
                        "(global, private, or a tenant name; default: the user's default tenant)")

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
    s.add_argument("--timezone", default=env_default("REINDEX_TZ", DEFAULT_TZ), metavar="ZONE",
                   help="zone for the dashboard clock and finish times: an IANA name, 'utc' or 'local' "
                        "[env REINDEX_TZ]")
    a = p.parse_args(argv)
    try:
        a.port = int(a.port)
    except (TypeError, ValueError):
        p.error(f"--port / OS_PORT must be an integer, got {a.port!r}")
    if a.workers < 1:
        p.error("--workers must be >= 1")
    if a.poll_interval <= 0:
        p.error("--poll-interval must be > 0")
    if a.batch_size < 1:
        p.error("--batch-size must be >= 1")
    if a.max_total_rate is not None:
        if a.requests_per_second != -1:
            p.error("--max-total-rate and --requests-per-second are mutually exclusive")
        if a.max_total_rate <= 0:
            p.error("--max-total-rate must be > 0")
        a.requests_per_second = max(1.0, a.max_total_rate / a.workers)
    if a.dest_shards is not None and a.dest_shards != "auto":
        try:
            a.dest_shards = str(int(a.dest_shards))
            if int(a.dest_shards) < 1:
                raise ValueError
        except ValueError:
            p.error("--dest-shards must be a positive integer or 'auto'")
    try:
        a.timezone, a.tzinfo = resolve_tz(str(a.timezone))
    except (KeyError, ValueError):
        if str(a.timezone).strip() != DEFAULT_TZ:
            p.error(f"--timezone / REINDEX_TZ: unknown zone {a.timezone!r} (use an IANA name such as "
                    f"Europe/Berlin, 'utc' or 'local'; install the tzdata package if the system has no zone database)")
        print(f"warning: zone {DEFAULT_TZ} is not available on this system; using the local zone "
              f"(install the tzdata package or pass --timezone)", file=sys.stderr)
        a.timezone, a.tzinfo = "local", None
    a.cluster_health = {}
    a.index_pattern = [i.strip() for chunk in (a.index_pattern or []) for i in chunk.split(",") if i.strip()]
    if a.index_pattern and not a.dashboards_url:
        p.error("--index-pattern requires --dashboards-url (or OSD_URL)")
    if a.dashboards_url and not str(a.dashboards_url).startswith(("http://", "https://")):
        p.error("--dashboards-url must start with http:// or https://")
    if a.dashboards_user is None:
        a.dashboards_user = a.user
    if a.dashboards_password_env is None:
        a.dashboards_password_env = a.password_env
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


def setup_dashboards(args: argparse.Namespace, password: str | None, console: Console) -> Dashboards | None:
    """Client for --index-pattern, after a read-only check that every id resolves. None when unused.

    Raises DashboardsError for anything that would make the post-run refresh fail.
    """
    if not args.index_pattern:
        return None
    user = args.dashboards_user or None
    if user and user == args.user and args.dashboards_password_env == args.password_env:
        dpw = password
    elif user:
        dpw = os.environ.get(args.dashboards_password_env)
        if dpw is None:
            if not sys.stdin.isatty():
                raise DashboardsError(f"set ${args.dashboards_password_env} (or --dashboards-password-env) "
                                      "when not running interactively")
            dpw = getpass.getpass(f"Dashboards password for {user} (env {args.dashboards_password_env} is unset): ")
    else:
        dpw = None
    d = Dashboards(url=args.dashboards_url, user=user, password=dpw, verify_certs=args.verify_certs,
                   ca_cert=args.ca_cert, tenant=args.tenant, timeout=args.timeout, dry_run=args.dry_run)
    titles = []
    for pid in args.index_pattern:
        title = (d.get_index_pattern(pid).get("attributes") or {}).get("title") or "?"
        titles.append(f"{pid} ({title})")
    LOG.info("Dashboards %s: %d index pattern(s) to refresh after the run: %s", d.url, len(titles), ", ".join(titles))
    console.print(Text(f"Dashboards {d.url}" + (f" tenant {args.tenant}" if args.tenant else "")
                       + f": will refresh {len(titles)} index pattern(s) after the run: " + ", ".join(titles),
                       style="dim"))
    return d


def refresh_index_patterns(dashboards: Dashboards, ids: list[str], console: Console) -> int:
    """Post-run hook: refresh each pattern's field list. Failures are reported, never fatal."""
    failed = 0
    for pid in ids:
        try:
            title, n = dashboards.refresh(pid)
        except Exception as e:  # noqa: BLE001 - a Dashboards problem must not change the exit code
            failed += 1
            reason = describe(e)
            LOG.error("index pattern %s not refreshed: %s", pid, reason,
                      extra={"event": "index_pattern_failed", "reason": reason})
            console.print(Text(f"index pattern {pid} not refreshed: {reason}", style="yellow"))
        else:
            LOG.info("index pattern %s (%s) refreshed: %d fields", pid, title, n,
                     extra={"event": "index_pattern_refreshed", "counts": {"fields": n}})
            console.print(Text.assemble("index pattern ", (title, "bold"), f" ({pid}) refreshed: {n:,} fields"))
    return failed


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
        dashboards = setup_dashboards(args, password, console)
    except DashboardsError as e:
        err(f"Dashboards: {e}")
        LOG.error("Dashboards: %s", e, extra={"event": "index_pattern_failed", "reason": str(e)})
        return 2

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
                console.print(writes_table(jobs, plan, args, cluster, dashboards))
            return 2
        console.print(Text(f"{len(problems)} job(s) failed preflight and will be skipped.", style="yellow"))
    runnable = [j for j in jobs if plan[j.key]["action"] not in ("skip", "error")]
    if args.dry_run:
        console.print(writes_table(jobs, plan, args, cluster, dashboards))
        console.print(Text(f"dry run: {len(runnable)} job(s) would run; nothing was written.", style="yellow"))
        return 0
    if not runnable:
        console.print(Text("nothing to do: every job is already done or skipped.", style="green"))
        return 0
    docs = sum(plan[j.key]["count"] for j in runnable)
    if args.max_total_rate:
        console.print(Text(f"rate cap: {args.max_total_rate:,.0f} docs/s total = {args.requests_per_second:,.0f} "
                           f"docs/s per task across {args.workers} worker(s)", style="dim"))
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
        h = args.cluster_health or {}
        seed = {"status": h.get("status"), "data_nodes": h.get("number_of_data_nodes"),
                "pending": h.get("number_of_pending_tasks"), "shards_pct": h.get("active_shards_percent_as_number"),
                "write_active": 0, "write_queue": 0, "write_rejected": 0, "nodes": 0,
                "ts": time.monotonic()} if h.get("status") else {}
        runner = Runner(args, cluster, state, jobs, plan, cluster_name, console, cluster_stats=seed)
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
    # post-run hooks (e.g. refreshing Dashboards index patterns) go here
    if dashboards is not None:
        refresh_index_patterns(dashboards, args.index_pattern, console)
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
