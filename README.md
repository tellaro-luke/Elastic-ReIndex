# Elastic-Reindex

**Bulk reindexing for OpenSearch, driven by a plain `source:destination` list.**

Give it a list of indices and it runs each reindex as a background task on the cluster,
polls it, verifies the result, and (only if you ask) deletes the source. Progress is
checkpointed to a state file, so an interrupted run picks up where it left off and
re-attaches to tasks that are still running on the cluster.

The name is historic: version 1 targeted Elasticsearch 7.x. Version 2 is written and tested
against OpenSearch 3.x; the APIs it uses (`_reindex`, `_tasks`, `_count`, `_cat/indices`,
`_alias`) are the same on OpenSearch 1.x and 2.x.

**Use at your own risk.** Run with `--dry-run` first, and keep `--delete-source` off until you
trust the results on your own data.

## Features

* Any number of `source:destination` pairs; destinations can be indices or aliases
* Read-only dry run that prints the plan and every write request a real run would send
* Checkpoint file: done jobs are skipped, running tasks are re-attached, failed jobs can be retried
* Verification before any delete: no failures, not cancelled, all docs accounted for, task total matches the source doc count, and (for index destinations) destination count is at least the source count
* `--workers N` to run several indices at once; each reindex is also sliced by OpenSearch (`slices=auto`)
* Live dashboard on the console: progress by documents, per-job bars, rate, ETA, recent problems
* Text log and JSON-lines log for tracing specific issues
* Two-stage stop: Ctrl+C (or SIGTERM) once to stop launching new jobs, twice to cancel the running tasks
* Preflight that checks every source and destination, and the user's task permissions, before anything starts
* Refuses wildcard or multi-index names, so a delete can never hit more than the one index listed
* No credentials in the code: the password comes from an environment variable or an interactive prompt

## Install

Requires Python 3.10+ and Poetry 2.x.

```bash
poetry install
```

Without Poetry: `pip install "opensearch-py>=2.6,<4" "rich>=13,<15"` and use `python reindex.py`
in place of `poetry run reindex`.

Run from the project directory. `poetry run` needs `pyproject.toml`, and `poetry -C <dir> run`
changes into `<dir>`, so the default relative paths (`list.txt`, `reindex-state.json`, the logs)
resolve there. Pass absolute paths to `--list`, `--state-file`, `--log-file` and `--json-log`
if you keep them elsewhere.

## Permissions

With the security plugin the user needs, at minimum:

* Cluster: `cluster:monitor/main`, `cluster:monitor/task/get`, `cluster:monitor/tasks/lists`,
  `cluster:monitor/state` (for `_cat/indices` and `_alias`), and `cluster:admin/tasks/cancel`
  (only used by the second Ctrl+C).
* Sources: `indices:admin/get`, `indices:admin/aliases/get`, `indices:admin/refresh`,
  `indices:data/read/*`, `indices:data/write/reindex`; plus `indices:admin/delete` with `--delete-source`.
* Destinations: `indices:admin/get`, `indices:admin/aliases/get`, `indices:admin/refresh`,
  `indices:data/read/search` (the verification count) and `indices:data/write/*`.

`admin` has all of these. Preflight checks the task-listing permission up front and refuses to
start if it is missing, because the poll loop cannot work without it.

## Usage

```bash
export OS_PASSWORD='your password'      # PowerShell: $env:OS_PASSWORD='your password'

# 1. Read-only: check the list and the cluster, print the plan and the writes a real run would send
poetry run reindex --host os-client.example --user admin --dry-run

# 2. Reindex, two indices at a time, keep the sources
poetry run reindex --host os-client.example --user admin --workers 2

# 3. Same, but delete each source once its reindex passes verification
poetry run reindex --host os-client.example --user admin --workers 2 --delete-source --yes

# 4. Interrupted? Run the same command again. Done jobs are skipped, running tasks re-attached.
poetry run reindex --host os-client.example --user admin --workers 2 --delete-source --yes

# 5. After fixing whatever made some jobs fail
poetry run reindex --host os-client.example --user admin --retry-failed

# 6. Cron or a log file: the dashboard switches itself off when stdout is not a terminal.
#    --no-tui forces plain lines on a real terminal too (tmux/screen logging).
#    Non-interactive runs need OS_PASSWORD set and --yes.
poetry run reindex --host os-client.example --user admin --yes --no-tui | tee run.out
```

Default is `https://` with certificate verification **off** and the warning silenced. Use
`--verify-certs` (with `--ca-cert` for a private CA) outside a lab.

`poetry run reindex --help` lists every option with its default. Grouped as in the help text:

| Option | Default | Purpose |
|---|---|---|
| `--host`, `--port`, `--user` | `localhost`, `9200`, `admin` | Connection. Env: `OS_HOST`, `OS_PORT`, `OS_USER`. `--user ''` disables authentication |
| `--password-env VAR` | `OS_PASSWORD` | Env var holding the password. Prompts if unset on a terminal, exits 2 otherwise |
| `--verify-certs` / `--no-verify-certs`, `--ca-cert PATH` | off | TLS verification. Env: `OS_VERIFY_CERTS=1`, `OS_CA_CERT` |
| `--no-ssl` | off | Plain HTTP |
| `--timeout` | `30` | Per-request timeout in seconds |
| `--list FILE` | `list.txt` | The `source:destination` list |
| `--workers N` | `1` | Concurrent indices |
| `--slices` | `auto` | Passed to `_reindex` |
| `--requests-per-second` | `-1` (unlimited) | Throttle passed to `_reindex`; effectively documents per second per task |
| `--conflicts` | `abort` | `proceed` tolerates version conflicts (only reachable with external versioning) |
| `--require-alias` | off | Fail in preflight if a destination is not an alias |
| `--delete-source` | off | Delete a source after its job verifies. Refuses alias sources |
| `--skip-missing` | off | Skip jobs that fail preflight instead of aborting |
| `--poll-interval`, `--poll-grace` | `5`, `300` | Seconds between task polls; seconds to tolerate an unreachable cluster before giving up on a job (it stays re-attachable) |
| `--dry-run` | off | Read-only preflight, plan, and the list of writes a real run would send |
| `-y`, `--yes` | off | No confirmation prompt |
| `--state-file`, `--reset-state`, `--retry-failed` | `reindex-state.json` | Checkpoint control |
| `--log-file`, `--json-log` | `reindex.log`, `reindex.jsonl` | Log files. Pass `''` to disable |
| `--no-tui`, `--debug` | off | Plain output; log every poll and HTTP request to the log files |

### The list file

One `source:destination` per line. Blank lines and `#` comments are ignored. Whitespace
around names is stripped. A source may appear only once. Names must be single, concrete,
lowercase index or alias names: patterns (`logs-*`), lists (`a,b`), `_all` and date math are
rejected so that a delete can never touch more than the one index you listed.

```
# rollover targets
logstash-example1-source-00345:logstash-example-destination-alias
logstash-example1-source-00346:logstash-example-destination-alias

# daily indices
logstash-example2-source-2022-08-22:logstash-example-destination-2022-08-22
logstash-example2-source-2022-08-23:logstash-example-destination-2022-08-23
```

Destinations must exist before the run. Create them with the mappings and settings you
want; the tool does not copy settings from the source. An alias destination must resolve
to a single index or have exactly one write index. For example:

```
PUT logstash-example-destination-2022-08-22
{ "settings": { "number_of_shards": 3, "number_of_replicas": 0, "refresh_interval": "-1" },
  "mappings": { "properties": { "@timestamp": { "type": "date" } } } }

PUT logstash-example-destination-000001
POST _aliases
{ "actions": [ { "add": { "index": "logstash-example-destination-000001",
                          "alias": "logstash-example-destination-alias", "is_write_index": true } } ] }
```

The tool refreshes a destination before counting it, so `refresh_interval: -1` does not
break verification.

**Sources must not receive writes during the run.** Verification compares the task's total
with the source document count taken when the job started; an index that is still being
written to (today's Logstash index) will fail verification and keep its source.

### Dry run

`--dry-run` sends only read requests: cluster info, one `_cat/indices` and one `_alias`
listing, and a task-listing probe for permissions. The cluster wrapper refuses every write in
this mode, so it cannot start, cancel, refresh or delete anything even by accident. It prints:

* the plan table (what each job would do: reindex, re-attach, finish, skip, or the preflight error);
* a table of the write requests a real run would send, per job and in order: the source
  refresh for the starting count, the full `POST /_reindex` with its query string and body,
  the destination refresh for verification, and the `DELETE` of the source in red when
  `--delete-source` is set;
* the conditional cancel request a second Ctrl+C would send, totals, and the read-only
  requests the dry run itself sent.

The state file is never written or deleted by a dry run; `--reset-state --dry-run` only
reports that it would delete it.

### The state file

`reindex-state.json` records every job as `pending`, `running`, `verifying`, `verified`,
`done`, or `failed`, with the task id, document counts, timestamps, and the error for failed
jobs. It is written atomically (temp file, fsync, rename) at most once a second and
immediately after a task id is recorded. A lock file next to it stops a second process from
using the same state file; run several batches with separate `--list`, `--state-file`,
`--log-file` and `--json-log`. Note `.gitignore` only covers the default names.

On the next run:

* `done` jobs are skipped.
* `running` or `verifying` jobs with a task id are re-attached if the task still exists on
  the cluster, otherwise re-run. Because `_reindex?wait_for_completion=false` stores its result
  in the `.tasks` index, a task that finished while the tool was down is picked up and verified
  rather than re-run. Reindexing overwrites by `_id`, so a re-run into the same index is safe.
* `verified` jobs (verification passed, delete not yet done) go straight to the delete when
  `--delete-source` is set, or are marked done without it.
* `failed` jobs whose task turns out to still be running are re-attached automatically.
  Other failed jobs are skipped unless you pass `--retry-failed`.
* A job cancelled by the second Ctrl+C goes back to `pending` and restarts from scratch. A
  task cancelled on the cluster by someone else fails verification and becomes `failed`.
* A job that lost contact with its task (cluster unreachable for longer than `--poll-grace`)
  is reported as `lost`, keeps `running` status and its task id, and re-attaches next run.

Before starting a job the tool also looks for a running reindex task with the same source
and destination and adopts it instead of starting a second one. This covers a crash between
starting a task and recording its id, and a dropped connection on the start request.

State is keyed by `source:destination`. Removing a line from the list leaves its entry in the
state file; changing a destination creates a new key. `--reset-state` deletes the file after
a confirmation (skipped with `--yes`; refused with exit 2 when stdin is not a terminal and
`--yes` is absent). Do not delete a destination and re-run against a stale state file that
says the job is done: done jobs are skipped without looking at the destination.

### What "verified" means

A job is marked `verified`, and the source becomes eligible for deletion, only when all of these hold:

1. The task completed with an empty `failures` list, was not cancelled, and did not time out
2. `created + updated + noops + deleted + version_conflicts == total`
3. `version_conflicts == 0` unless `--conflicts proceed`
4. `total` equals the source document count taken when the job started
5. For a plain index destination, the destination can be counted and now holds at least as
   many documents as the source (skipped for alias destinations, which collect many sources)

Anything else marks the job `failed`, leaves the source alone, and records the reason
(failures are summarised by type with the first document and message). If the delete itself
fails after verification, the job stays `verified` and the next run retries only the delete.

### Logs

* `reindex.log`: timestamped text, rotated at 20 MB with 5 backups. Every line is tagged
  `[source:destination]`, or `[-]` for run-level messages, so `grep 'src:dest' reindex.log`
  shows one job end to end.
* `reindex.jsonl`: one JSON object per log record, rotated at 50 MB with 5 backups. Fields:
  `ts` (UTC), `level`, `logger`, `job` (`null` for run-level lines), `message`, and when
  present `event`, `task`, `counts` (`total`, `created`, `updated`, `deleted`, `noops`,
  `version_conflicts`, `batches`), `elapsed_s`, `reason`, `exception`. Events: `run_start`,
  `start`, `adopt`, `reattach`, `reattach_failed`, `done`, `verify_failed`, `failed`, `lost`,
  `delete`, `cancelled`, `held`, `poll_retry`, `child_list_failed`, `soft_stop`, `hard_stop`,
  `force_exit`, `state_write_failed`, `crashed`, `run_end`, plus `poll` with `--debug` and
  `progress` in plain mode.

  ```bash
  jq 'select(.job=="src:dest")' reindex.jsonl
  jq -c 'select(.event=="verify_failed" or .event=="failed") | {ts,job,reason}' reindex.jsonl
  ```

  The file can be bulk-loaded straight into OpenSearch. With `--debug` it also receives every
  HTTP request from opensearch-py and every task poll, and grows quickly.

While jobs run, the console shows the live dashboard: a header with progress by documents,
rate, ETA and counters; one row per active job; a "Problems" panel with the last warnings and
errors; and the last few events. The full stream is in the files. The plan table is printed
before the run and a summary table after it. When stdout is not a terminal (cron, `| tee`),
or with `--no-tui`, events are printed as plain lines instead, plus a progress line every 30 s.

With `slices=auto` the parent task's counters stay at 0 until slices finish; the dashboard
reads progress from the child tasks (`GET _tasks?parent_task_id=<id>&detailed=true` shows the
same), while `GET _tasks/<id>` on the parent will show 0 until the end.

## Performance notes

* For big batches, set `number_of_replicas: 0` and `refresh_interval: -1` on the
  destinations for the duration, then restore them.
* `--slices auto` parallelises each reindex across the source's shards. Use `--workers` to
  overlap several indices as well; watch cluster CPU and the write thread pool.
* `--requests-per-second` throttles if the cluster is serving traffic at the same time. Despite
  the name it behaves as documents per second per task: each batch of 1000 docs waits `1000 / N`
  seconds, so `--requests-per-second 5000` is a mild throttle and `2` would take hours per
  index. The throttle is split across a task's slices, and `--workers N` multiplies the
  cluster-wide rate by N.
* Preflight makes three cluster calls regardless of list length (`_cat/indices`, `_alias`, a
  task-listing probe); plan and summary tables show at most 80 rows and hide already-done
  rows when the list is longer.
* Set `index.mapping.ignore_malformed: true` on the destination if the source has messy
  field types. See <https://github.com/elastic/elasticsearch/issues/22471>.

## Stopping

Ctrl+C or SIGTERM once: no new jobs start, running jobs finish and verify. Twice: running
tasks are cancelled on the cluster and their jobs go back to `pending`; a job that had
already verified keeps that status so a re-run only repeats the delete. A third signal exits
immediately after flushing the state file. Under systemd, SIGTERM is the first stage, so set
`TimeoutStopSec` long enough for running tasks to finish or accept that they are killed and
re-attached on the next run.

## Exit codes

* `0` every job done or skipped (also for `--dry-run` and "nothing to do")
* `1` at least one job failed verification, errored, or lost contact with its task
* `2` configuration error: bad list file, cannot connect, preflight failure without
  `--skip-missing`, password not set in a non-interactive run, confirmation declined, state
  file in use by another process
* `130` an interrupt was received and at least one job was held or cancelled because of it
