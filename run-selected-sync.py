#!/usr/bin/python3
"""Selective per-channel sync scheduler for discrawl-me.

This is an operational scheduler, not a crawler: every byte of Discord data is
still fetched by the existing native binary. The scheduler only decides *which*
native command runs, for how long, and in which order, so that the long backfill
of the two newly approved guilds cannot starve the existing EasyCop collection.

Per cycle (launchd fires the wrapper every 600 s):

  1. take an exclusive, non-blocking flock for the whole supervisor + child
     lifetime; a second instance exits harmlessly,
  2. skip the whole cycle if an external native sync for the same config is
     already running (detected, never killed),
  3. validate the approved manifest strictly before building any command,
  4. run the unchanged EasyCop command first, with no time limit,
  5. spend at most BUDGET seconds on the selected channels, one native child at
     a time, each capped at CHILD seconds or whatever is left of the budget,
     advancing a durable round-robin cursor after every reaped child.

Stdlib only, written for the system interpreter /usr/bin/python3 (3.9 on this
machine): no third-party packages, no TOML parsing, no network access and no
Discord client of its own.

Status caveat: a child exiting 0 means the native process finished that slice
without erroring. It does NOT mean the channel history is complete — only the
native page checkpoints in the database can establish that.
"""

import argparse
import collections
import errno
import fcntl
import hashlib
import json
import os
import shlex
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone

# --------------------------------------------------------------------------
# Fixed production configuration
# --------------------------------------------------------------------------

def _resolve_home():
    """Where the discrawl-me installation lives.

    This used to be the literal "/Users/ivansposato", which was honest while
    there was one machine. It stops being honest the moment the archiver also
    runs somewhere else: every path below would still point at a home
    directory that does not exist there, and the failure is quiet — a missing
    manifest reads as "no selected work this cycle", not as a misconfiguration.

    DISCRAWL_HOME wins when set, so a scheduler can be pointed at a specific
    installation without touching this file. Otherwise the user's own home is
    used, which is what the hard-coded value meant on the machine it was
    written for. Nothing changes there; it just stops being the only answer.
    """
    override = os.environ.get("DISCRAWL_HOME", "").strip()
    if override:
        return override.rstrip("/")
    return os.path.expanduser("~").rstrip("/")


_HOME = _resolve_home()

Paths = collections.namedtuple(
    "Paths", "binary config manifest state lock log child_log_dir")

DEFAULT_PATHS = Paths(
    binary=_HOME + "/GitHub/tools/discrawl-me/discrawl-me",
    config=_HOME + "/.config/discrawl-me/config.toml",
    manifest=_HOME + "/.config/discrawl-me/selected-guilds.json",
    state=_HOME + "/.local/share/discrawl-me/selected-sync-state.json",
    lock=_HOME + "/.local/share/discrawl-me/selected-sync.lock",
    log=_HOME + "/.local/share/discrawl-me/logs/selected-sync.jsonl",
    child_log_dir=_HOME + "/.local/share/discrawl-me/logs/selected-children",
)

# Existing collection: unchanged id, unchanged command, unchanged (absent) limit.
EASYCOP_GUILD_ID = "422855027535642625"

# The two guilds Ivan approved, and the exact approved channel counts.
SNEAKER_DEVELOPMENT_GUILD_ID = "539258061081018378"
DUSTIFY_GUILD_ID = "1347333602794541066"
EXPECTED_GUILDS = (SNEAKER_DEVELOPMENT_GUILD_ID, DUSTIFY_GUILD_ID)
EXPECTED_COUNTS = {SNEAKER_DEVELOPMENT_GUILD_ID: 35, DUSTIFY_GUILD_ID: 108}

DEFAULT_BUDGET_SECONDS = 180.0
DEFAULT_CHILD_SECONDS = 45.0
DEFAULT_GRACE_SECONDS = 5.0
MIN_SLICE_SECONDS = 1.0
CHILD_LOG_KEEP = 500

PGREP_BIN = "/usr/bin/pgrep"
PS_BIN = "/bin/ps"

EXIT_OK = 0
EXIT_EASYCOP_FAILED = 1
EXIT_MANIFEST_INVALID = 2
EXIT_EXTERNAL_CHECK_FAILED = 3
EXIT_INTERRUPTED = 130
EXIT_SIGNALLED = 143

STATUS_NOT_STARTED = "not_started"
STATUS_EXIT_ZERO = "child_exit_zero"
STATUS_EXIT_NONZERO = "child_exit_nonzero"
STATUS_TIMEOUT = "child_timeout_interrupted"
STATUS_SIGNAL_INTERRUPTED = "child_signal_interrupted"
STATUS_SPAWN_FAILED = "child_spawn_failed"

STATUS_SEMANTICS = (
    "child_exit_zero means the native process exited 0 for that slice; it is "
    "NOT history_complete. Only the native per-page checkpoints in the "
    "database establish whether a channel history is complete. "
    "child_timeout_interrupted and child_signal_interrupted mean the slice was "
    "explicitly cut short, never that it finished."
)

STATE_VERSION = 1

Target = collections.namedtuple(
    "Target", "guild_id guild_name channel_id channel_name channel_type")

_ASCII_DIGITS = frozenset("0123456789")


class ManifestError(Exception):
    """The approved manifest is missing, unreadable or not exactly as approved."""


class ExternalCheckError(Exception):
    """The external-sync probe could not be completed."""


class _Interrupted(BaseException):
    """Raised from the SIGTERM handler so the child can be reaped first."""

    def __init__(self, signame):
        BaseException.__init__(self, signame)
        self.signame = signame


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------


def _utcnow():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _is_snowflake(value):
    return (
        isinstance(value, str)
        and 15 <= len(value) <= 22
        and set(value) <= _ASCII_DIGITS
        and not value.startswith("0")
    )


def _ere_escape(text):
    out = []
    for ch in text:
        if ch in ".^$*+?()[]{}|\\":
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


# --------------------------------------------------------------------------
# Manifest: load, validate strictly, flatten
# --------------------------------------------------------------------------


def load_manifest(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (IOError, OSError) as exc:
        raise ManifestError("cannot read manifest %s: %s" % (path, exc))
    except ValueError as exc:
        raise ManifestError("manifest %s is not valid JSON: %s" % (path, exc))


def validate_manifest(doc, expected_guilds=EXPECTED_GUILDS,
                      expected_counts=EXPECTED_COUNTS,
                      easycop_guild_id=EASYCOP_GUILD_ID):
    """Fail closed. Anything not exactly as approved raises ManifestError.

    expected_guilds / expected_counts are parameters so the offline tests can
    drive small synthetic manifests. The production entry point never passes
    them, so there is no way to widen the selection from the command line.
    """
    if not isinstance(doc, dict):
        raise ManifestError("manifest root must be an object")

    if doc.get("existing_guild_unfiltered") != easycop_guild_id:
        raise ManifestError(
            "existing_guild_unfiltered must be %s, got %r"
            % (easycop_guild_id, doc.get("existing_guild_unfiltered")))

    gateway = doc.get("gateway")
    if not (gateway is False or gateway == "disabled"):
        raise ManifestError("gateway must be disabled, got %r" % (gateway,))

    if doc.get("include_dms") is not False:
        raise ManifestError("include_dms must be false, got %r"
                            % (doc.get("include_dms"),))

    guilds = doc.get("new_guilds")
    if not isinstance(guilds, list) or not guilds:
        raise ManifestError("new_guilds must be a non-empty list")

    seen_guilds = []
    for entry in guilds:
        if not isinstance(entry, dict):
            raise ManifestError("each new_guilds entry must be an object")
        gid = entry.get("guild_id")
        if not _is_snowflake(gid):
            raise ManifestError("invalid guild_id %r" % (gid,))
        if gid == easycop_guild_id:
            raise ManifestError(
                "guild %s is the existing unfiltered collection and must not be "
                "a selected guild" % gid)
        if gid in seen_guilds:
            raise ManifestError("duplicate guild_id %s" % gid)
        seen_guilds.append(gid)

    if sorted(seen_guilds) != sorted(expected_guilds):
        raise ManifestError(
            "unexpected guild set: got %s, approved %s"
            % (sorted(seen_guilds), sorted(expected_guilds)))

    global_ids = set()
    for entry in guilds:
        gid = entry.get("guild_id")
        channels = entry.get("channels")
        if not isinstance(channels, list) or not channels:
            raise ManifestError(
                "guild %s has no channels; an empty channel list would mean an "
                "unrestricted guild sync and is never allowed" % gid)
        per_guild = set()
        for channel in channels:
            if not isinstance(channel, dict):
                raise ManifestError("guild %s has a non-object channel" % gid)
            cid = channel.get("id")
            if not _is_snowflake(cid):
                raise ManifestError("guild %s has an invalid channel id %r"
                                    % (gid, cid))
            if cid in per_guild:
                raise ManifestError("guild %s has duplicate channel id %s"
                                    % (gid, cid))
            if cid in global_ids:
                raise ManifestError("duplicate channel id %s across guilds" % cid)
            per_guild.add(cid)
            global_ids.add(cid)
        expected = expected_counts.get(gid)
        if expected is None:
            raise ManifestError("no approved channel count for guild %s" % gid)
        if len(channels) != expected:
            raise ManifestError(
                "guild %s channel count is %d, approved count is %d"
                % (gid, len(channels), expected))
    return True


def flatten_targets(doc):
    """Flatten to a stable rotation order.

    Manifest order is preserved inside each guild (Sneaker Development first),
    but the guilds are interleaved round-robin so a cycle never spends its whole
    budget on the first guild and starves the second.
    """
    per_guild = []
    for entry in doc.get("new_guilds", []):
        gid = entry.get("guild_id")
        gname = entry.get("guild", "")
        per_guild.append([
            Target(gid, gname, channel.get("id"), channel.get("name", ""),
                   channel.get("type", ""))
            for channel in entry.get("channels", [])
        ])
    targets = []
    depth = max([len(lst) for lst in per_guild] or [0])
    for index in range(depth):
        for lst in per_guild:
            if index < len(lst):
                targets.append(lst[index])
    return targets


def targets_fingerprint(targets):
    digest = hashlib.sha256()
    for target in targets:
        digest.update(("%s/%s\n" % (target.guild_id, target.channel_id)).encode())
    return digest.hexdigest()


# --------------------------------------------------------------------------
# Native commands
# --------------------------------------------------------------------------


def easycop_command(paths, guild_id=EASYCOP_GUILD_ID):
    """The pre-existing command, byte for byte: unfiltered guild, no extra flags."""
    return [paths.binary, "--config", paths.config, "sync", "--guild", guild_id]


def selected_command(paths, target):
    """One native invocation for exactly one approved channel.

    --full because the incremental path has no durable per-page cursor, so an
    interrupted incremental pass would restart from scratch. An empty --channels
    value would mean "the whole guild": it is rejected here as well as in the
    manifest validator.
    """
    if not _is_snowflake(target.channel_id):
        raise ManifestError(
            "refusing to build a sync command for channel id %r: an empty or "
            "malformed channel list would sync the entire guild"
            % (target.channel_id,))
    if not _is_snowflake(target.guild_id):
        raise ManifestError("refusing to build a sync command for guild id %r"
                            % (target.guild_id,))
    return [
        paths.binary,
        "--config", paths.config,
        "sync",
        "--guild", target.guild_id,
        "--channels", target.channel_id,
        "--full",
        "--concurrency", "1",
        "--include-dms=false",
    ]


# --------------------------------------------------------------------------
# External native sync detection (detect, never kill)
# --------------------------------------------------------------------------


def external_sync_pattern(binary, config):
    """Loose pgrep pre-filter: candidate pids only, never the decision itself.

    Anything this matches is re-checked against the real argv shape by
    argv_is_external_sync, so the pattern only has to be wide enough not to miss
    a native invocation (config is deliberately absent from it, since it may be
    spelled --config PATH or --config=PATH).
    """
    return "(^|/)%s( |$)" % _ere_escape(os.path.basename(binary))


def same_config(candidate, config):
    """True when two config arguments name the same effective file."""
    if not isinstance(candidate, str) or not candidate:
        return False

    def normalise(value):
        return os.path.normcase(os.path.realpath(
            os.path.abspath(os.path.expanduser(value))))

    return normalise(candidate) == normalise(config)


def argv_is_external_sync(argv, binary, config):
    """Decide from a real argv whether this is a native sync for our config.

    The native CLI parses its global options with Go's flag package
    (internal/cli/cli.go): -config/--config is the only global option that takes
    a value, every other global option is boolean and consumes nothing, and
    parsing stops at the first non-flag argument, which is the subcommand. This
    mirrors that grammar, so all of

        discrawl-me --config PATH sync ...
        discrawl-me --config=PATH sync ...
        discrawl-me -config PATH sync ...
        discrawl-me --quiet --config PATH sync ...

    are recognised, while a different config, another subcommand, or the same
    text merely embedded in an unrelated command's arguments are not.
    """
    if not argv:
        return False
    program = argv[0]
    if os.path.basename(program) != os.path.basename(binary):
        return False

    config_value = None
    index = 1
    while index < len(argv):
        token = argv[index]
        if token == "--":
            index += 1
            break
        if not token.startswith("-"):
            break
        name = token.lstrip("-")
        if name.startswith("config="):
            config_value = name.split("=", 1)[1]
        elif name == "config":
            index += 1
            if index >= len(argv):
                return False
            config_value = argv[index]
        # every other global option of the native CLI is boolean: it never
        # consumes the following argument.
        index += 1

    if index >= len(argv) or argv[index] != "sync":
        return False
    return same_config(config_value, config)


def _read_argv_by_pid(pids, runner=None):
    """Return {pid: argv list} for the given pids, via ps. Argv only, no env."""
    if not pids:
        return {}
    runner = runner or subprocess.run
    command = [PS_BIN, "-o", "pid=,args="]
    for pid in pids:
        command += ["-p", str(pid)]
    try:
        proc = runner(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                      universal_newlines=True)
    except OSError as exc:
        raise ExternalCheckError("cannot run %s: %s" % (PS_BIN, exc))
    # ps exits 1 when none of the requested pids exist any more, which is a
    # normal race, not a failure.
    if proc.returncode not in (0, 1):
        raise ExternalCheckError("%s exited %s" % (PS_BIN, proc.returncode))
    found = {}
    for line in (proc.stdout or "").splitlines():
        line = line.strip()
        if not line:
            continue
        head, _, rest = line.partition(" ")
        try:
            pid = int(head)
        except ValueError:
            continue
        found[pid] = rest.split()
    return found


def pgrep_external_sync(config, binary=None, runner=None, ps_runner=None,
                        exclude_pids=()):
    """Return pids of native syncs already running against the same config.

    Two stages: a cheap pgrep pre-filter for candidates, then an argv-shaped
    check of each candidate. Our own pid and parent pid are filtered out, and
    only argv is inspected -- never the environment. Raises ExternalCheckError
    if the check cannot be completed, so callers can fail closed.
    """
    binary = binary or DEFAULT_PATHS.binary
    runner = runner or subprocess.run
    pattern = external_sync_pattern(binary, config)
    try:
        proc = runner([PGREP_BIN, "-f", pattern],
                      stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                      universal_newlines=True)
    except OSError as exc:
        raise ExternalCheckError("cannot run %s: %s" % (PGREP_BIN, exc))
    if proc.returncode == 1:
        return []
    if proc.returncode != 0:
        raise ExternalCheckError("%s exited %s" % (PGREP_BIN, proc.returncode))

    mine = set([os.getpid(), os.getppid()]) | set(exclude_pids)
    candidates = []
    for token in (proc.stdout or "").split():
        try:
            pid = int(token)
        except ValueError:
            continue
        if pid in mine or pid in candidates:
            continue
        candidates.append(pid)

    argv_by_pid = _read_argv_by_pid(candidates, runner=ps_runner or runner)
    return [pid for pid in candidates
            if argv_is_external_sync(argv_by_pid.get(pid, []), binary, config)]


# --------------------------------------------------------------------------
# Durable state
# --------------------------------------------------------------------------


def target_entry(target, status=STATUS_NOT_STARTED, exit_code=None,
                 started_at=None, finished_at=None, duration_seconds=None,
                 attempts=0):
    return {
        "guild_id": target.guild_id,
        "channel_id": target.channel_id,
        "channel": target.channel_name,
        "channel_type": target.channel_type,
        "status": status,
        "exit_code": exit_code,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": duration_seconds,
        "attempts": attempts,
    }


def new_state(fingerprint, targets=()):
    """Fresh state, with every approved target explicitly seeded not_started."""
    return {
        "version": STATE_VERSION,
        "targets_fingerprint": fingerprint,
        "cursor": 0,
        "updated_at": None,
        "status_semantics": STATUS_SEMANTICS,
        "targets": collections.OrderedDict(
            ("%s/%s" % (t.guild_id, t.channel_id), target_entry(t))
            for t in targets),
    }


def read_state(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (IOError, OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def write_state_atomic(path, state):
    directory = os.path.dirname(path)
    if directory and not os.path.isdir(directory):
        os.makedirs(directory, 0o700, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


# --------------------------------------------------------------------------
# Supervisor
# --------------------------------------------------------------------------


class Supervisor(object):
    def __init__(self, paths=DEFAULT_PATHS,
                 budget_seconds=DEFAULT_BUDGET_SECONDS,
                 child_seconds=DEFAULT_CHILD_SECONDS,
                 grace_seconds=DEFAULT_GRACE_SECONDS,
                 max_targets=None, dry_run=False,
                 expected_guilds=EXPECTED_GUILDS,
                 expected_counts=EXPECTED_COUNTS,
                 easycop_guild_id=EASYCOP_GUILD_ID,
                 popen=subprocess.Popen, external_check=None,
                 env=None, clock=time.monotonic):
        self.paths = paths
        self.budget_seconds = float(budget_seconds)
        self.child_seconds = float(child_seconds)
        self.grace_seconds = float(grace_seconds)
        self.max_targets = max_targets
        self.dry_run = bool(dry_run)
        self.expected_guilds = expected_guilds
        self.expected_counts = expected_counts
        self.easycop_guild_id = easycop_guild_id
        self._popen = popen
        self._env = env
        self._clock = clock
        self._child_seq = 0
        # Held for the whole supervisor + child lifetime and inherited by every
        # native child, so the lock outlives an abrupt supervisor death.
        self._lock_fd = None
        self._unreaped_children = 0
        if external_check is None:
            external_check = lambda config: pgrep_external_sync(
                config, binary=paths.binary)
        self._external_check = external_check

    # -- infrastructure ----------------------------------------------------

    def _ensure_dirs(self):
        for directory in (os.path.dirname(self.paths.state),
                          os.path.dirname(self.paths.lock),
                          os.path.dirname(self.paths.log),
                          self.paths.child_log_dir):
            if directory and not os.path.isdir(directory):
                os.makedirs(directory, 0o700, exist_ok=True)

    def _log(self, event, **fields):
        if self.dry_run:
            return
        record = {"ts": _utcnow(), "event": event, "pid": os.getpid()}
        record.update(fields)
        directory = os.path.dirname(self.paths.log)
        if directory and not os.path.isdir(directory):
            os.makedirs(directory, 0o700, exist_ok=True)
        line = json.dumps(record, sort_keys=True, ensure_ascii=False)
        with open(self.paths.log, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def _acquire_lock(self):
        directory = os.path.dirname(self.paths.lock)
        if directory and not os.path.isdir(directory):
            os.makedirs(directory, 0o700, exist_ok=True)
        # Deliberately not O_CLOEXEC: this descriptor is handed to every native
        # child through Popen(pass_fds=...). A flock belongs to the open file
        # description, so while any inheritor still holds a copy the lock stays
        # taken -- including when this supervisor is SIGKILLed mid-child.
        fd = os.open(self.paths.lock, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError) as exc:
            os.close(fd)
            if exc.errno in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                return None
            raise
        return fd

    def _release_lock(self, fd):
        """Give up our share of the lifecycle lock.

        LOCK_UN acts on the shared open file description, so it would also
        unlock any native child that inherited the descriptor. It is therefore
        only used once every owned child is known to be reaped; if a child may
        have survived (a reap that could not be confirmed), we merely close our
        own descriptor and let the surviving child keep the lock until it exits.
        """
        if self._unreaped_children == 0:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except (IOError, OSError):
                pass
        try:
            os.close(fd)
        except (IOError, OSError):
            pass
        self._lock_fd = None

    def _child_log_path(self, target):
        self._child_seq += 1
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        if target is None:
            suffix = "easycop-%s" % self.easycop_guild_id
        else:
            suffix = "%s-%s" % (target.guild_id, target.channel_id)
        name = "%s-%d-%02d-%s.log" % (stamp, os.getpid(), self._child_seq,
                                      suffix)
        return os.path.join(self.paths.child_log_dir, name)

    def _prune_child_logs(self, keep=CHILD_LOG_KEEP):
        try:
            names = sorted(n for n in os.listdir(self.paths.child_log_dir)
                           if n.endswith(".log"))
        except (IOError, OSError):
            return
        for name in names[:max(0, len(names) - keep)]:
            try:
                os.unlink(os.path.join(self.paths.child_log_dir, name))
            except (IOError, OSError):
                pass

    # -- child lifecycle ---------------------------------------------------

    def _reap(self, proc):
        """Terminate (child pid only), grace, then kill. Always waitpid."""
        if proc.poll() is not None:
            return proc.returncode
        try:
            proc.terminate()
        except (IOError, OSError):
            pass
        try:
            return proc.wait(timeout=self.grace_seconds)
        except subprocess.TimeoutExpired:
            pass
        except BaseException:
            pass  # a second signal during the grace window: escalate
        try:
            proc.kill()
        except (IOError, OSError):
            pass
        for _ in range(3):
            try:
                return proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                continue
            except BaseException:
                continue
        return proc.poll()

    def _run_child(self, command, timeout, target, on_reap=None):
        self._ensure_dirs()
        log_path = self._child_log_path(target)
        result = {
            "command": list(command),
            "child_log": log_path,
            "started_at": _utcnow(),
            "timeout_seconds": timeout,
        }
        started = self._clock()
        handle = open(log_path, "ab", 0)
        # The child inherits the lifecycle lock descriptor and nothing else:
        # close_fds still shuts every unrelated descriptor. The native Go
        # process never touches this fd, it only has to hold it open, which is
        # what keeps the lock taken if this supervisor dies abruptly.
        pass_fds = () if self._lock_fd is None else (self._lock_fd,)
        try:
            proc = self._popen(command, stdout=handle,
                               stderr=subprocess.STDOUT,
                               stdin=subprocess.DEVNULL, env=self._env,
                               close_fds=True, pass_fds=pass_fds)
        except BaseException:
            handle.close()
            raise
        result["pid_child"] = proc.pid
        self._unreaped_children += 1
        try:
            try:
                code = proc.wait(timeout=timeout)
                status = STATUS_EXIT_ZERO if code == 0 else STATUS_EXIT_NONZERO
            except subprocess.TimeoutExpired:
                code = self._reap(proc)
                status = STATUS_TIMEOUT
        except BaseException:
            code = self._reap(proc)
            self._note_reaped(proc)
            self._finish(result, code, STATUS_SIGNAL_INTERRUPTED, started)
            if on_reap is not None:
                on_reap(result)
            raise
        finally:
            try:
                handle.close()
            except (IOError, OSError):
                pass
        self._note_reaped(proc)
        self._finish(result, code, status, started)
        if on_reap is not None:
            on_reap(result)
        return result

    def _note_reaped(self, proc):
        """Account for a child only once its exit has actually been collected.

        A child whose exit could not be confirmed still counts as possibly
        alive, which keeps _release_lock from unlocking the shared description
        out from under it.
        """
        if proc.poll() is not None:
            self._unreaped_children = max(0, self._unreaped_children - 1)

    def _finish(self, result, code, status, started):
        result["exit_code"] = code
        result["status"] = status
        result["finished_at"] = _utcnow()
        result["duration_seconds"] = round(self._clock() - started, 3)

    # -- cycle -------------------------------------------------------------

    def run(self, out=None):
        if out is None:
            out = _stdout_line
        if self.dry_run:
            return self._dry_run(out)

        lock_fd = self._acquire_lock()
        if lock_fd is None:
            self._log("lock_busy", lock=self.paths.lock)
            return EXIT_OK
        # Kept on the instance for the whole run so every native child spawned
        # by _run_child inherits it.
        self._lock_fd = lock_fd
        try:
            try:
                previous = signal.signal(signal.SIGTERM, self._on_sigterm)
                installed = True
            except ValueError:
                previous, installed = None, False
            try:
                return self._cycle()
            except _Interrupted as exc:
                self._log("terminated", signal=exc.signame,
                          exit_code=EXIT_SIGNALLED)
                return EXIT_SIGNALLED
            except KeyboardInterrupt:
                self._log("terminated", signal="SIGINT",
                          exit_code=EXIT_INTERRUPTED)
                return EXIT_INTERRUPTED
            finally:
                if installed:
                    signal.signal(signal.SIGTERM, previous)
        finally:
            self._release_lock(lock_fd)

    def _on_sigterm(self, signum, frame):
        raise _Interrupted("SIGTERM" if signum == signal.SIGTERM
                           else str(signum))

    def _cycle(self):
        cycle_started = self._clock()
        self._log("cycle_start", manifest=self.paths.manifest,
                  binary=self.paths.binary, config=self.paths.config,
                  budget_seconds=self.budget_seconds,
                  child_seconds=self.child_seconds,
                  max_targets=self.max_targets)

        try:
            pids = self._external_check(self.paths.config)
        except ExternalCheckError as exc:
            # Fail closed: without a working check we cannot prove that no other
            # native sync is running, so we must not start one.
            self._log("external_check_unavailable", error=str(exc),
                      action="skipped entire cycle, launched nothing")
            self._log("cycle_end", exit_code=EXIT_EXTERNAL_CHECK_FAILED,
                      targets_attempted=0,
                      elapsed_seconds=round(self._clock() - cycle_started, 3),
                      skipped="external_check_unavailable")
            return EXIT_EXTERNAL_CHECK_FAILED
        if pids:
            self._log("external_sync_detected", pids=pids,
                      action="skipped entire cycle, external process left alone")
            self._log("cycle_end", exit_code=EXIT_OK, targets_attempted=0,
                      elapsed_seconds=round(self._clock() - cycle_started, 3),
                      skipped="external_sync_running")
            return EXIT_OK

        targets = None
        manifest_error = None
        try:
            document = load_manifest(self.paths.manifest)
            validate_manifest(document, self.expected_guilds,
                              self.expected_counts, self.easycop_guild_id)
            targets = flatten_targets(document)
        except ManifestError as exc:
            manifest_error = str(exc)
            self._log("manifest_invalid", manifest=self.paths.manifest,
                      error=manifest_error,
                      action="selected work skipped; EasyCop still runs")

        easycop_cmd = easycop_command(self.paths, self.easycop_guild_id)
        try:
            easycop = self._run_child(easycop_cmd, None, None)
        except Exception as exc:  # could not even start the native process
            self._log("easycop_error", guild_id=self.easycop_guild_id,
                      error="%s: %s" % (type(exc).__name__, exc))
            easycop = {"status": STATUS_SPAWN_FAILED, "exit_code": None,
                       "duration_seconds": None, "child_log": None,
                       "pid_child": None, "command": easycop_cmd}
        self._log("easycop_sync", guild_id=self.easycop_guild_id,
                  status=easycop["status"], exit_code=easycop["exit_code"],
                  duration_seconds=easycop["duration_seconds"],
                  child_log=easycop["child_log"],
                  pid_child=easycop["pid_child"],
                  command=easycop["command"])
        easycop_failed = easycop["exit_code"] != 0

        attempted = 0
        if targets:
            try:
                attempted = self._run_selected(targets)
            except Exception as exc:  # never let selected work break the cycle
                self._log("selected_phase_error",
                          error="%s: %s" % (type(exc).__name__, exc))

        if easycop_failed:
            exit_code = EXIT_EASYCOP_FAILED
        elif manifest_error is not None:
            exit_code = EXIT_MANIFEST_INVALID
        else:
            exit_code = EXIT_OK
        self._prune_child_logs()
        self._log("cycle_end", exit_code=exit_code, targets_attempted=attempted,
                  easycop_status=easycop["status"],
                  elapsed_seconds=round(self._clock() - cycle_started, 3))
        return exit_code

    def _run_selected(self, targets):
        state = read_state(self.paths.state)
        fingerprint = targets_fingerprint(targets)
        if (state.get("targets_fingerprint") != fingerprint
                or not isinstance(state.get("targets"), dict)):
            state = new_state(fingerprint, targets)
        state["status_semantics"] = STATUS_SEMANTICS
        cursor = state.get("cursor", 0)
        if not isinstance(cursor, int) or cursor < 0:
            cursor = 0
        cursor = cursor % len(targets)

        deadline = self._clock() + self.budget_seconds
        attempted = 0
        while True:
            if self.max_targets is not None and attempted >= self.max_targets:
                break
            if attempted >= len(targets):
                break  # never lap the rotation twice inside one cycle
            remaining = deadline - self._clock()
            if remaining < MIN_SLICE_SECONDS:
                break
            target = targets[cursor]
            limit = min(self.child_seconds, remaining)
            command = selected_command(self.paths, target)
            key = "%s/%s" % (target.guild_id, target.channel_id)
            previous = state["targets"].get(key, {})
            attempts = previous.get("attempts", 0)
            if not isinstance(attempts, int):
                attempts = 0
            next_cursor = (cursor + 1) % len(targets)

            reaped = {"done": False}

            def on_reap(result, key=key, target=target, attempts=attempts,
                        next_cursor=next_cursor, state=state, reaped=reaped):
                reaped["done"] = True
                state["targets"][key] = target_entry(
                    target, status=result["status"],
                    exit_code=result["exit_code"],
                    started_at=result["started_at"],
                    finished_at=result["finished_at"],
                    duration_seconds=result["duration_seconds"],
                    attempts=attempts + 1)
                state["cursor"] = next_cursor
                state["updated_at"] = _utcnow()
                write_state_atomic(self.paths.state, state)
                self._log("selected_target", guild_id=target.guild_id,
                          guild=target.guild_name,
                          channel_id=target.channel_id,
                          channel=target.channel_name,
                          channel_type=target.channel_type,
                          status=result["status"],
                          exit_code=result["exit_code"],
                          duration_seconds=result["duration_seconds"],
                          timeout_seconds=result["timeout_seconds"],
                          child_log=result["child_log"],
                          pid_child=result["pid_child"],
                          command=result["command"])

            try:
                self._run_child(command, limit, target, on_reap=on_reap)
            except Exception as exc:
                # A target that cannot even be started must not wedge the
                # rotation: record it and move on to the next one.
                self._log("selected_target_error", guild_id=target.guild_id,
                          channel_id=target.channel_id,
                          channel=target.channel_name,
                          error="%s: %s" % (type(exc).__name__, exc))
                if not reaped["done"]:
                    state["targets"][key] = target_entry(
                        target, status=STATUS_SPAWN_FAILED,
                        attempts=attempts + 1)
                    state["cursor"] = next_cursor
                    state["updated_at"] = _utcnow()
                    write_state_atomic(self.paths.state, state)
            cursor = next_cursor
            attempted += 1
        return attempted

    # -- dry run -----------------------------------------------------------

    def _dry_run(self, out):
        out("# discrawl-me selective sync — DRY RUN")
        out("# no native launch, no token read, no state/log/lock writes")
        out("binary:    %s" % self.paths.binary)
        out("config:    %s" % self.paths.config)
        out("manifest:  %s" % self.paths.manifest)
        out("state:     %s" % self.paths.state)
        out("lock:      %s" % self.paths.lock)
        out("log:       %s" % self.paths.log)
        out("budget_seconds=%s child_seconds=%s max_targets=%s"
            % (self.budget_seconds, self.child_seconds, self.max_targets))

        easycop = easycop_command(self.paths, self.easycop_guild_id)
        try:
            document = load_manifest(self.paths.manifest)
            validate_manifest(document, self.expected_guilds,
                              self.expected_counts, self.easycop_guild_id)
            targets = flatten_targets(document)
        except ManifestError as exc:
            out("")
            out("MANIFEST INVALID: %s" % exc)
            out("selected work would be skipped entirely this cycle")
            out("")
            out("step 1 — existing collection (unfiltered, no time limit):")
            out("  " + _quote(easycop))
            return EXIT_MANIFEST_INVALID

        state = read_state(self.paths.state)
        cursor = state.get("cursor", 0)
        if (not isinstance(cursor, int) or cursor < 0
                or state.get("targets_fingerprint") != targets_fingerprint(targets)):
            cursor = 0
        cursor = cursor % len(targets)

        out("")
        out("step 1 — existing collection (unfiltered, no time limit):")
        out("  " + _quote(easycop))
        out("")
        out("step 2 — selected rotation: %d targets, resuming at index %d,"
            " one native child at a time" % (len(targets), cursor))
        for offset in range(len(targets)):
            target = targets[(cursor + offset) % len(targets)]
            out("  [%03d] %s / %s (%s, %s)"
                % (offset, target.guild_name, target.channel_name,
                   target.guild_id, target.channel_type))
            out("        " + _quote(selected_command(self.paths, target)))
        return EXIT_OK


def _quote(command):
    return " ".join(shlex.quote(part) for part in command)


def _stdout_line(text):
    sys.stdout.write(text + "\n")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _positive_float(text):
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError("%r is not a number" % text)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return value


def _positive_int(text):
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError("%r is not an integer" % text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return value


def parse_args(argv):
    parser = argparse.ArgumentParser(
        prog="run-selected-sync.py",
        description="Run the existing EasyCop sync, then a bounded slice of the "
                    "approved selective backfill. Paths and the approved "
                    "selection are fixed in this file.")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the exact planned commands and target order, "
                             "launch nothing, write nothing")
    parser.add_argument("--budget-seconds", type=_positive_float,
                        default=DEFAULT_BUDGET_SECONDS,
                        help="total seconds for selected work per cycle "
                             "(default: %(default)s)")
    parser.add_argument("--child-seconds", type=_positive_float,
                        default=DEFAULT_CHILD_SECONDS,
                        help="per-target cap, further clipped by the remaining "
                             "budget (default: %(default)s)")
    parser.add_argument("--max-targets", type=_positive_int, default=None,
                        help="stop after this many selected targets this cycle")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    supervisor = Supervisor(paths=DEFAULT_PATHS,
                            budget_seconds=args.budget_seconds,
                            child_seconds=args.child_seconds,
                            max_targets=args.max_targets,
                            dry_run=args.dry_run)
    return supervisor.run()


if __name__ == "__main__":
    sys.exit(main())
