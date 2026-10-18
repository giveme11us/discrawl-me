"""Offline fake-clock budget tests for run-selected-sync.py (t_6e2da362 §4).

No Discord, no token, no real collector/service: native children are either
simulated (fake clock) or a local `python3 -c sleep` process for reap tests.
Budget figures here are LOCAL technical bounds, not a live duration measure.
"""
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("runner", ROOT / "run-selected-sync.py")
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
import test_selected_discovery as fx  # noqa: E402  (shared synthetic fixtures)

EC, DU, SD = r.EASYCOP_GUILD_ID, r.DUSTIFY_GUILD_ID, r.SNEAKER_DEVELOPMENT_GUILD_ID
PARENTS = {EC: [str(720000000000000000 + n) for n in range(10)],
           DU: [str(730000000000000000 + n) for n in range(200)]}


class Clock(object):
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class SimSupervisor(r.Supervisor):
    """Simulated children advance the fake clock by min(duration, timeout)."""
    toplevel_seconds = 40.0
    parent_seconds_each = 10.0
    parent_base = 20.0
    easycop_seconds = 2000.0
    selected_seconds = 45.0
    toplevel_fail = False

    def _run_child(self, command, timeout, target, on_reap=None):
        start = self._clock()
        pending_chunk = None
        if "discover" in command:
            gid = command[command.index("--guild") + 1]
            if "--thread-parents" in command:
                chunk = command[command.index("--thread-parents") + 1].split(",")
                slow = getattr(self, "slow_parents", {})
                duration = self.parent_base + sum(slow.get(c, self.parent_seconds_each) for c in chunk)
                self.archive_pages_seen.append(
                    command[command.index("--archive-pages") + 1] if "--archive-pages" in command else None)
                kind = "parent"
                pending_chunk = (gid, chunk)
            else:
                duration = self.toplevel_seconds
                kind = "toplevel"
            log = Path(self.tmp) / ("metadata-%s.json" % gid)
            log.write_text(json.dumps(self.reports[gid]))
        elif "--channels" in command and EC in command and "--exact-channels" in command \
                and self.easycop_cmd_marker not in self.kinds_seen():
            duration, kind, log = self.easycop_seconds, "easycop", Path(self.tmp) / "easycop.log"
        else:
            duration, kind, log = self.selected_seconds, "selected", Path(self.tmp) / "sel.log"
        exit_status = r.STATUS_EXIT_ZERO
        code = 0
        if kind == "toplevel" and self.toplevel_fail:
            exit_status, code = r.STATUS_EXIT_NONZERO, 1
        if timeout is not None and duration > timeout:
            self.clock.t += timeout
            exit_status, code = r.STATUS_TIMEOUT, -15
        else:
            self.clock.t += duration
            if kind == "parent" and pending_chunk:
                # only a COMPLETED slice counts as scanned
                self.scanned.setdefault(pending_chunk[0], []).extend(pending_chunk[1])
        if kind == "parent" and pending_chunk:
            self.attempted.setdefault(pending_chunk[0], []).append(list(pending_chunk[1]))
        self.events.append({"kind": kind, "timeout": timeout, "start": start,
                            "end": self._clock(), "status": exit_status})
        result = {"status": exit_status, "exit_code": code, "child_log": str(log),
                  "duration_seconds": self._clock() - start, "pid_child": None,
                  "command": command, "started_at": "sim", "finished_at": "sim",
                  "timeout_seconds": timeout}
        if on_reap:
            on_reap(result)
        return result

    easycop_cmd_marker = "easycop"

    def kinds_seen(self):
        return [e["kind"] for e in self.events]


def make(tmp, clock):
    doc, reports = fx.fixtures()
    p = Path(tmp)
    paths = r.Paths("fixture-native", str(p / "config"), str(p / "manifest"), str(p / "state"),
                    str(p / "lock"), str(p / "log"), str(p / "children"))
    Path(paths.manifest).write_text(json.dumps(doc))
    sup = SimSupervisor(paths=paths, expected_counts={SD: 1, DU: 2},
                        external_check=lambda _: [], clock=clock)
    sup.tmp, sup.reports, sup.clock = tmp, reports, clock
    sup.events, sup.scanned, sup.archive_pages_seen, sup.attempted = [], {}, [], {}
    return sup


def parent_ids(document, reports):
    return {gid: list(ids) for gid, ids in PARENTS.items()}


class BudgetTests(unittest.TestCase):
    def run_cycle(self, tmp, clock, **attrs):
        sup = make(tmp, clock)  # new instance per cycle = resume after restart
        for k, v in attrs.items():
            setattr(sup, k, v)
        start = clock()
        with mock.patch.object(r, "discovery_parent_ids", parent_ids):
            code = sup.run()
        return sup, code, start

    def assert_cycle_bounds(self, sup, start):
        ev = sup.events
        tops = [e for e in ev if e["kind"] == "toplevel"]
        self.assertEqual(len(tops), 2, "both top-level inventories every cycle")
        meta = [e for e in ev if e["kind"] in ("toplevel", "parent")]
        self.assertLessEqual(max(e["end"] for e in meta) - start, r.METADATA_BUDGET_SECONDS + 1e-6)
        easy = [e for e in ev if e["kind"] == "easycop"]
        self.assertEqual(len(easy), 1)
        self.assertLessEqual(easy[0]["timeout"], r.EASYCOP_MESSAGE_SECONDS)
        selected = [e for e in ev if e["kind"] == "selected"]
        self.assertTrue(selected, "selected phase must not be starved")
        self.assertLessEqual(max(e["end"] for e in ev) - start,
                             r.APP_DEADLINE_SECONDS - r.CLEANUP_MARGIN_SECONDS + 1e-6)
        self.assertLess(r.APP_DEADLINE_SECONDS, 1800.0)

    def test_whole_cycle_bounded_parents_rotate_and_resume(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as tmp:
            seen = {EC: set(), DU: set()}
            for cycle in range(60):
                sup, code, start = self.run_cycle(tmp, clock)
                self.assertEqual(code, 0, "EasyCop budget expiry is expected, not an error")
                self.assert_cycle_bounds(sup, start)
                for gid in seen:
                    seen[gid].update(sup.scanned.get(gid, []))
                state = json.loads(Path(tmp, "state.parents.json").read_text())
                self.assertIn(DU, state["guilds"])
                if seen[DU] == set(PARENTS[DU]) and seen[EC] == set(PARENTS[EC]):
                    break
                clock.t += 600
            self.assertEqual(seen[EC], set(PARENTS[EC]))
            self.assertEqual(seen[DU], set(PARENTS[DU]), "rotation must eventually cover all parents")
            self.cycles_for_full_rotation = cycle + 1
            print("\nsimulated cycles for full 210-parent rotation: %d" % (cycle + 1))
            self.assertGreater(cycle, 0, "200 parents cannot fit one 300 s metadata budget")
            plan = json.loads(Path(tmp, "state.discovery.json").read_text())
            self.assertFalse(plan["thread_coverage_complete"])
            self.assertIn(plan["parent_scan"][DU]["status"], (r.PARENT_STATUS_OK, r.PARENT_STATUS_SKIPPED))

    def test_parent_budget_expiry_is_partial_not_starvation(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as tmp:
            sup, code, start = self.run_cycle(tmp, clock, parent_seconds_each=60.0)
            self.assertEqual(code, 0)
            self.assert_cycle_bounds(sup, start)
            plan = json.loads(Path(tmp, "state.discovery.json").read_text())
            self.assertEqual(plan["parent_scan"][EC]["status"], r.PARENT_STATUS_BUDGET)
            state = json.loads(Path(tmp, "state.parents.json").read_text())
            self.assertEqual(state["guilds"][EC]["cursor"], 0, "expiry must not advance the cursor")
            first_hint = state["guilds"][EC]["slice_hint"]
            # Next cycle: smaller slice, eventually progresses.
            for _ in range(6):
                clock.t += 600
                sup, code, start = self.run_cycle(tmp, clock, parent_seconds_each=60.0)
                self.assertEqual(code, 0)
                state = json.loads(Path(tmp, "state.parents.json").read_text())
                if state["guilds"][EC]["cursor"] > 0:
                    break
            self.assertLess(state["guilds"][EC]["slice_hint"] if state["guilds"][EC]["cursor"] == 0 else 0,
                            first_hint + 1)
            self.assertGreater(state["guilds"][EC]["cursor"], 0, "halving slices must make progress")

    def test_single_slow_parent_does_not_block_rotation(self):
        # R1-03: one parent slower than any slice (even size 1). The runner
        # steps past it after a size-1 expiry; later parents keep progressing
        # and the slow one is retried on its next turn (native page cursor).
        clock = Clock()
        slow_id = sorted(PARENTS[EC])[0]
        with tempfile.TemporaryDirectory() as tmp:
            seen = set()
            slow_turns = 0
            for cycle in range(80):
                sup, code, start = self.run_cycle(tmp, clock, slow_parents={slow_id: 5000.0})
                self.assertEqual(code, 0)
                self.assert_cycle_bounds(sup, start)
                self.assertTrue(all(v == str(r.ARCHIVE_PAGES_PER_PARENT) for v in sup.archive_pages_seen))
                slow_turns += sum(1 for c in sup.attempted.get(EC, []) if slow_id in c)
                plan = json.loads(Path(tmp, "state.discovery.json").read_text())
                scan = plan["parent_scan"][EC]
                if scan.get("advanced_past_slow_parent"):
                    self.assertEqual(scan["cursor_after"], (scan["cursor_before"] + 1) % scan["total_parents"])
                seen.update(p for p in sup.scanned.get(EC, []) if p != slow_id)
                if seen == set(PARENTS[EC]) - {slow_id} and slow_turns >= 2:
                    break
                clock.t += 600
            self.assertEqual(seen, set(PARENTS[EC]) - {slow_id}, "parents after the slow one keep progressing")
            self.assertGreaterEqual(slow_turns, 2, "slow parent is retried on later turns")

    def test_toplevel_error_stays_fail_closed(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as tmp:
            sup, code, _ = self.run_cycle(tmp, clock)
            self.assertEqual(code, 0)
            plan_before = Path(tmp, "state.discovery.json").read_bytes()
            parents_before = Path(tmp, "state.parents.json").read_bytes()
            sup, code, _ = self.run_cycle(tmp, clock, toplevel_fail=True)
            self.assertEqual(code, 4)
            self.assertEqual([e["kind"] for e in sup.events], ["toplevel"])
            self.assertEqual(Path(tmp, "state.discovery.json").read_bytes(), plan_before)
            self.assertEqual(Path(tmp, "state.parents.json").read_bytes(), parents_before)

    def test_toplevel_timeout_is_fail_closed_too(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as tmp:
            sup, code, _ = self.run_cycle(tmp, clock, toplevel_seconds=500.0)
            self.assertEqual(code, 4, "no cached=fresh: top-level expiry never publishes a plan")
            self.assertFalse(any(e["kind"] in ("easycop", "selected") for e in sup.events))

    def test_memory_policy_only_on_sync_commands_and_default_unchanged(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as tmp:
            sup = make(tmp, clock)
            sup.memory_policy = "/abs/policy.json"
            commands = []
            orig = sup._run_child

            def spy(command, timeout, target, on_reap=None):
                commands.append(command)
                return orig(command, timeout, target, on_reap)
            sup._run_child = spy
            with mock.patch.object(r, "discovery_parent_ids", parent_ids):
                self.assertEqual(sup.run(), 0)
            syncs = [c for c in commands if "sync" in c]
            self.assertTrue(syncs)
            self.assertTrue(all(c[-2:] == ["--memory-policy", "/abs/policy.json"] for c in syncs))
            self.assertFalse(any("--memory-policy" in c for c in commands if "discover" in c))
            legacy = make(tmp, Clock())
            self.assertEqual(legacy._with_memory(["x", "sync"]), ["x", "sync"])

    def test_easycop_disabled_by_explicit_null(self):
        """Final policy: manifest existing_guild_unfiltered=null -> no EasyCop
        discovery and no EasyCop sync; SD/Dustify unchanged; missing key fails."""
        clock = Clock()
        with tempfile.TemporaryDirectory() as tmp:
            sup = make(tmp, clock)
            doc = json.loads(Path(sup.paths.manifest).read_text())
            doc["existing_guild_unfiltered"] = None
            Path(sup.paths.manifest).write_text(json.dumps(doc))
            sup.memory_policy, sup.with_embeddings = "/abs/policy.json", True
            commands = []
            orig = sup._run_child

            def spy(command, timeout, target, on_reap=None):
                commands.append(command)
                return orig(command, timeout, target, on_reap)
            sup._run_child = spy
            with mock.patch.object(r, "discovery_parent_ids",
                                   lambda d, rep: {DU: list(PARENTS[DU])}):
                self.assertEqual(sup.run(), 0)
            self.assertTrue(commands)
            self.assertFalse(any(EC in c for c in commands), "nothing launched for EasyCop")
            self.assertEqual(len([e for e in sup.events if e["kind"] == "toplevel"]), 1)
            syncs = [c for c in commands if "sync" in c]
            self.assertTrue(syncs)
            self.assertTrue(all(c[-3:] == ["--memory-policy", "/abs/policy.json", "--with-embeddings"] for c in syncs))
            del doc["existing_guild_unfiltered"]
            with self.assertRaises(r.ManifestError):
                r.validate_manifest(doc, expected_counts={SD: 1, DU: 2})


class ReapTests(unittest.TestCase):
    def paths(self, tmp):
        p = Path(tmp)
        return r.Paths(sys.executable, str(p / "config"), str(p / "manifest"), str(p / "state"),
                       str(p / "lock"), str(p / "log"), str(p / "children"))

    def test_timeout_kills_and_reaps_real_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            sup = r.Supervisor(paths=self.paths(tmp), grace_seconds=0.5)
            res = sup._run_child([sys.executable, "-c", "import time; time.sleep(30)"], 0.5,
                                 r.Target(EC, "g", "1", "c", "t"))
            self.assertEqual(res["status"], r.STATUS_TIMEOUT)
            self.assertEqual(sup._unreaped_children, 0)
            with self.assertRaises(ProcessLookupError):
                os.kill(res["pid_child"], 0)

    def test_sigterm_mid_child_reaps_and_releases_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = self.paths(tmp)
            sup = r.Supervisor(paths=paths, grace_seconds=0.5, external_check=lambda _: [])
            pids = []

            def cycle():
                res_holder = {}
                def on_reap(res):
                    res_holder.update(res)
                threading.Timer(0.7, lambda: os.kill(os.getpid(), signal.SIGTERM)).start()
                try:
                    sup._run_child([sys.executable, "-c", "import time; time.sleep(30)"], 60,
                                   r.Target(EC, "g", "1", "c", "t"), on_reap=on_reap)
                finally:
                    pids.append(res_holder.get("pid_child"))
                return 0
            sup._cycle = cycle
            code = sup.run()
            self.assertEqual(code, r.EXIT_SIGNALLED)
            self.assertEqual(sup._unreaped_children, 0)
            with self.assertRaises(ProcessLookupError):
                os.kill(pids[0], 0)
            # Lock released: a second supervisor can take it immediately.
            other = r.Supervisor(paths=paths)
            fd = other._acquire_lock()
            self.assertIsNotNone(fd)
            other._release_lock(fd)


if __name__ == "__main__":
    unittest.main(verbosity=2)
