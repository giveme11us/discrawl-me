"""Offline fixtures only. No Discord/config token reads or native live sync."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("runner", ROOT / "run-selected-sync.py")
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
EC, DU, SD = r.EASYCOP_GUILD_ID, r.DUSTIFY_GUILD_ID, r.SNEAKER_DEVELOPMENT_GUILD_ID

def sid(n):
    return str(700000000000000000 + n)

def channel(n, gid, name, kind=0, parent=None, archived=False):
    ch = {"id": sid(n), "guild_id": gid, "name": name, "type": kind}
    if parent is not None:
        ch["parent_id"] = sid(parent)
    if archived:
        ch["thread_metadata"] = {"archived": True}
    return ch

def fixtures():
    doc = {"gateway": "disabled", "include_dms": False, "existing_guild_unfiltered": EC,
           "new_guilds": [{"guild_id": SD, "guild": "SD", "channels": [
               {"id": sid(50), "name": "existing", "category": "General", "type": "Testo"}]},
               {"guild_id": DU, "guild": "Dustify", "channels": [
                   {"id": sid(2), "name": "06-11-old-release", "category": "POKEMON RELEASES", "type": "Testo"},
                   {"id": sid(3), "name": "one-piece-releases", "category": "TCG CARDS", "type": "Forum"}]}]}
    dust = [channel(1, DU, "POKEMON RELEASES", 4), channel(9, DU, "TCG CARDS", 4),
            channel(2, DU, "06-11-old-release", parent=1), channel(3, DU, "one-piece-releases", 15, 9),
            channel(4, DU, "20-11-new-item", parent=1), channel(5, DU, "payments-monitor", parent=1),
            channel(6, DU, "UNKNOWN RELEASES", 4), channel(7, DU, "20-12-ambiguous", parent=6),
            channel(8, DU, "new-public-thread", 11, 4), channel(10, DU, "secret", 12, 4),
            channel(11, DU, "excluded-thread", 11, 5), channel(12, DU, "02-11-funko", parent=1)]
    easy = [channel(20, EC, "TCG INFO", 4), channel(21, EC, "20-11-new-item", parent=20),
            channel(22, EC, "PAST RELEASES", 4), channel(23, EC, "old", parent=22),
            channel(24, EC, "new-thread", 11, 21), channel(25, EC, "dm", 1),
            channel(26, EC, "voice", 2)]
    return doc, {gid: {"guild_id": gid, "observed_at": "2026-10-15T08:00:00Z", "channels": channels,
                       "coverage": {"top_level": "observed", "active_threads": "unavailable", "complete": False}}
                 for gid, channels in ((EC, easy), (DU, dust))}

def full_size_fixtures():
    doc, reports = fixtures()
    for entry, count in zip(doc["new_guilds"], (35, 108)):
        for n in range(len(entry["channels"]), count):
            entry["channels"].append({"id": str(710000000000000000 + n + (1000 if entry["guild_id"] == DU else 0)),
                                      "name": "fixture-" + str(n), "type": "Testo", "category": "General"})
    return doc, reports


def decisions(plan, gid):
    return {d["id"]: d for d in plan["guilds"][gid]["decisions"]}

class SelectionTests(unittest.TestCase):
    def test_new_subject_exclusions_cover_threads_and_preserve_legacy_seeds(self):
        doc, reports = full_size_fixtures()
        dust_entry = next(e for e in doc["new_guilds"] if e["guild_id"] == DU)
        # All 108 synthetic legacy seeds are observed in their approved category.
        reports[DU]["channels"].append(channel(60, DU, "General", 4))
        for seed in dust_entry["channels"][2:]:
            reports[DU]["channels"].append({"id": seed["id"], "guild_id": DU,
                "name": seed["name"], "type": 0, "parent_id": sid(60)})
        for offset, subject in enumerate(("funko", "topps", "vinyl")):
            seed = dust_entry["channels"][2 + offset]
            seed["name"] = "02-11-" + subject
            current = next(c for c in reports[DU]["channels"] if c["id"] == seed["id"])
            current["name"] = seed["name"]
            # Text, news thread, public thread, cached archive and forum post.
            for kind, parent, n in ((0, 1, 100), (10, 4, 110), (11, 4, 120), (11, 3, 130)):
                reports[DU]["channels"].append(channel(n + offset, DU, seed["name"], kind, parent))
            reports[DU].setdefault("cached_threads", []).append(
                channel(140 + offset, DU, seed["name"], 11, 3, True))
        reports[DU]["channels"].append(channel(150, DU, "new-pokemon-post", 11, 3))
        original = copy.deepcopy(doc)
        effective, plan = r.collection_plan(doc, reports)
        selected = {t.channel_id for t in r.flatten_targets(effective) if t.guild_id == DU}
        self.assertEqual(len(dust_entry["channels"]), 108)
        self.assertTrue({s["id"] for s in dust_entry["channels"]} <= selected)
        for d in decisions(plan, DU).values():
            if d["id"] in {sid(n + offset) for n in (100, 110, 120, 130, 140) for offset in range(3)}:
                self.assertFalse(d["collect"], d)
                self.assertEqual(d["reason"], "excluded editorial subject")
                self.assertNotIn(d["id"], selected)
        self.assertTrue(decisions(plan, DU)[sid(8)]["collect"])
        self.assertTrue(decisions(plan, DU)[sid(150)]["collect"])
        self.assertEqual(doc, original)
        self.assertIn("--exact-channels", r.selected_command(r.DEFAULT_PATHS, r.Target(DU, "", sid(3), "", "Forum")))
        self.assertNotIn("--exact-channels", r.selected_command(r.DEFAULT_PATHS, r.Target(SD, "", sid(50), "", "Forum")))
        self.assertIn("--exact-channels", r.easycop_command(r.DEFAULT_PATHS, EC, [sid(21)]))

    def test_new_relevant_both_guilds_and_excluded(self):
        doc, reports = fixtures()
        effective, plan = r.collection_plan(doc, reports)
        d, e = decisions(plan, DU), decisions(plan, EC)
        for cid in (4, 8):
            self.assertTrue(d[sid(cid)]["collect"])
        self.assertTrue(e[sid(21)]["collect"])
        self.assertTrue(e[sid(24)]["collect"])
        for cid in (5, 7, 10, 11, 12):
            self.assertFalse(d[sid(cid)]["collect"])
        self.assertFalse(e[sid(25)]["collect"])
        self.assertFalse(e[sid(26)]["collect"])
        self.assertEqual(e[sid(23)]["room_state"], "historical")
        self.assertFalse(plan["thread_coverage_complete"])
        self.assertEqual(effective["new_guilds"][0], doc["new_guilds"][0])
        self.assertEqual(doc, fixtures()[0])
        self.assertIn(sid(4), {t.channel_id for t in r.flatten_targets(effective)})

    def test_rename_move_archival_and_missing_not_deletion(self):
        doc, reports = fixtures()
        reports[DU]["channels"][2]["name"] = "renamed-existing"
        _, plan = r.collection_plan(doc, reports)
        self.assertTrue(decisions(plan, DU)[sid(2)]["collect"])
        reports[DU]["channels"][2]["parent_id"] = sid(6)
        _, plan = r.collection_plan(doc, reports)
        self.assertFalse(decisions(plan, DU)[sid(2)]["collect"])
        reports[EC]["channels"][1]["parent_id"] = sid(22)
        reports[EC]["channels"][4]["thread_metadata"] = {"archived": True}
        _, plan = r.collection_plan(doc, reports)
        self.assertEqual(decisions(plan, EC)[sid(21)]["room_state"], "historical")
        self.assertEqual(decisions(plan, EC)[sid(24)]["room_state"], "historical")
        reports[DU]["channels"] = [c for c in reports[DU]["channels"] if c["id"] != sid(2)]
        _, plan = r.collection_plan(doc, reports)
        self.assertIn(sid(2), plan["guilds"][DU]["missing_baseline_ids"])

    def test_duplicate_category_and_channel_type_change_fail_closed(self):
        doc, reports = fixtures()
        reports[DU]["channels"].append(channel(30, DU, "POKEMON RELEASES", 4))
        _, plan = r.collection_plan(doc, reports)
        self.assertFalse(decisions(plan, DU)[sid(4)]["collect"])
        self.assertIn("ambiguous", decisions(plan, DU)[sid(4)]["reason"])
        reports[DU]["channels"].pop()
        reports[DU]["channels"][2]["type"] = 15
        _, plan = r.collection_plan(doc, reports)
        self.assertFalse(decisions(plan, DU)[sid(2)]["collect"])
        self.assertFalse(decisions(plan, DU)[sid(4)]["collect"])

    def test_cached_thread_never_promoted_to_freshness(self):
        doc, reports = fixtures()
        reports[EC]["cached_threads"] = [channel(29, EC, "cached", 11, 21, True)]
        _, plan = r.collection_plan(doc, reports)
        self.assertEqual(decisions(plan, EC)[sid(29)]["freshness"], "cached_unconfirmed")
        self.assertEqual(decisions(plan, EC)[sid(29)]["room_state"], "archived_thread")

    def test_second_plan_idempotent_and_state_migration_preserves(self):
        doc, reports = fixtures()
        a, p = r.collection_plan(doc, reports)
        b, q = r.collection_plan(doc, reports)
        self.assertEqual(a, b); self.assertEqual(p, q)
        old = {"cursor": 2, "targets": {DU + "/" + sid(2): {"attempts": 7}, "old": {"attempts": 9}}}
        state = r.reconcile_state(old, r.targets_fingerprint(r.flatten_targets(a)), r.flatten_targets(a))
        self.assertEqual(state["targets"][DU + "/" + sid(2)]["attempts"], 7)
        self.assertEqual(state["targets"]["old"]["attempts"], 9)
        self.assertEqual(state["cursor"], 2)
        self.assertNotIn("targets_fingerprint", old)

    def test_invalid_metadata_and_empty_command_fail_closed(self):
        doc, reports = fixtures()
        reports[DU]["channels"][0]["guild_id"] = EC
        with self.assertRaises(r.ManifestError): r.collection_plan(doc, reports)
        with self.assertRaises(r.ManifestError): r.easycop_command(r.DEFAULT_PATHS, EC, [])
        with self.assertRaises(r.ManifestError): r.selected_command(r.DEFAULT_PATHS, r.Target(DU,"", "", "", ""))
        with self.assertRaises(r.ManifestError): r.discovery_command(r.DEFAULT_PATHS, SD)

class CycleTests(unittest.TestCase):
    def test_metadata_timeout_scales_with_unique_parents_and_is_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "metadata.json"
            log.write_text("{}")
            sup = r.Supervisor()
            calls = []
            def metadata_child(command, timeout, target):
                calls.append((command, timeout))
                return {"status": r.STATUS_EXIT_ZERO, "child_log": str(log)}
            sup._run_child = metadata_child
            sup._discover(DU)
            sup._discover(DU, [sid(1), sid(2), sid(1)])
            sup._discover(DU, [sid(n) for n in range(200)])
            self.assertEqual([timeout for _, timeout in calls], [120.0, 140.0, 1800.0])
            self.assertNotIn("--thread-parents", calls[0][0])
            self.assertEqual(calls[1][0][-1], sid(1) + "," + sid(2))

    def test_cycle_discovery_before_sync_and_error_preserves_state(self):
        doc, reports = fixtures()
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            paths = r.Paths("fixture-native", str(p/"config"), str(p/"manifest"), str(p/"state"),
                            str(p/"lock"), str(p/"log"), str(p/"children"))
            Path(paths.manifest).write_text(json.dumps(doc))
            class FixtureSupervisor(r.Supervisor):
                fail = False
                def _run_child(self, command, timeout, target, on_reap=None):
                    self.commands.append(command)
                    is_discovery = "discover" in command
                    gid = command[command.index("--guild")+1]
                    log = p / ("metadata-" + gid)
                    if is_discovery:
                        log.write_text(json.dumps(reports[gid]))
                    result = {"status": r.STATUS_EXIT_NONZERO if self.fail and is_discovery else r.STATUS_EXIT_ZERO,
                              "exit_code": 1 if self.fail and is_discovery else 0, "child_log": str(log),
                              "duration_seconds": 0, "pid_child": None, "command": command,
                              "started_at": "fixture", "finished_at": "fixture", "timeout_seconds": timeout}
                    if on_reap: on_reap(result)
                    return result
            sup = FixtureSupervisor(paths=paths, expected_counts={SD:1, DU:2}, external_check=lambda _:[], max_targets=2)
            sup.commands=[]
            self.assertEqual(sup.run(), 0)
            self.assertTrue(all("discover" in c for c in sup.commands[:4]))
            self.assertTrue(all("--thread-parents" in c for c in sup.commands[2:4]))
            self.assertTrue(all("--channels" in c for c in sup.commands[4:]))
            self.assertTrue(all("--include-dms=false" in c for c in sup.commands[4:]))
            old_state = Path(paths.state).read_bytes()
            old_plan = Path(paths.state + ".discovery.json").read_bytes()
            sup.fail=True; sup.commands=[]
            self.assertEqual(sup.run(), 4)
            self.assertEqual(len(sup.commands), 1)
            self.assertEqual(Path(paths.state).read_bytes(), old_state)
            self.assertEqual(Path(paths.state + ".discovery.json").read_bytes(), old_plan)
            sup.fail = False
            reports[EC]["channels"] = []
            self.assertEqual(sup.run(), 4)
            self.assertEqual(Path(paths.state).read_bytes(), old_state)
            self.assertEqual(Path(paths.state + ".discovery.json").read_bytes(), old_plan)

    def test_public_parent_metadata_precedes_plan_archived_uncached_and_exact_sync(self):
        doc, top = fixtures()
        # Newly observed eligible forum parents are inferred from existing
        # category/role evidence too, not only the original manifest IDs.
        top[EC]["channels"].append(channel(30, EC, "new-forum", 15, 20))
        top[DU]["channels"].append(channel(70, DU, "pokemon-releases", 15, 9))
        archived = {EC: [channel(31, EC, "new-archived-ec", 11, 30, True)],
                    DU: [channel(40, DU, "new-archived-pokemon", 11, 3, True),
                         channel(41, DU, "new-archived-funko", 11, 3, True),
                         channel(71, DU, "new-parent-archive", 11, 70, True)]}
        fallback_active = channel(42, DU, "parent-only-active", 11, 3)
        self.assertTrue(all(not report.get("cached_threads") for report in top.values()))
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            paths = r.Paths("fixture-native", str(p/"config"), str(p/"manifest"), str(p/"state"),
                            str(p/"lock"), str(p/"log"), str(p/"children"))
            Path(paths.manifest).write_text(json.dumps(doc))
            class ForumSupervisor(r.Supervisor):
                fail_parent_guild = None
                failure_status = r.STATUS_EXIT_NONZERO
                malformed_parent = False
                def _run_child(self, command, timeout, target, on_reap=None):
                    self.commands.append(command)
                    gid = command[command.index("--guild") + 1]
                    parent_phase = "--thread-parents" in command
                    failed = parent_phase and gid == self.fail_parent_guild
                    log = p / ("metadata-" + gid)
                    if "discover" in command:
                        report = copy.deepcopy(top[gid])
                        if parent_phase:
                            requested = command[command.index("--thread-parents") + 1].split(",")
                            self.asserted_parents[gid] = requested
                            report["channels"] += [ch for ch in archived[gid] if ch["parent_id"] in requested]
                            if gid == DU and sid(3) in requested:
                                report["channels"].append(fallback_active)
                            report["coverage"].update(
                                parent_active_threads={cid: "observed" for cid in requested},
                                public_archives={cid: "observed" for cid in requested},
                                unavailable_reasons={"guild_active": "http_403"}, private_archives="not_requested")
                            if self.malformed_parent:
                                report["guild_id"] = SD
                        log.write_text(json.dumps(report))
                    result = {"status": self.failure_status if failed else r.STATUS_EXIT_ZERO,
                              "exit_code": 1 if failed else 0, "child_log": str(log),
                              "duration_seconds": 0, "pid_child": None, "command": command,
                              "started_at": "fixture", "finished_at": "fixture", "timeout_seconds": timeout}
                    if on_reap:
                        on_reap(result)
                    return result
            sup = ForumSupervisor(paths=paths, expected_counts={SD: 1, DU: 2},
                                  external_check=lambda _: [], max_targets=100)
            sup.commands = []; sup.asserted_parents = {}
            self.assertEqual(sup.run(), 0)
            plan = json.loads(Path(paths.state + ".discovery.json").read_text())
            self.assertIn(sid(40), decisions(plan, DU), "uncached public archive must be discovered before selection")
            for gid, cids in ((EC, (31,)), (DU, (40, 42, 71))):
                for cid in cids:
                    self.assertTrue(decisions(plan, gid)[sid(cid)]["collect"])
                    self.assertEqual(decisions(plan, gid)[sid(cid)]["freshness"], "observed")
            self.assertFalse(decisions(plan, DU)[sid(41)]["collect"])
            self.assertEqual(decisions(plan, DU)[sid(40)]["room_state"], "archived_thread")
            self.assertEqual(sup.asserted_parents, {EC: [sid(21), sid(23), sid(30)],
                                                   DU: [sid(2), sid(3), sid(4), sid(70)]})
            self.assertTrue(all("discover" in c for c in sup.commands[:4]))
            syncs = sup.commands[4:]
            self.assertTrue(syncs)
            self.assertTrue(all("sync" in c and "--channels" in c for c in syncs))
            selected_ids = {cid for c in syncs for cid in c[c.index("--channels") + 1].split(",")}
            self.assertTrue({sid(31), sid(40), sid(42), sid(71)} <= selected_ids)
            self.assertNotIn(sid(41), selected_ids)
            self.assertTrue(all("--exact-channels" in c for c in syncs if SD not in c))
            self.assertEqual(plan["guilds"][DU]["coverage"]["active_threads"], "unavailable")
            self.assertEqual(plan["guilds"][DU]["coverage"]["unavailable_reasons"], {"guild_active": "http_403"})
            self.assertFalse(plan["thread_coverage_complete"])
            # A second full cycle preserves the selection and increments attempts,
            # never duplicates targets or resets an existing rotation entry.
            old_state = r.read_state(paths.state)
            old_plan = Path(paths.state + ".discovery.json").read_bytes()
            sup.commands = []
            self.assertEqual(sup.run(), 0)
            self.assertEqual(Path(paths.state + ".discovery.json").read_bytes(), old_plan)
            new_state = r.read_state(paths.state)
            self.assertEqual(set(new_state["targets"]), set(old_state["targets"]))
            for key in old_state["targets"]:
                self.assertEqual(new_state["targets"][key]["attempts"], old_state["targets"][key]["attempts"] + 1)
            # Failure in EITHER parent metadata child stops before all messages,
            # retaining both the prior complete plan and the scheduler state.
            saved_state = Path(paths.state).read_bytes()
            for gid, status in ((EC, r.STATUS_EXIT_NONZERO), (DU, r.STATUS_TIMEOUT)):
                sup.fail_parent_guild = gid; sup.failure_status = status; sup.commands = []
                self.assertEqual(sup.run(), 4)
                self.assertTrue(all("discover" in c for c in sup.commands))
                self.assertEqual(Path(paths.state).read_bytes(), saved_state)
                self.assertEqual(Path(paths.state + ".discovery.json").read_bytes(), old_plan)
            sup.fail_parent_guild = None; sup.malformed_parent = True; sup.commands = []
            self.assertEqual(sup.run(), 4)
            self.assertTrue(all("discover" in c for c in sup.commands))
            self.assertEqual(Path(paths.state).read_bytes(), saved_state)
            self.assertEqual(Path(paths.state + ".discovery.json").read_bytes(), old_plan)

    def test_cli_dry_run_fixture_no_native_or_state_writes(self):
        # Exercise the actual CLI with full-sized synthetic approved manifest.
        doc, reports = full_size_fixtures()
        with tempfile.TemporaryDirectory() as tmp:
            reports[DU]["channels"] += [channel(40, DU, "02-11-funko", 11, 4),
                                         channel(41, DU, "new-pokemon-post", 11, 3),
                                         channel(42, DU, "02-11-vinyl", 11, 3)]
            p=Path(tmp); (p/".config/discrawl-me").mkdir(parents=True)
            (p/".config/discrawl-me/selected-guilds.json").write_text(json.dumps(doc))
            (p/"metadata.json").write_text(json.dumps(reports))
            before={str(f.relative_to(p)):f.read_bytes() for f in p.rglob("*") if f.is_file()}
            command=["python3",str(ROOT/"run-selected-sync.py"),"--dry-run","--discovery-fixture",str(p/"metadata.json")]
            proc=subprocess.run(command,env={**os.environ,"DISCRAWL_HOME":str(p)},capture_output=True,text=True)
            self.assertEqual(proc.returncode,0,proc.stderr+proc.stdout)
            self.assertIn("offline fixture decisions:",proc.stdout)
            self.assertIn(sid(4),proc.stdout)
            sync_lines = [line for line in proc.stdout.splitlines() if " sync " in line]
            self.assertTrue(sync_lines)
            self.assertFalse(any(sid(40) in line or sid(42) in line for line in sync_lines), proc.stdout)
            self.assertTrue(any(sid(41) in line for line in sync_lines), proc.stdout)
            self.assertTrue(all("--exact-channels" in line for line in sync_lines if DU in line or EC in line))
            self.assertEqual({str(f.relative_to(p)):f.read_bytes() for f in p.rglob("*") if f.is_file()},before)
            proc=subprocess.run(["python3",str(ROOT/"run-selected-sync.py"),"--discovery-fixture",str(p/"metadata.json")],
                                env={**os.environ,"DISCRAWL_HOME":str(p)},capture_output=True,text=True)
            self.assertEqual(proc.returncode,2)

if __name__ == "__main__": unittest.main(verbosity=2)
