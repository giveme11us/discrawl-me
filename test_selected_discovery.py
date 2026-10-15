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
            self.assertTrue(all("discover" in c for c in sup.commands[:2]))
            self.assertTrue(all("--channels" in c for c in sup.commands[2:]))
            self.assertTrue(all("--include-dms=false" in c for c in sup.commands[2:]))
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

    def test_cli_dry_run_fixture_no_native_or_state_writes(self):
        # Exercise the actual CLI with full-sized synthetic approved manifest.
        doc, reports = full_size_fixtures()
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp); (p/".config/discrawl-me").mkdir(parents=True)
            (p/".config/discrawl-me/selected-guilds.json").write_text(json.dumps(doc))
            (p/"metadata.json").write_text(json.dumps(reports))
            before={str(f.relative_to(p)):f.read_bytes() for f in p.rglob("*") if f.is_file()}
            command=["python3",str(ROOT/"run-selected-sync.py"),"--dry-run","--discovery-fixture",str(p/"metadata.json")]
            proc=subprocess.run(command,env={**os.environ,"DISCRAWL_HOME":str(p)},capture_output=True,text=True)
            self.assertEqual(proc.returncode,0,proc.stderr+proc.stdout)
            self.assertIn("offline fixture decisions:",proc.stdout)
            self.assertIn(sid(4),proc.stdout)
            self.assertEqual({str(f.relative_to(p)):f.read_bytes() for f in p.rglob("*") if f.is_file()},before)
            proc=subprocess.run(["python3",str(ROOT/"run-selected-sync.py"),"--discovery-fixture",str(p/"metadata.json")],
                                env={**os.environ,"DISCRAWL_HOME":str(p)},capture_output=True,text=True)
            self.assertEqual(proc.returncode,2)

if __name__ == "__main__": unittest.main(verbosity=2)
