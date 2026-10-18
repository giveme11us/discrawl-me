"""Offline tests for tools/verify_copy_learning.py (SYNTHETIC fixture only).

Shape mirrors the skill: 2 releases / 2 channels, 7 processed messages with
the exact skill fingerprint, 6 local drafts. Real state/registry are never read.
"""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
TOOL = HERE / "verify_copy_learning.py"
sys.path.insert(0, str(HERE))
import verify_copy_learning as vcl  # noqa: E402

EC = "422855027535642625"
CH = ["1547000000000000001", "1547000000000000002"]


class CopyLearningHarnessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.d = Path(self.tmp.name)
        self.skill = d / "SKILL.md"
        self.skill.write_text("synthetic skill copy v1.1.0\n")
        self.skill_sha = vcl.sha_file(self.skill)
        self.db = d / "selective.db"
        conn = sqlite3.connect(self.db)
        conn.execute("create table messages (id text primary key, guild_id text, channel_id text, content text, raw_json text, created_at text)")
        conn.execute("create table sync_state (scope text primary key, cursor text, updated_at text)")
        conn.execute("insert into sync_state values ('sync:last_success','2026-10-15T11:00:00Z','x')")
        conn.execute("insert into sync_state values ('memory:selective','{}','x')")
        processed = {}
        for n in range(7):
            mid = str(1547100000000000000 + n)
            ch = CH[n % 2]
            content = "post %d" % n
            embeds = [{"title": "T%d" % n, "fields": [{"name": "Retailer", "value": "v"}]}] if n % 3 == 0 else []
            conn.execute("insert into messages values (?,?,?,?,?,?)",
                         (mid, EC, ch, content, json.dumps({"id": mid, "embeds": embeds}), "2026-09-0%dT10:00:00Z" % (n + 1)))
            processed[mid] = vcl.fingerprint(content, embeds)
        conn.commit(); conn.close()
        drafts = []
        for n in range(6):
            p = d / ("draft-%d.md" % n)
            p.write_text("draft %d" % n)
            drafts.append(str(p))
        self.registry = d / "releases.json"
        self.registry.write_text(json.dumps({"releases": [
            {"slug": "synthetic-a", "guild_id": EC, "channel_id": CH[0], "since": "2026-09-01T00:00:00Z", "draft_files": drafts[:3]},
            {"slug": "synthetic-b", "guild_id": EC, "channel_id": CH[1], "since": "2026-09-01T00:00:00Z", "draft_files": drafts[3:]}]}))
        self.state = d / "state.json"
        self.state.write_text(json.dumps({"processed": processed, "signal_fingerprints": []}))

    def tearDown(self):
        self.tmp.cleanup()

    def run_tool(self, sha=None):
        before = self.state.read_bytes()
        proc = subprocess.run([sys.executable, str(TOOL), "--db", str(self.db), "--registry", str(self.registry),
                               "--state", str(self.state), "--skill", str(self.skill),
                               "--skill-sha256", sha or self.skill_sha], capture_output=True, text=True)
        self.assertEqual(self.state.read_bytes(), before, "harness must never write state")
        return proc.returncode, json.loads(proc.stdout)

    def test_baseline_seven_unchanged_six_drafts_zero_signals_and_replay(self):
        for _ in range(2):  # replay: identical result, state untouched
            code, out = self.run_tool()
            self.assertEqual(code, 0, out)
            self.assertEqual(out["totals"]["unchanged"], 7)
            self.assertEqual(out["totals"]["drafts_reachable"], 6)
            self.assertEqual(out["signals_emitted"], 0)
            self.assertEqual(out["novelties"], 0)
            self.assertTrue(out["selective_memory"])

    def test_new_and_edit_are_novelties_without_advancing_state(self):
        conn = sqlite3.connect(self.db)
        conn.execute("insert into messages values (?,?,?,?,?,?)", ("1547100000000000099", EC, CH[0], "new", "{}", "2026-10-10T10:00:00Z"))
        conn.execute("update messages set content = 'post 1 edited' where id = '1547100000000000001'")
        conn.commit(); conn.close()
        code, out = self.run_tool()
        self.assertEqual(code, 1)
        self.assertEqual((out["totals"]["new"], out["totals"]["changed"], out["totals"]["unchanged"]), (1, 1, 6))

    def test_errors_do_not_advance_state(self):
        code, out = self.run_tool(sha="0" * 64)
        self.assertEqual(code, 2)
        self.assertIn("fingerprint", out["error"])
        os.unlink(self.db)
        code, out = self.run_tool()
        self.assertEqual(code, 2)

    def test_missing_processed_message_is_reported(self):
        conn = sqlite3.connect(self.db)
        conn.execute("delete from messages where id = '1547100000000000003'")
        conn.commit(); conn.close()
        code, out = self.run_tool()
        self.assertEqual(code, 1)
        self.assertEqual(out["totals"]["processed_missing"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
