"""Offline tests for tools/check_weekly.py. SYNTHETIC data only, no network.

The named fixture testdata/weekly-synthetic-regression.json is a byte copy of
the owner's regression fixture (sha256 4fc7a67c...416c8b). The generic Inbox
checker is executed READ-ONLY as a subprocess; it is never modified.
"""
import copy
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
TOOL = HERE / "check_weekly.py"
FIXTURE = HERE / "testdata" / "weekly-synthetic-regression.json"
INBOX = os.environ.get("INBOX_CHECKER", "/home/hermes/.hermes/profiles/inbox/scripts/dustify_radar.py")
EC = "422855027535642625"
DU = "1347333602794541066"
INFO = "1465792933512089782"
ROOM = "1547163155417927770"      # referenced by the owner fixture
ARCHIVED_ROOM = "1547163155417927771"
PAST_ROOM = "1547163155417927772"
DUST_ROOM = "1547163155417927773"
CATALOG = [
    {"id": INFO, "guild_id": EC, "parent_id": "", "kind": "category", "name": "TCG INFO"},
    {"id": "1465000000000000001", "guild_id": EC, "parent_id": "", "kind": "category", "name": "ARCHIVE"},
    {"id": "1465000000000000002", "guild_id": EC, "parent_id": "", "kind": "category", "name": "PAST RELEASES"},
    {"id": ROOM, "guild_id": EC, "parent_id": INFO, "kind": "text", "name": "synthetic-item"},
    {"id": ARCHIVED_ROOM, "guild_id": EC, "parent_id": "1465000000000000001", "kind": "text", "name": "old"},
    {"id": PAST_ROOM, "guild_id": EC, "parent_id": "1465000000000000002", "kind": "text", "name": "past"},
    {"id": DUST_ROOM, "guild_id": DU, "parent_id": "", "kind": "text", "name": "other-guild"},
]


@unittest.skipUnless(os.path.isfile(INBOX), "Inbox checker not available")
class WeeklyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.catalog = self.dir / "catalog.json"
        self.catalog.write_text(json.dumps(CATALOG))
        self.doc = json.loads(FIXTURE.read_text())

    def tearDown(self):
        self.tmp.cleanup()

    def run_tool(self, doc=None, extra=(), week="2026-10-19"):
        path = self.dir / "draft.json"
        path.write_text(json.dumps(doc if doc is not None else self.doc, ensure_ascii=False))
        proc = subprocess.run([sys.executable, str(TOOL), str(path), "--week-start", week,
                               "--catalog", str(self.catalog), "--inbox-checker", INBOX] + list(extra),
                              capture_output=True, text=True)
        out = json.loads(proc.stdout) if proc.stdout.strip() else {}
        return proc.returncode, out

    def test_red_old_generic_checker_on_named_fixture(self):
        proc = subprocess.run([sys.executable, INBOX, "check-draft", str(FIXTURE)], capture_output=True, text=True)
        self.assertEqual(proc.returncode, 1)
        self.assertIn("menzione Discord", proc.stdout)

    def test_green_new_validator_on_named_fixture(self):
        code, out = self.run_tool()
        self.assertEqual(code, 0, out)
        self.assertTrue(out["ok"])
        self.assertEqual(out["channel_refs_checked"], 1)
        self.assertEqual(out["inbox_checker"]["exit"], 0)

    def test_green_with_readonly_sqlite_catalog(self):
        db = self.dir / "catalog.db"
        conn = sqlite3.connect(db)
        conn.execute("create table channels (id text primary key, guild_id text, parent_id text, kind text, name text, is_private_thread integer default 0)")
        for c in CATALOG:
            conn.execute("insert into channels(id, guild_id, parent_id, kind, name) values(?,?,?,?,?)",
                         (c["id"], c["guild_id"], c["parent_id"], c["kind"], c["name"]))
        conn.commit(); conn.close()
        before = db.read_bytes()
        path = self.dir / "draft.json"
        path.write_text(json.dumps(self.doc, ensure_ascii=False))
        proc = subprocess.run([sys.executable, str(TOOL), str(path), "--week-start", "2026-10-19",
                               "--db", str(db), "--inbox-checker", INBOX], capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(db.read_bytes(), before)

    def mutate(self, fn):
        doc = copy.deepcopy(self.doc)
        fn(doc)
        return doc

    def assert_red(self, doc, needle, week="2026-10-19"):
        code, out = self.run_tool(doc, week=week)
        self.assertEqual(code, 1, out)
        self.assertTrue(any(needle in p for p in out["problems"]), out)

    def set_value(self, value, day=0):
        return self.mutate(lambda d: d["fields"][day].__setitem__("value", value))

    def test_red_cases(self):
        self.assert_red(self.set_value("- Old <#%s>\n" % ARCHIVED_ROOM), "escluso")
        self.assert_red(self.set_value("- Past <#%s>\n" % PAST_ROOM), "escluso")
        self.assert_red(self.set_value("- Unknown <#1547999999999999999>\n"), "assente")
        self.assert_red(self.set_value("- Cross <#%s>\n" % DUST_ROOM), "guild")
        self.assert_red(self.set_value("- Ping @everyone\n"), "ping")
        self.assert_red(self.set_value("- Role <@&1547163155417927770>\n"), "ping")
        self.assert_red(self.set_value("- Shop [x](https://shop.example.com/p/1?utm_source=dc)\n"), "inbox: link non ripulito")
        self.assert_red(self.set_value("- Competitor via dustify\n"), "inbox: nome della fonte")
        self.assert_red(self.set_value("- Link https://discord.com/channels/1/2\n"), "inbox")
        self.assert_red(self.mutate(lambda d: d["fields"][1].__setitem__("name", "**```Tuesday / Martedì / Dinsdag - 21/10```**")), "incoerente")
        self.assert_red(self.mutate(lambda d: d["fields"].reverse()), "nome atteso")
        self.assert_red(self.mutate(lambda d: d["fields"].pop()), "6 field")
        self.assert_red(self.mutate(lambda d: d["fields"][2].__setitem__("inline", True)), "inline")
        self.assert_red(self.set_value("- No newline <#%s>" % ROOM), "\\n")
        self.assert_red(self.set_value("item without dash\n"), "formato")
        self.assert_red(self.mutate(lambda d: d.__setitem__("image_mode", "auto")), "image_mode")
        self.assert_red(self.mutate(lambda d: d.__setitem__("title", "Weekly")), "title")
        self.assert_red(self.mutate(lambda d: d.__setitem__("extra", 1)), "chiavi")
        # Same DD/MM but explicit week of a different year: dates incoherent.
        self.assert_red(self.doc, "incoerente", week="2027-10-18")

    def test_usage_errors(self):
        code, out = self.run_tool(week="2026-10-20")
        self.assertEqual(code, 2)
        self.assertIn("Monday", out["usage_error"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
