#!/usr/bin/python3
"""Offline validator for the EasyCop «Weekly Releases» JSON (6 fields EN/IT/NL).

Verification CLI only: no network, no Discord client, no job/scheduler, no
writes except one temporary sanitized copy passed to the generic Inbox
checker. It exists because the generic `dustify_radar.py check-draft` rejects
every `<#ID>` reference, which the weekly format REQUIRES (proved: exit 1,
"menzione Discord" / "id numerico tipo Discord").

Flow:
  1. strict shape: title/description/image_mode, exactly 6 fields Mon..Sat,
     EN/IT/NL day names, DD/MM dates equal to --week-start + n days (the year
     comes ONLY from the explicit --week-start, never inferred from DD/MM),
     inline false, every value ends with a newline, lines are "- item" or ✖️;
  2. every `<#ID>` must be a public EasyCop text/news channel currently under
     one of the INFO categories in the catalog, never under ARCHIVE / PAST
     RELEASES; other mentions/pings are refused;
  3. the generic Inbox checker runs read-only as a subprocess on a temporary
     copy where ONLY the already validated `<#ID>` are replaced by a neutral
     placeholder, so its link/tracking/competitor/source rules still apply.

Catalog: --catalog JSON (list of {id,guild_id,parent_id,kind,name,
is_private_thread}) or --db SQLITE (opened mode=ro; selective memory or archive).
Exit: 0 ok, 1 problems, 2 usage/catalog error.
"""
import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
from datetime import date, timedelta

DEFAULT_INBOX_CHECKER = "/home/hermes/.hermes/profiles/inbox/scripts/dustify_radar.py"
EASYCOP_GUILD_ID = "422855027535642625"
DEFAULT_INFO_CATEGORIES = ("1465792933512089782", "1465791988799504687", "1466013707363684488")
TITLE = "Weekly Releases"
DESCRIPTION = "**This Week Releases:**"
DAYS = ("Monday / Lunedì / Maandag", "Tuesday / Martedì / Dinsdag",
        "Wednesday / Mercoledì / Woensdag", "Thursday / Giovedì / Donderdag",
        "Friday / Venerdì / Vrijdag", "Saturday / Sabato / Zaterdag")
EMPTY_DAY = "✖️"
NAME_RE = re.compile(r"^\*\*```(?P<day>.+) - (?P<dd>\d{2})/(?P<mm>\d{2})```\*\*$")
CHANNEL_REF_RE = re.compile(r"<#(\d{17,20})>")
OTHER_MENTION_RE = re.compile(r"<(@[!&]?|@&|a?:\w+:|/)[^>]*>|<@\d+>|@everyone|@here")
EXCLUDED_CATEGORY_RE = re.compile(r"archive|past\s*releases", re.I)
PLACEHOLDER = "#easycop-channel"
TEXT_KINDS = {"text", "news", "0", "5", "announcement"}


class UsageError(Exception):
    pass


def load_catalog(args):
    if bool(args.catalog) == bool(args.db):
        raise UsageError("exactly one of --catalog or --db is required")
    rows = []
    if args.catalog:
        with open(args.catalog, encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, list):
            raise UsageError("catalog JSON must be a list of channels")
        rows = data
    else:
        if os.path.islink(args.db) or not os.path.isfile(args.db):
            raise UsageError("--db must be an existing regular file")
        conn = sqlite3.connect("file:%s?mode=ro" % args.db, uri=True)
        try:
            conn.execute("pragma query_only = 1")
            for cid, gid, parent, kind, name, priv in conn.execute(
                    "select id, guild_id, coalesce(parent_id,''), kind, name, is_private_thread from channels"):
                rows.append({"id": cid, "guild_id": gid, "parent_id": parent, "kind": kind,
                             "name": name, "is_private_thread": bool(priv)})
        finally:
            conn.close()
    return {str(r["id"]): r for r in rows}


def check_ref(cid, catalog, guild_id, info_categories):
    ch = catalog.get(cid)
    if ch is None:
        return "riferimento <#%s> assente dal catalogo EasyCop" % cid
    if str(ch.get("guild_id")) != guild_id:
        return "riferimento <#%s> non appartiene alla guild EasyCop" % cid
    if ch.get("is_private_thread"):
        return "riferimento <#%s> è un thread privato" % cid
    if str(ch.get("kind")) not in TEXT_KINDS:
        return "riferimento <#%s> non è un canale testo/news (%s)" % (cid, ch.get("kind"))
    seen = set()
    cur = str(ch.get("parent_id") or "")
    while cur and cur not in seen:
        seen.add(cur)
        parent = catalog.get(cur)
        if parent is None:
            break
        if EXCLUDED_CATEGORY_RE.search(str(parent.get("name", ""))):
            return "riferimento <#%s> escluso (categoria %s)" % (cid, parent.get("name"))
        if cur in info_categories:
            return None
        cur = str(parent.get("parent_id") or "")
    return "riferimento <#%s> non è sotto una categoria INFO attiva" % cid


def validate(doc, week_start, catalog, guild_id, info_categories):
    problems, refs = [], []
    if not isinstance(doc, dict):
        return ["il JSON deve essere un oggetto"], refs
    expected_keys = {"title", "description", "fields", "image_mode"}
    if set(doc) != expected_keys:
        problems.append("chiavi attese %s, trovate %s" % (sorted(expected_keys), sorted(doc)))
    if doc.get("title") != TITLE:
        problems.append("title diverso da %r" % TITLE)
    if doc.get("description") != DESCRIPTION:
        problems.append("description diversa da %r" % DESCRIPTION)
    if doc.get("image_mode") != "manual":
        problems.append("image_mode deve essere \"manual\"")
    fields = doc.get("fields")
    if not isinstance(fields, list) or len(fields) != 6:
        problems.append("servono esattamente 6 field (lunedì-sabato)")
        return problems, refs
    for i, field in enumerate(fields):
        day = DAYS[i]
        if not isinstance(field, dict) or set(field) != {"name", "value", "inline"}:
            problems.append("field %d: chiavi attese name/value/inline" % (i + 1))
            continue
        if field["inline"] is not False:
            problems.append("field %d: inline deve essere false" % (i + 1))
        m = NAME_RE.match(str(field["name"]))
        if not m or m.group("day") != day:
            problems.append("field %d: nome atteso **```%s - DD/MM```**" % (i + 1, day))
        else:
            expected = week_start + timedelta(days=i)
            if (int(m.group("dd")), int(m.group("mm"))) != (expected.day, expected.month):
                problems.append("field %d: data %s/%s incoerente, attesa %02d/%02d"
                                % (i + 1, m.group("dd"), m.group("mm"), expected.day, expected.month))
        value = field["value"]
        if not isinstance(value, str) or not value.endswith("\n") or value.endswith("\n\n"):
            problems.append("field %d: il valore deve terminare con un solo \\n" % (i + 1))
            continue
        lines = value[:-1].split("\n")
        if lines == [EMPTY_DAY]:
            continue
        for line in lines:
            if not line.startswith("- ") or len(line.strip()) < 3:
                problems.append("field %d: riga non nel formato \"- item\": %r" % (i + 1, line))
            if EMPTY_DAY in line:
                problems.append("field %d: ✖️ mescolato a item" % (i + 1))
        if OTHER_MENTION_RE.search(value):
            problems.append("field %d: ping/menzione non ammessa" % (i + 1))
        for cid in CHANNEL_REF_RE.findall(value):
            refs.append(cid)
            why = check_ref(cid, catalog, guild_id, info_categories)
            if why:
                problems.append("field %d: %s" % (i + 1, why))
    for key in ("title", "description"):
        if isinstance(doc.get(key), str) and (CHANNEL_REF_RE.search(doc[key]) or OTHER_MENTION_RE.search(doc[key])):
            problems.append("%s: menzioni non ammesse" % key)
    return problems, refs


def run_inbox_checker(doc, validated_refs, checker, python):
    text = json.dumps(doc, ensure_ascii=False, indent=2)
    for cid in sorted(set(validated_refs)):
        text = text.replace("<#%s>" % cid, PLACEHOLDER)
    fd, path = tempfile.mkstemp(prefix="weekly-sanitized-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        proc = subprocess.run([python, checker, "check-draft", path], capture_output=True,
                              text=True, timeout=60, stdin=subprocess.DEVNULL)
    finally:
        os.unlink(path)
    try:
        payload = json.loads(proc.stdout.strip().splitlines()[-1]) if proc.stdout.strip() else {}
    except ValueError:
        payload = {"raw_stdout": proc.stdout[-500:]}
    return {"argv": [python, checker, "check-draft", "<sanitized-temp>"], "exit": proc.returncode,
            "result": payload, "stderr_tail": proc.stderr[-300:]}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("file")
    ap.add_argument("--week-start", required=True, help="Monday, YYYY-MM-DD (explicit year)")
    ap.add_argument("--catalog")
    ap.add_argument("--db")
    ap.add_argument("--guild-id", default=EASYCOP_GUILD_ID)
    ap.add_argument("--info-category", action="append", dest="info_categories")
    ap.add_argument("--inbox-checker", default=DEFAULT_INBOX_CHECKER)
    ap.add_argument("--python", default=sys.executable)
    ap.add_argument("--no-inbox", action="store_true", help="skip the generic checker (reported)")
    args = ap.parse_args(argv)
    try:
        try:
            week_start = date.fromisoformat(args.week_start)
        except ValueError:
            raise UsageError("--week-start must be YYYY-MM-DD")
        if week_start.weekday() != 0:
            raise UsageError("--week-start must be a Monday")
        catalog = load_catalog(args)
        with open(args.file, encoding="utf-8") as fh:
            doc = json.load(fh)
    except (UsageError, OSError, ValueError, sqlite3.Error) as exc:
        print(json.dumps({"ok": False, "usage_error": str(exc)}, ensure_ascii=False))
        return 2
    info = set(args.info_categories or DEFAULT_INFO_CATEGORIES)
    problems, refs = validate(doc, week_start, catalog, args.guild_id, info)
    report = {"ok": False, "problems": problems, "channel_refs_checked": len(refs)}
    if args.no_inbox:
        report["inbox_checker"] = "skipped (--no-inbox)"
    elif not problems:
        inbox = run_inbox_checker(doc, refs, args.inbox_checker, args.python)
        report["inbox_checker"] = inbox
        if inbox["exit"] != 0:
            problems.extend("inbox: %s" % p for p in inbox["result"].get("problems", ["exit %d" % inbox["exit"]]))
    else:
        report["inbox_checker"] = "not run: typed validation already failed"
    report["ok"] = not problems
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
