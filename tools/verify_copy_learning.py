#!/usr/bin/python3
"""Verification-only harness for wf-easycop-copy-learning on a selective memory.

NOT a production runner and NOT a substitute for the agent workflow: it never
writes state.json, comparisons, examples, proposals, signals or last-run.json.
It recomputes, read-only, exactly the fingerprint documented in step 3 of the
skill, for the registered channels from `since`, and reports counts.

The skill file is pinned by EXACT sha256 (--skill-sha256); any other version
is refused, so a changed procedure cannot be "verified" by a stale harness.
Exit: 0 ok and zero novelties, 1 novelties (new/changed/missing), 2 error.
"""
import argparse
import hashlib
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone


def fingerprint(content, embeds):
    # Verbatim formula from the skill (step 3).
    return hashlib.sha256(json.dumps({'content': content, 'embeds': embeds}, ensure_ascii=False,
                                     sort_keys=True, separators=(',', ':')).encode('utf-8')).hexdigest()


def sha_file(path):
    with open(path, 'rb') as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def parse_utc(value):
    if not (value.endswith('Z') or value.endswith('+00:00')):
        raise ValueError('since must be UTC: %r' % value)
    return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)


def run(args):
    if sha_file(args.skill) != args.skill_sha256:
        raise RuntimeError('skill fingerprint mismatch: harness pinned to %s' % args.skill_sha256)
    state_before = sha_file(args.state)
    with open(args.registry, encoding='utf-8') as fh:
        registry = json.load(fh)
    with open(args.state, encoding='utf-8') as fh:
        state = json.load(fh)
    processed = state.get('processed')
    if not isinstance(processed, dict):
        raise RuntimeError('state.json has no processed map')
    if os.path.islink(args.db) or not os.path.isfile(args.db):
        raise RuntimeError('--db must be an existing regular file')
    conn = sqlite3.connect('file:%s?mode=ro' % args.db, uri=True)
    conn.execute('pragma query_only = 1')
    try:
        last_success = conn.execute("select cursor from sync_state where scope = 'sync:last_success'").fetchone()
        marker = conn.execute("select cursor from sync_state where scope = 'memory:selective'").fetchone()
        report = {'skill_sha256': args.skill_sha256, 'db': args.db, 'selective_memory': marker is not None,
                  'sync_last_success': last_success[0] if last_success else None, 'releases': [],
                  'totals': {'unchanged': 0, 'changed': 0, 'new': 0, 'processed_missing': 0,
                             'drafts_declared': 0, 'drafts_reachable': 0}}
        for rel in registry.get('releases', []):
            since = parse_utc(rel['since'])
            rows = conn.execute('select id, content, raw_json, created_at from messages where channel_id = ? and guild_id = ?',
                                (rel['channel_id'], rel['guild_id'])).fetchall()
            ids_seen, counts = set(), {'unchanged': 0, 'changed': 0, 'new': 0}
            for mid, content, raw, created in rows:
                created_at = datetime.fromisoformat(created.replace('Z', '+00:00')).astimezone(timezone.utc)
                if created_at < since and mid not in processed:
                    continue
                ids_seen.add(mid)
                embeds = (json.loads(raw) if raw else {}).get('embeds') or []
                fp = fingerprint(content, embeds)
                if mid not in processed:
                    counts['new'] += 1
                elif processed[mid] == fp:
                    counts['unchanged'] += 1
                else:
                    counts['changed'] += 1
            declared = rel.get('draft_files') or []
            reachable = [p for p in declared if os.path.isfile(p)]
            entry = dict(slug=rel.get('slug'), messages_considered=len(ids_seen), drafts_declared=len(declared),
                         drafts_reachable=len(reachable), **counts)
            report['releases'].append(entry)
            for k in counts:
                report['totals'][k] += counts[k]
            report['totals']['drafts_declared'] += len(declared)
            report['totals']['drafts_reachable'] += len(reachable)
        all_ids = {r[0] for r in conn.execute('select id from messages')}
        report['totals']['processed_missing'] = len(set(processed) - all_ids)
    finally:
        conn.close()
    report['state_sha256_before'] = state_before
    report['state_sha256_after'] = sha_file(args.state)
    report['signals_emitted'] = 0  # verification-only: never emits signals
    if report['state_sha256_after'] != state_before:
        raise RuntimeError('state.json changed during verification')
    t = report['totals']
    report['novelties'] = t['changed'] + t['new'] + t['processed_missing']
    return report


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    for name in ('--db', '--registry', '--state', '--skill', '--skill-sha256'):
        ap.add_argument(name, required=True)
    args = ap.parse_args(argv)
    try:
        report = run(args)
    except (RuntimeError, OSError, ValueError, KeyError, sqlite3.Error) as exc:
        print(json.dumps({'ok': False, 'error': str(exc)}))
        return 2
    report['ok'] = report['novelties'] == 0
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    sys.exit(main())
