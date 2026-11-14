#!/usr/bin/env python3
"""Verify detailed reclamation fault coverage and retained evidence."""
import argparse
from collections import Counter
import gzip
import hashlib
import itertools
import json
from pathlib import Path, PurePosixPath
import re

from reclamation_eio_campaign import EXTERNAL, FORMATS, ORDERS, check_outcome
from sync_eio_campaign import SYSCALL_SETS, reports_injected_error


def require(condition, message):
    if not condition:
        raise ValueError(message)


def valid_digest(value):
    return isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value)


def check_store(value, fmt=None, *, complete):
    require(isinstance(value, dict) and set(value) == {'records.log', 'commit.state', 'store.lock', 'v3_metadata', 'format'},
            'invalid store evidence')
    for key in ('records.log', 'commit.state', 'store.lock'):
        require(valid_digest(value[key]) if complete else value[key] is None or valid_digest(value[key]),
                'invalid store file digest')
    actual_format, meta = value['format'], value['v3_metadata']
    require((actual_format in FORMATS if value['records.log'] is not None else actual_format is None)
            and (fmt is None or actual_format == fmt), 'invalid or unexpected log format')
    require(valid_digest(meta) if actual_format == 'v3' else meta is None, 'invalid metadata digest')


def check_probe(probe, fmt, order, syscall=None, error_name=None):
    outcome, verified = probe['outcome'], probe['verification']
    require(outcome['operation'] == 'reclaim' and outcome['status'] in ('success', 'error'), 'unexpected reclamation outcome')
    require(verified['status'] == 'verified' and verified['order'] == order and verified['backup_restored'] is True,
            'missing source or backup verification')
    original, source, previous = probe['original'], probe['source_after'], probe['previous_after']
    check_store(original, fmt, complete=True)
    # Compaction can produce the current creation format even from a V2 source.
    check_store(source, complete=True)
    if previous is not None:
        check_store(previous, complete=outcome['classification'] in ('candidate_unknown', 'exchange_unknown'))
    check_outcome(outcome, verified, original, source, previous)
    external = probe['external']
    require(set(external) == set(EXTERNAL) and all(valid_digest(value) for value in external.values()), 'invalid external evidence')
    require(external['original-log.sha256'] == hashlib.sha256(bytes.fromhex(original['records.log'])).hexdigest(),
            'original log digest does not match reference')
    require(probe['source_after_verification'] == source and probe['previous_after_verification'] == previous
            and probe['external_after_verification'] == external and probe['stages_after_verification'] == probe['stages_after'],
            'verification changed retained evidence')
    for path, entry in probe['stages_after'].items():
        name = PurePosixPath(path)
        require(bool(name.parts) and not name.is_absolute() and '..' not in name.parts
                and name.parts[0].startswith('.emda-stage-v1-'),
                'invalid staged path')
        valid_file = (set(entry) in ({'type', 'sha256'}, {'type', 'sha256', 'v3_metadata'})
                      and entry['type'] == 'file' and valid_digest(entry['sha256'])
                      and ('v3_metadata' not in entry or valid_digest(entry['v3_metadata'])))
        require(entry == {'type': 'directory'} or valid_file, 'invalid staged evidence')
    traces = probe['injected_traces']
    require(len(traces) == int(syscall is not None), 'wrong number of recorded injections')
    if syscall is not None:
        require(re.search(r'\b' + re.escape(syscall) + r'\(', traces[0])
                and reports_injected_error(traces[0], error_name), 'recorded syscall or errno differs')


def verify(result):
    require(result['completed'] is True and result['failure'] is None and result['current'] is None,
            'campaign is incomplete or failed')
    group = result['syscall_set']
    require(group in SYSCALL_SETS and result['fixture_paths_only'] == (group != 'sync'), 'invalid syscall group or path mode')
    error_name, formats = result['injected_error'], result['formats']
    require(error_name in ('EIO', 'EAGAIN'), 'unknown injected errno')
    require(formats and len(formats) == len(set(formats)) and all(fmt in FORMATS for fmt in formats), 'invalid formats')
    cases = result['cases']
    require(Counter(case['format'] for case in cases) == Counter(formats), 'missing or duplicate reclamation control')
    limit = result['max_ordinals']
    require(type(limit) is int and 0 <= limit <= 100, 'invalid ordinal limit')
    outcomes, total = Counter(), 0
    for case in cases:
        fmt, control = case['format'], case['control']
        check_probe(control, fmt, 'source-first')
        require(control['outcome']['status'] == 'success', 'reclamation control did not succeed')
        expected = []
        for syscall in SYSCALL_SETS[group]:
            ordinals = control['eligible_ordinals'][syscall]
            require(ordinals == sorted(set(ordinals)) and all(type(n) is int and 1 <= n <= control['counts'][syscall] for n in ordinals),
                    'invalid selected ordinal')
            if group == 'sync':
                require(ordinals == list(range(1, control['counts'][syscall] + 1)), 'sync control omitted an observed call')
            expected.extend((syscall, ordinal, order) for ordinal in (ordinals[:limit] if limit else ordinals) for order in ORDERS)
        actual = [(item['syscall'], item['ordinal'], item['order']) for item in case['injections']]
        require(Counter(actual) == Counter(expected), 'missing or duplicate selected injection')
        for item in case['injections']:
            check_probe(item, fmt, item['order'], item['syscall'], error_name)
            outcomes[item['outcome']['classification'] or 'success'] += 1
            total += 1
    return {'controls': len(cases), 'injections': total, 'outcomes': dict(outcomes), 'errno': error_name}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    args = parser.parse_args()
    try:
        opener = gzip.open if args.report.suffix == '.gz' else open
        with opener(args.report, 'rt', encoding='utf-8') as file:
            summary = verify(json.load(file))
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise SystemExit(f'reclamation coverage verification failed: {error}') from error
    print(json.dumps(summary, sort_keys=True))


if __name__ == '__main__':
    main()
