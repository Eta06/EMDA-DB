#!/usr/bin/env python3
"""Verify selected-call coverage and recorded compaction-handoff evidence."""
import argparse
from collections import Counter
import gzip
import hashlib
import itertools
import json
from pathlib import Path
import re

from handoff_eio_campaign import BACKENDS, FORMATS, ORDERS, check_outcome
from sync_eio_campaign import SYSCALL_SETS, reports_injected_error


def require(condition, message):
    if not condition:
        raise ValueError(message)


def check_probe(probe, backend, order, syscall=None, error_name=None):
    outcome = probe['outcome']
    verified = probe['verification']
    require(outcome['operation'] == backend and outcome['status'] in ('success', 'error'),
            'unexpected handoff outcome')
    require(verified['status'] == 'verified' and verified['operation'] == backend and verified['order'] == order,
            'missing handoff recovery verification')
    check_outcome(outcome, verified)
    original, source, target = probe['original'], probe['source_after'], probe['target_after']
    for state in (original, source, target):
        if state is None:
            continue
        for key in ('log_sha256', 'witness_sha256'):
            require(isinstance(state[key], str) and re.fullmatch(r'[0-9a-f]{64}', state[key]),
                    'invalid handoff data digest')
        for key in ('metadata_sha256', 'selection_sha256', 'automatic_owner_sha256'):
            require(state[key] is None or isinstance(state[key], str) and re.fullmatch(r'[0-9a-f]{64}', state[key]),
                    'invalid handoff metadata digest')
    require(verified['source_original'] == (source == original)
            and verified['target_original'] == (target == original)
            and verified['target_visible'] == (target is not None), 'original handoff evidence differs')
    require(probe['source_after_verification'] == source and probe['target_after_verification'] == target
            and probe['external_before'] == probe['external_after'] == probe['external_after_verification'],
            'reference backup or checkpoint changed')
    require(set(probe['stages_after_cleanup']) == {'root', 'source', 'target'}
            and not any(probe['stages_after_cleanup'].values()), 'handoff staging cleanup incomplete')
    for key in ('reference-backup_sha256', 'checkpoint_sha256', 'checkpoint.digest_sha256', 'original-log.sha256_sha256'):
        value = probe['external_before'][key]
        require(isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value), 'missing external fixture digest')
    require(probe['external_before']['original-log.sha256_sha256'] ==
            hashlib.sha256(bytes.fromhex(original['log_sha256'])).hexdigest(),
            'original log digest does not match the fixture reference')
    managed = probe['external_before']['managed-base_sha256']
    require((isinstance(managed, str) and re.fullmatch(r'[0-9a-f]{64}', managed)) if backend == 'managed' else managed is None,
            'invalid managed fixture digest')
    traces = probe['injected_traces']
    require(len(traces) == int(syscall is not None), 'wrong number of recorded injections')
    if syscall is not None:
        require(re.search(r'\b' + re.escape(syscall) + r'\(', traces[0])
                and reports_injected_error(traces[0], error_name), 'recorded injection syscall or errno differs')


def verify(result):
    require(result['completed'] is True and result['failure'] is None and result['current'] is None,
            'campaign is incomplete or failed')
    group = result['syscall_set']
    require(group in SYSCALL_SETS and result['fixture_paths_only'] == (group != 'sync'),
            'unknown syscall group or path-selection mode')
    error_name = result['injected_error']
    require(error_name in ('EIO', 'EAGAIN'), 'unknown injected errno')
    formats, backends = result['formats'], result['backends']
    require(formats and backends and len(formats) == len(set(formats)) and len(backends) == len(set(backends))
            and all(fmt in FORMATS for fmt in formats) and all(backend in BACKENDS for backend in backends),
            'invalid or duplicate fixture format or backend')
    cases = result['cases']
    require(Counter((case['format'], case['backend']) for case in cases) == Counter(itertools.product(formats, backends)),
            'missing or duplicate handoff control')
    limit = result['max_ordinals']
    require(type(limit) is int and limit >= 0, 'invalid ordinal limit')
    outcomes = Counter()
    total = 0
    for case in cases:
        backend, control = case['backend'], case['control']
        check_probe(control, backend, 'source-first')
        require(control['outcome']['status'] == 'success', 'handoff control did not succeed')
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
            check_probe(item, backend, item['order'], item['syscall'], error_name)
            outcomes[item['outcome']['status']] += 1
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
        raise SystemExit(f'handoff coverage verification failed: {error}') from error
    print(json.dumps(summary, sort_keys=True))


if __name__ == '__main__':
    main()
