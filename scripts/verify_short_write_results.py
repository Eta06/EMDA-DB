#!/usr/bin/env python3
"""Independently check the recorded matrix and outcomes of a short-write run."""
import argparse
from collections import Counter
import errno
import gzip
import itertools
import json
from pathlib import Path


def expected_cases():
    cases = set()
    for fmt, operation, mode, order in itertools.product(
            ('v2', 'v3'), ('write', 'batch', 'checkpoint-write', 'checkpoint-batch'),
            ('success', 'error'), ('ordinary-first', 'checkpoint-first')):
        for cap in (1, 8, 25, 32):
            cases.add((fmt, operation, 'records.log', cap, mode, order))
        if fmt == 'v2':
            for cap in (1, 8, 24, 25, 32):
                cases.add((fmt, operation, 'commit.state', cap, mode, order))
    for fmt, mode, order, cap in itertools.product(
            ('v2', 'v3'), ('success', 'error'),
            ('ordinary-first', 'checkpoint-first'), (7, 17)):
        cases.add((fmt, 'concurrent-group', 'records.log', cap, mode, order))
    return cases


def receipts_count(receipts, mode):
    workers = receipts.get('workers')
    if not isinstance(workers, list) or len(workers) != 4:
        raise ValueError('invalid group worker count')
    acknowledged = 0
    failed = 0
    for index, worker in enumerate(workers):
        written = worker.get('acknowledged')
        failure = worker.get('failed')
        if (worker.get('id') != index or not isinstance(written, list)
                or written != list(range(len(written))) or len(written) > 64
                or (failure is not None and (failure != len(written) or failure >= 64))
                or ((failure is None) != (worker.get('error') is None))
                or (failure is None and len(written) != 64)):
            raise ValueError('invalid group receipt')
        acknowledged += len(written)
        failed += failure is not None
    if (mode == 'success' and (failed or acknowledged != 256)) or (mode == 'error' and not failed):
        raise ValueError('group receipts disagree with the requested fault')
    return acknowledged


def check_backend_results(results, backends, acknowledged):
    if not isinstance(results, list) or len(results) != 2:
        raise ValueError('missing group recovery results')
    for result, backend in zip(results, backends):
        if (result.get('status') != 'verified' or result.get('mode') != 'single'
                or result.get('backend') != backend or result.get('acknowledged') != acknowledged):
            raise ValueError('invalid group recovery result')
    if results[0].get('uncertain_present') != results[1].get('uncertain_present'):
        raise ValueError('group recovery paths disagree on uncertain writes')


def verify_case(case):
    fmt, operation, target, cap, mode, order = (case[name] for name in
        ('format', 'operation', 'target', 'cap', 'mode', 'order'))
    backends = ['ordinary', 'checkpoint'] if order == 'ordinary-first' else ['checkpoint', 'ordinary']
    call = 'writev' if operation == 'concurrent-group' else 'pwrite' if target == 'commit.state' else 'write'
    events = case.get('events')
    if not isinstance(events, list) or not events:
        raise ValueError('missing short-write events')
    for event in events:
        requested, result, error = (event.get(name) for name in ('requested', 'result', 'errno'))
        if (event.get('call') != call or not all(isinstance(n, int) for n in (requested, result, error))
                or requested <= 0 or result > requested or (result >= 0 and error != 0)):
            raise ValueError('invalid intercepted event')
    if not any(event['requested'] > cap and event['result'] == cap for event in events):
        raise ValueError('missing successful short write')
    if mode == 'success':
        if any(event['result'] < 0 for event in events):
            raise ValueError('unexpected successful-case error')
    elif (events[-1]['result'] != -1 or events[-1]['errno'] != errno.EIO
          or any(event['result'] < 0 for event in events[:-1])):
        raise ValueError('missing EIO after the short write')

    before, after, final = (case[name] for name in ('before', 'after', 'after_verification'))
    for field in ('backup_sha256', 'checkpoint_sha256', 'checkpoint_digest_sha256', 'backup_bytes'):
        if before[field] != after[field] or before[field] != final[field]:
            raise ValueError('reference fixture changed')
    if fmt == 'v3' and (before['witness_sha256'] != after['witness_sha256']
                        or before['witness_sha256'] != final['witness_sha256']):
        raise ValueError('V3 identity anchor changed')
    verification = case['verification']
    recovery = case['recovery']
    if recovery == 'rejected':
        if (verification.get('status') != 'rejected' or not verification.get('backup_verified')
                or verification.get('recovery_order') != backends or after != final):
            raise ValueError('rejection did not preserve the recorded evidence')
    elif recovery in ('reopened', 'reopened-uncertain'):
        if operation != 'concurrent-group' and (verification.get('status') != 'verified'
                or verification.get('recovery_order') != backends or not verification.get('new_present')):
            raise ValueError('recovery verification failed')
    else:
        raise ValueError('unknown recovery outcome')

    if operation == 'concurrent-group':
        acknowledged = receipts_count(case['receipts'], mode)
        results = verification if mode == 'success' else verification.get('known_tail_copy')
        if recovery != ('reopened' if mode == 'success' else 'rejected'):
            raise ValueError('incorrect group recovery')
        check_backend_results(results, backends, acknowledged)
        return acknowledged

    if case['outcome'].get('status') != mode or case['outcome'].get('operation') != operation:
        raise ValueError('action outcome disagrees with its case')
    if mode == 'success' and recovery != 'reopened':
        raise ValueError('successful write was not recovered')
    if mode == 'error' and target == 'records.log':
        if (recovery != 'rejected' or after['log_bytes'] != before['log_bytes'] + cap
                or after['witness_sha256'] != before['witness_sha256']
                or after['metadata_sha256'] != before['metadata_sha256']):
            raise ValueError('partial log failure did not preserve the witness')
    if mode == 'error' and target == 'commit.state':
        unchanged = after['witness_sha256'] == before['witness_sha256']
        if recovery != ('reopened-uncertain' if unchanged else 'rejected'):
            raise ValueError('incorrect witness failure outcome')
    return 0


def verify(report):
    if not report.get('completed') or report.get('failure') is not None or report.get('current') is not None:
        raise ValueError('campaign did not complete successfully')
    cases = report.get('cases')
    if not isinstance(cases, list):
        raise ValueError('missing cases')
    seen = set()
    outcomes = Counter()
    group_acknowledged = Counter()
    for case in cases:
        key = tuple(case[name] for name in ('format', 'operation', 'target', 'cap', 'mode', 'order'))
        if key in seen or key not in expected_cases():
            raise ValueError('duplicate or unexpected case')
        seen.add(key)
        count = verify_case(case)
        outcomes[(case['target'], case['mode'], case['recovery'])] += 1
        if case['operation'] == 'concurrent-group':
            group_acknowledged[case['mode']] += count
    if seen != expected_cases():
        raise ValueError('incomplete case matrix')
    return {'cases': len(seen), 'outcomes': {'/'.join(k): v for k, v in outcomes.items()},
            'group_acknowledgements': dict(group_acknowledged)}


def load_report(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', encoding='utf-8') as file:
        return json.load(file)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(load_report(args.report)), sort_keys=True))


if __name__ == '__main__':
    main()
