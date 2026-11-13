#!/usr/bin/env python3
"""Validate a completed interrupted-sync report's case matrix and outcomes."""
import argparse
import errno
import itertools
import json
from pathlib import Path


def expected_cases():
    return {(fmt, target, call, operation, mode, order)
            for (fmt, target, call), operation, mode, order in itertools.product(
                (('v2', 'records.log', 'fdatasync'), ('v2', 'commit.state', 'fdatasync'),
                 ('v3', 'records.log', 'fsync')),
                ('write', 'batch', 'checkpoint-write', 'checkpoint-batch'),
                ('success', 'error'), ('ordinary-first', 'checkpoint-first'))}


def verify_case(case):
    fmt, target, call, operation, mode, order = (case[name] for name in
        ('format', 'target', 'syscall', 'operation', 'mode', 'order'))
    events = [{'call': call, 'number': 1, 'actual': 0, 'reported': -1, 'errno': errno.EINTR},
              {'call': call, 'number': 2, 'actual': -2 if mode == 'error' else 0,
               'reported': -1 if mode == 'error' else 0,
               'errno': errno.EIO if mode == 'error' else 0}]
    if case['events'] != events:
        raise ValueError('missing completed sync, interrupted return or expected retry')
    outcome = case['outcome']
    if outcome.get('status') != mode or outcome.get('operation') != operation:
        raise ValueError('incorrect operation outcome')
    if mode == 'error' and (outcome.get('read_blocked') is not True or outcome.get('write_blocked') is not True):
        raise ValueError('uncertain handle did not block further access')
    backends = ['ordinary', 'checkpoint'] if order == 'ordinary-first' else ['checkpoint', 'ordinary']
    checked = case['verification']
    if (checked.get('status') != 'verified' or checked.get('operation') != operation
            or checked.get('new_present') is not True or checked.get('recovery_order') != backends):
        raise ValueError('incomplete recovery verification')
    before, after, final = (case[name] for name in ('before', 'after', 'after_verification'))
    for field in ('backup_sha256', 'checkpoint_sha256', 'checkpoint_digest_sha256', 'backup_bytes'):
        if before[field] != after[field] or before[field] != final[field]:
            raise ValueError('reference fixture changed')
    if fmt == 'v3' and (before['witness_sha256'] != after['witness_sha256']
                        or before['witness_sha256'] != final['witness_sha256']):
        raise ValueError('V3 identity anchor changed')
    if after['log_bytes'] <= before['log_bytes'] or final['log_bytes'] != after['log_bytes']:
        raise ValueError('unexpected log length')


def verify(report):
    if not report.get('completed') or report.get('failure') is not None or report.get('current') is not None:
        raise ValueError('campaign did not complete successfully')
    cases = report.get('cases')
    if not isinstance(cases, list):
        raise ValueError('missing cases')
    expected = expected_cases()
    seen = set()
    for case in cases:
        key = tuple(case[name] for name in ('format', 'target', 'syscall', 'operation', 'mode', 'order'))
        if key not in expected or key in seen:
            raise ValueError('unexpected or duplicate case')
        seen.add(key)
        verify_case(case)
    if seen != expected:
        raise ValueError('incomplete case matrix')
    return {'cases': len(seen), 'successes': sum(case['mode'] == 'success' for case in cases),
            'uncertain_failures': sum(case['mode'] == 'error' for case in cases)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(json.loads(args.report.read_text())), sort_keys=True))


if __name__ == '__main__':
    main()
