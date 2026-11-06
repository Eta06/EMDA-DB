#!/usr/bin/env python3
"""Verify the observed-path coverage recorded by a paired-EIO campaign."""
import argparse
from collections import Counter
import gzip
import itertools
import json
from pathlib import Path
import re

SYNC = frozenset(('fsync', 'fdatasync'))
SECONDARY = frozenset(('fsync', 'fdatasync', 'unlink', 'unlinkat', 'rmdir'))
ORDERS = {'ordinary-first': ['ordinary', 'checkpoint'],
          'checkpoint-first': ['checkpoint', 'ordinary']}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def check_probe(probe, operation, order, injected):
    require(probe['outcome']['operation'] == operation
            and probe['outcome']['status'] in ('success', 'error'), 'unexpected probe outcome')
    verified = probe['verification']
    require(verified['status'] == 'verified' and verified['operation'] == operation
            and verified['recovery_order'] == ORDERS[order], 'missing recovery verification')
    traces = probe['injected_traces']
    require(len(traces) == len(injected), 'wrong number of recorded injected errors')
    for line, name in zip(traces, injected):
        require('(INJECTED)' in line and re.search(r'\b' + re.escape(name) + r'\(', line),
                'recorded injection syscall differs')


def verify(result):
    require(result['completed'] is True and result['failure'] is None and result['current'] is None,
            'campaign is incomplete or failed')
    formats, operations = result['formats'], result['operations']
    require(formats and operations and len(formats) == len(set(formats))
            and len(operations) == len(set(operations)), 'empty or duplicate campaign format or operation')
    expected_cases = Counter(itertools.product(formats, operations))
    cases = result['cases']
    require(Counter((case['format'], case['operation']) for case in cases) == expected_cases,
            'missing or duplicate format/operation control')
    primary_names = result['primary_syscalls']
    require(primary_names and len(primary_names) == len(set(primary_names)), 'empty or duplicate primary syscall')
    limit = result['max_primary']
    require(isinstance(limit, int) and limit >= 0, 'invalid primary limit')
    primary_count = paired_count = same_count = 0
    for case in cases:
        operation = case['operation']
        control = case['control']
        check_probe(control, operation, 'ordinary-first', [])
        require(control['outcome']['status'] == 'success', 'control did not succeed')
        expected_primary = []
        for name in primary_names:
            if result.get('all_sync_paths', False) and name in SYNC:
                ordinals = list(range(1, control['counts'][name] + 1))
            else:
                ordinals = control['eligible_ordinals'][name]
            require(ordinals == sorted(set(ordinals))
                    and all(isinstance(n, int) and 1 <= n <= control['counts'][name] for n in ordinals),
                    'invalid or duplicate selected primary ordinal')
            expected_primary.extend((name, n) for n in (ordinals[:limit] if limit else ordinals))
        checks = case['primary_checks']
        actual_primary = [(check['syscall'], check['ordinal']) for check in checks]
        require(Counter(actual_primary) == Counter(expected_primary), 'missing or duplicate primary injection')
        primary_count += len(checks)
        for check in checks:
            first = check['first']
            check_probe(first, operation, 'ordinary-first', [check['syscall']])
            later = first['calls_after_primary']
            require(set(later) == SECONDARY, 'unexpected secondary syscall set')
            for name, values in later.items():
                require(values == sorted(set(values))
                        and all(isinstance(n, int) and 1 <= n <= first['counts'][name]
                                and (name != check['syscall'] or n > check['ordinal']) for n in values),
                        'invalid or duplicate selected secondary ordinal')
            expected_pairs = [(name, n, order) for name, values in later.items()
                              for n in values for order in ORDERS]
            actual_pairs = [(item['syscall'], item['ordinal'], item['order']) for item in check['second']]
            require(Counter(actual_pairs) == Counter(expected_pairs), 'missing or duplicate secondary injection')
            for item in check['second']:
                check_probe(item, operation, item['order'], [check['syscall'], item['syscall']])
                paired_count += 1
                same_count += int(check['syscall'] == item['syscall'])
    return {'controls': len(cases), 'primary': primary_count, 'paired': paired_count,
            'same_syscall_pairs': same_count}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    args = parser.parse_args()
    try:
        opener = gzip.open if args.report.suffix == '.gz' else open
        with opener(args.report, 'rt', encoding='utf-8') as file:
            result = json.load(file)
        summary = verify(result)
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise SystemExit(f'coverage verification failed: {error}') from error
    print(json.dumps(summary, sort_keys=True))


if __name__ == '__main__':
    main()
