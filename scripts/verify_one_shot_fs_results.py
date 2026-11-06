#!/usr/bin/env python3
"""Verify recorded observed-path coverage from a one-shot filesystem campaign."""
import argparse
from collections import Counter
import gzip
import itertools
import json
from pathlib import Path
import re

from sync_eio_campaign import RECOVERY_ORDERS, SYSCALL_SETS

ORDERS = {'ordinary-first': ['ordinary', 'checkpoint'],
          'checkpoint-first': ['checkpoint', 'ordinary']}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def check_probe(probe, operation, order, syscall=None, error_name=None):
    require(probe['outcome']['operation'] == operation
            and probe['outcome']['status'] in ('success', 'error'), 'unexpected probe outcome')
    verified = probe['verification']
    require(verified['status'] == 'verified' and verified['operation'] == operation
            and verified['recovery_order'] == ORDERS[order], 'missing recovery verification')
    traces = probe['injected_traces']
    require(len(traces) == int(syscall is not None), 'wrong number of recorded injections')
    if syscall is not None:
        line = traces[0]
        require('(INJECTED)' in line and re.search(r'\b' + re.escape(syscall) + r'\(', line)
                and re.search(r'=\s*-1\s+' + re.escape(error_name) + r'\b', line),
                'recorded injection syscall or errno differs')


def verify(result):
    require(result['completed'] is True and result['failure'] is None and result['current'] is None,
            'campaign is incomplete or failed')
    require(result['injection_mode'] == 'one-shot', 'only one-shot campaigns are supported')
    group = result['syscall_set']
    require(group in SYSCALL_SETS and result['fixture_paths_only'] == (group != 'sync'),
            'unknown syscall group or path-selection mode')
    error_name = result.get('injected_error', 'EIO')
    require(error_name in ('EIO', 'EINTR', 'ENOSPC', 'EDQUOT'), 'unknown injected errno')
    formats, operations = result['formats'], result['operations']
    require(formats and operations and len(formats) == len(set(formats))
            and len(operations) == len(set(operations)), 'empty or duplicate campaign format or operation')
    cases = result['cases']
    require(Counter((case['format'], case['operation']) for case in cases)
            == Counter(itertools.product(formats, operations)), 'missing or duplicate control')
    limit = result['max_ordinals']
    require(type(limit) is int and limit >= 0, 'invalid ordinal limit')
    outcomes = Counter()
    total = 0
    for case in cases:
        operation = case['operation']
        control = case['control']
        check_probe(control, operation, 'ordinary-first')
        require(control['outcome']['status'] == 'success', 'control did not succeed')
        expected = []
        for syscall in SYSCALL_SETS[group]:
            ordinals = control['eligible_ordinals'][syscall]
            require(ordinals == sorted(set(ordinals))
                    and all(type(n) is int and 1 <= n <= control['counts'][syscall] for n in ordinals),
                    'invalid or duplicate selected ordinal')
            if group == 'sync':
                require(ordinals == list(range(1, control['counts'][syscall] + 1)),
                        'sync control omitted an observed call')
            expected.extend((syscall, n, order) for n in (ordinals[:limit] if limit else ordinals)
                            for order in RECOVERY_ORDERS)
        injections = case['injections']
        actual = [(item['syscall'], item['ordinal'], item['order']) for item in injections]
        require(Counter(actual) == Counter(expected), 'missing or duplicate selected injection')
        for item in injections:
            check_probe(item, operation, item['order'], item['syscall'], error_name)
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
        raise SystemExit(f'coverage verification failed: {error}') from error
    print(json.dumps(summary, sort_keys=True))


if __name__ == '__main__':
    main()
