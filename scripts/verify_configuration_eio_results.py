#!/usr/bin/env python3
"""Verify selected-call coverage and automatic-configuration fault evidence."""
import argparse
from collections import Counter
import gzip
import itertools
import json
from pathlib import Path
import re

from configuration_eio_campaign import FILES, FORMATS, OPERATIONS, ORDERS, check_outcome
from sync_eio_campaign import SYSCALL_SETS, reports_injected_error


def require(condition, message):
    if not condition:
        raise ValueError(message)


def check_probe(probe, operation, order, fmt, syscall=None, error_name=None):
    outcome, verification = probe['outcome'], probe['verification']
    require(outcome['operation'] == operation and outcome['status'] in ('success', 'error'), 'unexpected outcome')
    require(verification['status'] == 'verified' and verification['operation'] == operation
            and verification['order'] == order and verification['backup_restored'] is True
            and type(verification['enabled']) is bool, 'missing recovery or backup verification')
    check_outcome(operation, outcome, verification, probe['marker_before'], probe['marker_after'])
    evidence = probe['before']
    require(set(evidence) == set(FILES) | {'v3_metadata'}, 'missing retained evidence')
    for key in FILES:
        require(isinstance(evidence[key], str) and re.fullmatch(r'[0-9a-f]{64}', evidence[key]), 'invalid retained digest')
    metadata = evidence['v3_metadata']
    require((isinstance(metadata, str) and re.fullmatch(r'[0-9a-f]{64}', metadata)) if fmt == 'v3' else metadata is None,
            'invalid metadata witness digest')
    require(evidence == probe['after_attempt'] == probe['after_verification'], 'retained evidence changed')
    require(set(probe['stages_after_cleanup']) == {'root', 'source', 'configuration'}
            and not any(probe['stages_after_cleanup'].values()), 'staging cleanup incomplete')
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
    error_name = result['injected_error']
    require(error_name in ('EIO', 'EAGAIN'), 'unknown injected errno')
    formats, operations = result['formats'], result['operations']
    require(formats and operations and len(formats) == len(set(formats)) and len(operations) == len(set(operations))
            and all(fmt in FORMATS for fmt in formats) and all(op in OPERATIONS for op in operations), 'invalid formats or operations')
    cases = result['cases']
    require(Counter((case['format'], case['operation']) for case in cases) == Counter(itertools.product(formats, operations)),
            'missing or duplicate configuration control')
    limit = result['max_ordinals']
    require(type(limit) is int and 0 <= limit <= 100, 'invalid ordinal limit')
    outcomes, total = Counter(), 0
    for case in cases:
        fmt, operation, control = case['format'], case['operation'], case['control']
        check_probe(control, operation, 'ordinary-first', fmt)
        require(control['outcome']['status'] == 'success', 'configuration control did not succeed')
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
            check_probe(item, operation, item['order'], fmt, item['syscall'], error_name)
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
        raise SystemExit(f'configuration coverage verification failed: {error}') from error
    print(json.dumps(summary, sort_keys=True))


if __name__ == '__main__':
    main()
