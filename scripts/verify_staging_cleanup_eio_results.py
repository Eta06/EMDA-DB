#!/usr/bin/env python3
"""Independently verify recorded staging-cleanup syscall-injection results."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re

from sync_eio_campaign import SYSCALL_SETS, reports_injected_error, targets_fixture

FILE = '.emda-stage-v1-f-aaaaaaaaaaaaaaaa.owner'
DIRECTORY = '.emda-stage-v1-d-bbbbbbbbbbbbbbbb.owner'
LINK = '.emda-stage-v1-f-cccccccccccccccc.owner'
RETIRED = '.emda-stage-v1-r-dddddddddddddddd.owner'
BAD = '.emda-stage-v1-f-eeeeeeeeeeeeeeee.owner'
REMOVABLE = (FILE, DIRECTORY)


def file_entry(data):
    return {'kind': 'file', 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}


def expected_fixture():
    return {
        'sentinel': file_entry(b'preserve unrelated source data'),
        FILE: file_entry(b'EMDA-STAGING-V1-FILE\n'),
        FILE + '.data': file_entry(b'abandoned file'),
        DIRECTORY: file_entry(b'EMDA-STAGING-V1-DIRECTORY\n'),
        DIRECTORY + '.data': {'kind': 'directory'},
        DIRECTORY + '.data/nested': file_entry(b'abandoned directory payload'),
        DIRECTORY + '.data/link': {'kind': 'symlink', 'target': '../sentinel'},
        LINK: file_entry(b'EMDA-STAGING-V1-FILE\n'),
        LINK + '.data': {'kind': 'symlink', 'target': 'sentinel'},
        RETIRED: file_entry(b'EMDA-STAGING-V1-RETIRED\n' + bytes(20)),
        RETIRED + '.data': {'kind': 'directory'},
        RETIRED + '.data/keep': file_entry(b'preserve retired data'),
        BAD: file_entry(b'invalid marker'),
    }


def removable(path):
    return any(path == marker or path == marker + '.data' or path.startswith(marker + '.data/')
               for marker in REMOVABLE)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify_case(case, *, syscall=None, ordinal=None, error=None):
    before, after, final = (case[name] for name in ('before', 'after_attempt', 'after_cleanup'))
    require(before == expected_fixture(), 'initial fixture differs')
    require(all(path in before and value == before[path] for path, value in after.items()),
            'attempt changed or introduced fixture data')
    protected = {path: value for path, value in before.items() if not removable(path)}
    require(all(after.get(path) == value for path, value in protected.items()),
            'attempt removed protected fixture data')
    outcome = case['outcome']
    require(outcome.get('operation') == 'cleanup', 'invalid cleanup operation')
    if outcome.get('status') == 'success':
        require(outcome.get('classification') is None and not any(removable(p) for p in after),
                'successful cleanup left a recognized stage')
        require(outcome.get('removed_payloads') == 2 and outcome.get('removed_markers') == 2
                and outcome.get('skipped') == 3, 'invalid successful cleanup report')
    else:
        require(outcome.get('status') == 'error' and outcome.get('classification') in ('not_started', 'unknown'),
                'invalid cleanup failure')
        if outcome['classification'] == 'not_started':
            require(after == before, 'not-started cleanup changed the fixture')
    for marker in REMOVABLE:
        require(marker in after or not any(path == marker + '.data' or path.startswith(marker + '.data/')
                                           for path in after), 'removed marker still has a payload')
    resumed = case['resumed']
    require(resumed.get('status') == 'success' and resumed.get('classification') is None
            and resumed.get('operation') == 'cleanup' and resumed.get('skipped') == 3,
            'cleanup could not safely resume')
    require(resumed.get('removed_markers') == sum(marker in after for marker in REMOVABLE)
            and resumed.get('removed_payloads') == sum(marker + '.data' in after for marker in REMOVABLE),
            'resume removal counts differ from the inspected state')
    require(final == protected, 'cleanup changed protected fixture data')
    traces = case['injected_traces']
    require(len(traces) == int(syscall is not None), 'wrong number of injected traces')
    if syscall is not None:
        line = traces[0]
        require(re.search(r'\b' + re.escape(syscall) + r'\(', line)
                and reports_injected_error(line, error)
                and targets_fixture(line, syscall, Path(case['fixture'])), 'injection target or errno differs')


def verify(result):
    require(result.get('completed') is True and result.get('failure') is None and result.get('current') is None,
            'campaign is incomplete or failed')
    group, error, limit = result['syscall_set'], result['injected_error'], result['max_ordinals']
    require(group in SYSCALL_SETS and error in ('EIO', 'EAGAIN', 'EINTR', 'ENOSPC', 'EDQUOT')
            and type(limit) is int and 0 <= limit <= 100,
            'invalid campaign configuration')
    control = result['control']
    verify_case(control)
    require(control['outcome']['status'] == 'success', 'control cleanup failed')
    expected = []
    for syscall in SYSCALL_SETS[group]:
        ordinals = control['eligible_ordinals'][syscall]
        require(ordinals == sorted(set(ordinals))
                and all(type(n) is int and 1 <= n <= control['counts'][syscall] for n in ordinals),
                'invalid eligible syscall ordinals')
        expected.extend((syscall, n) for n in (ordinals[:limit] if limit else ordinals))
    actual = [(case['syscall'], case['ordinal']) for case in result['cases']]
    require(Counter(actual) == Counter(expected), 'missing or duplicate selected injection')
    outcomes = Counter()
    for case in result['cases']:
        verify_case(case, syscall=case['syscall'], ordinal=case['ordinal'], error=error)
        outcomes[case['outcome'].get('classification') or 'success'] += 1
    return {'injections': len(actual), 'errno': error, 'outcomes': dict(outcomes)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report')
    args = parser.parse_args()
    print(json.dumps(verify(json.loads(Path(args.report).read_text()))))


if __name__ == '__main__':
    main()
