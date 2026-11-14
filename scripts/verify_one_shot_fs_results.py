#!/usr/bin/env python3
"""Verify recorded observed-path coverage from a one-shot filesystem campaign."""
import argparse
from collections import Counter
import gzip
import itertools
import json
from pathlib import Path
import re

from sync_eio_campaign import LEGACY_OPERATIONS, MANAGED_OPERATIONS, MANAGED_REBUILD_OPERATIONS, MANAGED_SUFFIX_OPERATIONS, RECOVERY_ORDERS, SYSCALL_SETS, WRITES

ORDERS = {'ordinary-first': ['ordinary', 'checkpoint'],
          'checkpoint-first': ['checkpoint', 'ordinary']}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def check_probe(probe, operation, order, syscall=None, error_name=None, publication=False, managed_binding=0, managed_handle=0):
    require(probe['outcome']['operation'] == operation
            and probe['outcome']['status'] in ('success', 'error'), 'unexpected probe outcome')
    verified = probe['verification']
    require(verified['status'] == 'verified' and verified['operation'] == operation
            and verified['recovery_order'] == ORDERS[order], 'missing recovery verification')
    if operation not in WRITES:
        before = probe['before']
        digest_fields = ('log_sha256', 'witness_sha256', 'backup_sha256',
                         'checkpoint_sha256', 'checkpoint_digest_sha256')
        require(all(isinstance(before[name], str) and re.fullmatch(r'[0-9a-f]{64}', before[name])
                    for name in digest_fields)
                and (before['metadata_sha256'] is None or
                     isinstance(before['metadata_sha256'], str) and
                     re.fullmatch(r'[0-9a-f]{64}', before['metadata_sha256']))
                and all(type(before[name]) is int and before[name] >= 0
                        for name in ('log_bytes', 'backup_bytes'))
                and probe['after_attempt'] == before and probe['after_verification'] == before,
                'copy changed source, checkpoint, or backup evidence')
    if publication:
        from sync_eio_campaign import check_managed_publication_outcome, check_publication_outcome
        require('classification' in probe['outcome'], 'missing publication classification')
        if operation in MANAGED_OPERATIONS:
            selection = probe['managed_selection']
            check_managed_publication_outcome(probe['outcome'], verified, selection['before'], selection['after_attempt'],
                                              require_target=managed_binding, require_handle=managed_handle)
        else:
            check_publication_outcome(probe['outcome'], verified)
    if operation in MANAGED_OPERATIONS:
        selection = probe['managed_selection']
        before, after = selection['before'], selection['after_attempt']
        require((before is None) != operation.endswith('-existing')
                and after == selection['after_verification']
                and verified['selection_visible'] == (after is not None)
                and not selection['stages_after_cleanup'], 'managed selection evidence changed')
        suffix = None
        if operation in MANAGED_SUFFIX_OPERATIONS:
            require(managed_binding == 2, 'missing managed suffix binding version')
            suffix = probe['managed_suffix']
            reference = suffix['before']
            require(all(isinstance(reference[name], str) and re.fullmatch(r'[0-9a-f]{64}', reference[name])
                        for name in ('checkpoint', 'digest'))
                    and reference['digest'] != probe['before']['checkpoint_digest_sha256']
                    and suffix['after_attempt'] == reference and suffix['after_verification'] == reference,
                    'trusted suffix checkpoint evidence changed')
        for state in (before, after):
            if state is not None:
                expected_digest = (suffix['before']['digest'] if suffix and state['checkpoint'] == 'result'
                                   else probe['before']['checkpoint_digest_sha256'])
                require(type(state['bytes']) is int and state['bytes'] > 0
                        and isinstance(state['sha256'], str) and re.fullmatch(r'[0-9a-f]{64}', state['sha256'])
                        and (not managed_binding or
                             state['checkpoint_digest_sha256'] == expected_digest),
                        'invalid managed selection digest')
        previous = selection['previous_checkpoint_sha256']
        require((previous is None if before is None else
                 isinstance(previous, str) and re.fullmatch(r'[0-9a-f]{64}', previous))
                and previous == selection['previous_after_attempt_sha256']
                and previous == selection['previous_after_verification_sha256'],
                'previous managed checkpoint evidence changed')
    if operation in LEGACY_OPERATIONS:
        states = probe['legacy_source']
        legacy = states['before']
        length = legacy['bytes']
        require(type(length) is int and (length == 0 if operation == 'upgrade-empty-legacy' else length > 0),
                'invalid legacy source length')
        before = legacy['log_sha256']
        require(isinstance(before, str) and re.fullmatch(r'[0-9a-f]{64}', before)
                and legacy['witness_sha256'] is None and legacy['metadata_sha256'] is None
                and states['after_attempt'] == legacy and states['after_verification'] == legacy,
                'legacy source log or witness changed or digest missing')
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
    require(error_name in ('EIO', 'EINTR', 'EAGAIN', 'ENOSPC', 'EDQUOT'), 'unknown injected errno')
    formats, operations = result['formats'], result['operations']
    publication = result.get('publication_outcomes', False)
    require(type(publication) is bool, 'invalid publication-outcome mode')
    managed_binding = result.get('managed_selection_binding', 0)
    require(type(managed_binding) is int and managed_binding in (0, 1, 2), 'unknown managed selection binding version')
    managed_handle = result.get('managed_handle_check', 0)
    require(type(managed_handle) is int and managed_handle in (0, 1), 'unknown managed handle check version')
    if publication and any(operation in MANAGED_REBUILD_OPERATIONS for operation in operations):
        require(all(operation in MANAGED_REBUILD_OPERATIONS for operation in operations)
                and managed_handle == 0, 'offline rebuild cannot report an open handle check')
    if publication:
        require(all(operation in ('backup', 'checkpoint-backup', 'restore', 'checkpoint', 'checkpoint-copy', 'compaction', 'checkpoint-compaction', 'upgrade', 'checkpoint-rollover', *LEGACY_OPERATIONS, *MANAGED_OPERATIONS) for operation in operations),
                'invalid publication operation')
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
        check_probe(control, operation, 'ordinary-first', publication=publication, managed_binding=managed_binding,
                    managed_handle=managed_handle)
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
            check_probe(item, operation, item['order'], item['syscall'], error_name, publication, managed_binding,
                        managed_handle)
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
