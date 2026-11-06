#!/usr/bin/env python3
"""Independently check a completed concurrent filesystem-fault report."""
import argparse
import collections
import itertools
import json
import re
from pathlib import Path


def verify(report):
    if report.get('completed') is not True or report.get('failure') is not None or report.get('current') is not None:
        raise ValueError('incomplete concurrent campaign')
    error = report.get('error')
    mode = report.get('mode')
    repetitions = report.get('repetitions')
    if error not in ('EIO', 'EINTR') or mode not in ('single', 'transaction') or type(repetitions) is not int or not 1 <= repetitions <= 20:
        raise ValueError('invalid campaign configuration')
    expected = set(itertools.product(range(1, repetitions + 1), ('v2', 'v3'),
                                     ('ordinary', 'checkpoint'), ('witness', 'log-sync'),
                                     (1, 2), ('ordinary-first', 'checkpoint-first')))
    seen = set()
    totals = collections.Counter()
    for case in report.get('cases', []):
        identity = (case.get('repetition'), case.get('format'), case.get('backend'), case.get('fault'),
                    case.get('ordinal_per_tracee'), case.get('recovery_order'))
        if (type(identity[0]) is not int or type(identity[4]) is not int
                or identity not in expected or identity in seen):
            raise ValueError('duplicate or unexpected concurrent case')
        seen.add(identity)
        _, format_name, _, fault, _, order = identity
        workers = case.get('receipts', {}).get('workers')
        if not isinstance(workers, list) or len(workers) != 4:
            raise ValueError('invalid worker receipts')
        acknowledged = 0
        failed = set()
        for index, worker in enumerate(workers):
            values = worker.get('acknowledged')
            failure = worker.get('failed')
            if (type(worker.get('id')) is not int or worker['id'] != index
                    or not isinstance(values, list) or len(values) > 64
                    or any(type(value) is not int for value in values)
                    or values != list(range(len(values)))
                    or (failure is None and (len(values) != 64 or worker.get('error') is not None))
                    or (failure is not None and (type(failure) is not int or failure != len(values)
                        or failure >= 64 or not isinstance(worker.get('error'), str) or not worker['error']))):
                raise ValueError('invalid worker receipt')
            acknowledged += len(values)
            if failure is not None:
                failed.add((index, failure))
        if error == 'EIO' and not failed:
            raise ValueError('EIO case did not report a failed writer')

        checks = case.get('verified')
        expected_backends = ('ordinary', 'checkpoint') if order == 'ordinary-first' else ('checkpoint', 'ordinary')
        if not isinstance(checks, list) or len(checks) != 2:
            raise ValueError('missing recovery verification')
        recovered = None
        for item, backend in zip(checks, expected_backends):
            uncertain = item.get('uncertain_present')
            if (item.get('status') != 'verified' or item.get('backend') != backend
                    or item.get('mode') != mode or type(item.get('acknowledged')) is not int
                    or item['acknowledged'] != acknowledged
                    or not isinstance(uncertain, list)):
                raise ValueError('invalid recovery result')
            try:
                positions = [tuple(position) for position in uncertain]
                valid = all(len(position) == 2 and all(type(n) is int for n in position)
                            and position in failed for position in positions)
                valid = valid and len(set(positions)) == len(positions)
            except (TypeError, ValueError):
                valid = False
            if not valid or (recovered is not None and uncertain != recovered):
                raise ValueError('invalid or inconsistent uncertain outcomes')
            recovered = uncertain

        syscall = ('pwrite64' if format_name == 'v2' else 'fsetxattr') if fault == 'witness' else (
            'fdatasync' if format_name == 'v2' else 'fsync')
        suffix = 'commit.state' if fault == 'witness' and format_name == 'v2' else 'records.log'
        target = re.compile(r'\b' + syscall + r'\(\d+<[^<>]*/source/' + re.escape(suffix) + r'>')
        traces = case.get('injected_traces')
        if (not isinstance(traces, list) or not traces or any(
                not isinstance(line, str) or not target.search(line) or '(INJECTED)' not in line
                or not re.search(r'=\s*-1\s+' + error + r'\b', line)
                or (syscall == 'fsetxattr' and '"user.db.commit.v3"' not in line) for line in traces)):
            raise ValueError('injected trace did not match its source or errno')

        before = case.get('before', {})
        for after in (case.get('after_attempt', {}), case.get('after_verification', {})):
            stable = ('backup_sha256', 'backup_bytes', 'checkpoint_sha256', 'checkpoint_digest_sha256')
            if any(before.get(field) is None or before.get(field) != after.get(field) for field in stable):
                raise ValueError('reference fixture changed or was not recorded')
            if format_name == 'v3' and (before.get('witness_sha256') is None
                                        or before['witness_sha256'] != after.get('witness_sha256')):
                raise ValueError('V3 identity anchor changed')
            if (type(before.get('log_bytes')) is not int or type(after.get('log_bytes')) is not int
                    or after['log_bytes'] < before['log_bytes']):
                raise ValueError('source log length was not preserved')
        totals['cases'] += 1
        totals['acknowledgements'] += acknowledged
        totals['failed_calls'] += len(failed)
        totals['uncertain_present'] += len(recovered)
        totals['injections'] += len(traces)
    if seen != expected:
        raise ValueError('concurrent report omitted one or more selected cases')
    return {'errno': error, 'mode': mode, **totals}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    args = parser.parse_args()
    with args.report.open(encoding='utf-8') as file:
        result = json.load(file)
    print(json.dumps(verify(result), sort_keys=True))


if __name__ == '__main__':
    main()
