"""D2.4 Advanced: polymorphic data-quality checks and reports.

Implementation for this task; run its adjacent pytest suite from the project root.
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from common import atomic_json, utc_now
from d1.d1_2_dates.solution import normalise_ts


@dataclass
class CheckResult:
    name: str
    passed: bool
    failed_count: int
    sample_failures: list
    severity: str = 'error'


class Check(ABC):
    registry = {}

    def __init_subclass__(cls, *, name=None, **kwargs):
        super().__init_subclass__(**kwargs)
        if name:
            if name in Check.registry:
                raise ValueError(f'duplicate check plugin: {name}')
            Check.registry[name] = cls

    def __init__(self, column, *, severity='error'):
        if severity not in {'error', 'warning'}:
            raise ValueError('severity must be error or warning')
        self.column, self.severity = column, severity

    @abstractmethod
    def run(self, records) -> CheckResult: ...

    def evaluate(self, records, predicate):
        failed, sample = 0, []
        for number, record in enumerate(records, 1):
            try:
                valid = predicate(record.get(self.column))
            except Exception:
                valid = False
            if not valid:
                failed += 1
                if len(sample) < 5:
                    sample.append({'row': number, 'record': record})
        return CheckResult(type(self).__name__, failed == 0, failed, sample, self.severity)


class NotNull(Check, name='not_null'):
    def run(self, records):
        return self.evaluate(records, lambda value: value is not None and value != '')


class Unique(Check, name='unique'):
    def run(self, records):
        seen = set()
        def predicate(value):
            if value is None or value in seen:
                return False
            seen.add(value)
            return True
        return self.evaluate(records, predicate)


class InRange(Check, name='in_range'):
    def __init__(self, column, low, high, *, severity='error'):
        super().__init__(column, severity=severity)
        if low > high:
            raise ValueError('low must not exceed high')
        self.low, self.high = low, high

    def run(self, records):
        def predicate(value):
            if isinstance(value, str) and not isinstance(self.low, str):
                value = Decimal(value)
            return self.low <= value <= self.high
        return self.evaluate(records, predicate)


class MatchesPattern(Check, name='matches_pattern'):
    def __init__(self, column, regex, *, severity='error'):
        super().__init__(column, severity=severity)
        self.regex = re.compile(regex)

    def run(self, records):
        return self.evaluate(records, lambda value: isinstance(value, str) and self.regex.fullmatch(value) is not None)


class Freshness(Check, name='freshness'):
    def __init__(self, column, max_age: timedelta, *, severity='error', now=None):
        super().__init__(column, severity=severity)
        if not isinstance(max_age, timedelta) or max_age < timedelta(0):
            raise ValueError('max_age must be a nonnegative timedelta')
        self.max_age, self.now = max_age, now or (lambda: datetime.now(timezone.utc))

    def run(self, records):
        now = self.now()
        return self.evaluate(records, lambda value: timedelta(0) <= now - datetime.fromisoformat(normalise_ts(value)) <= self.max_age)


class QualitySuite:
    def __init__(self, checks):
        self.checks = list(checks)

    def run(self, records, report_path=None):
        # Replay generators for each check. File-backed capstone uses a replay factory.
        rows = list(records)
        return self.run_factory(lambda: iter(rows), report_path)

    def run_factory(self, factory, report_path=None):
        results = [asdict(check.run(factory())) for check in self.checks]
        report = {'run_timestamp': utc_now(),
                  'passed': all(r['passed'] or r['severity'] == 'warning' for r in results),
                  'checks': results}
        if report_path is not None:
            atomic_json(report_path, report)
        return report
