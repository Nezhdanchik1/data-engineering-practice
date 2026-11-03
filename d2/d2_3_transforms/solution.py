"""D2.3 Core: chainable transforms and Pipeline.

Implementation for this task; run its adjacent pytest suite from the project root.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from d1.d1_1_csv_ingestion.solution import parse_amount
from d1.d1_2_dates.solution import normalise_ts
from d1.d1_3_text_cleaning.solution import validate_email, normalise_phone, strip_html


CONVERTERS = {'str': str, 'int': int, 'decimal': parse_amount,
              'positive_amount': lambda v: parse_amount(v, positive=True),
              'timestamp': normalise_ts, 'email': validate_email,
              'phone': normalise_phone, 'strip_html': strip_html}


SCHEMAS = {'orders': {'order_id': int, 'amount': lambda v: parse_amount(v, positive=True)}}


class Transform(ABC):
    registry = {}

    def __init_subclass__(cls, *, name=None, **kwargs):
        super().__init_subclass__(**kwargs)
        if name:
            if name in Transform.registry:
                raise ValueError(f'duplicate transform plugin: {name}')
            Transform.registry[name] = cls

    @abstractmethod
    def apply(self, record) -> dict | None: ...

    def reset(self):
        pass

    def __or__(self, other):
        if not isinstance(other, Transform):
            return NotImplemented
        return TransformChain([self, other])


class TransformChain(Transform, name='chain'):
    def __init__(self, transforms):
        transforms = list(transforms)
        if not all(isinstance(t, Transform) for t in transforms):
            raise TypeError('chain requires transforms')
        self.transforms = list(transforms)

    def apply(self, record):
        for transform in self.transforms:
            record = transform.apply(record)
            if record is None:
                break
        return record

    def reset(self):
        for transform in self.transforms:
            transform.reset()


class Filter(Transform, name='filter'):
    def __init__(self, predicate):
        self.predicate = predicate

    def apply(self, record):
        return dict(record) if self.predicate(record) else None


class Rename(Transform, name='rename'):
    def __init__(self, mapping):
        self.mapping = dict(mapping)

    def apply(self, record):
        row = {}
        for key, value in record.items():
            target = self.mapping.get(key, key)
            if target in row:
                raise ValueError(f'rename collision: {target}')
            row[target] = value
        return row


class Cast(Transform, name='cast'):
    def __init__(self, schema):
        schema = SCHEMAS[schema] if isinstance(schema, str) else schema
        self.schema = {key: CONVERTERS[value] if isinstance(value, str) else value
                       for key, value in schema.items()}

    def apply(self, record):
        row = dict(record)
        for key, converter in self.schema.items():
            row[key] = converter(row[key])
        return row


class Deduplicate(Transform, name='dedupe'):
    def __init__(self, key):
        self.key, self.seen = key, set()

    def apply(self, record):
        value = record[self.key]
        if value in self.seen:
            return None
        self.seen.add(value)
        return dict(record)

    def reset(self):
        self.seen.clear()


class NormaliseDate(Transform, name='date'):
    def __init__(self, column, source_tz='UTC', dayfirst=True):
        self.column, self.source_tz, self.dayfirst = column, source_tz, dayfirst

    def apply(self, record):
        row = dict(record)
        row[self.column] = normalise_ts(row[self.column], self.source_tz, self.dayfirst)
        return row


@dataclass
class PipelineStats:
    rows_in: int = 0
    rows_out: int = 0
    rejected: int = 0
    dropped: int = 0
    rejects: list = field(default_factory=list)
    steps: list = field(default_factory=list)


class Pipeline:
    def __init__(self, source, transforms=(), sink=None, checks=()):
        self.source, self.transforms, self.sink = source, list(transforms), sink
        self.checks = list(checks)

    def __or__(self, transform):
        if not isinstance(transform, Transform):
            return NotImplemented
        return Pipeline(self.source, self.transforms + [transform], self.sink, self.checks)

    def into(self, sink=None):
        sink = sink or self.sink
        if sink is None:
            raise ValueError('pipeline requires a sink')
        stats = PipelineStats(steps=[dict(name=type(t).__name__, rows_in=0, rows_out=0,
                                         rejected=0, dropped=0) for t in self.transforms])
        for transform in self.transforms:
            transform.reset()
        with self.source, sink:
            for original in self.source.read():
                stats.rows_in += 1
                record = dict(original)
                for transform, step in zip(self.transforms, stats.steps):
                    step['rows_in'] += 1
                    try:
                        record = transform.apply(record)
                        if record is None:
                            step['dropped'] += 1
                            stats.dropped += 1
                            break
                        if not isinstance(record, dict):
                            raise TypeError('transform must return dict or None')
                        step['rows_out'] += 1
                    except Exception as exc:
                        stats.rejects.append(dict(record=original, step=step['name'], reason=str(exc)))
                        step['rejected'] += 1
                        record = None
                        break
                if record is not None:
                    try:
                        stats.rows_out += sink.write([record])
                    except (OSError, ConnectionError, TimeoutError):
                        raise
                    except Exception as exc:
                        stats.rejects.append(dict(record=original, step='sink', reason=str(exc)))
            stats.rejects.extend(self.source.rejects)
            stats.rows_in += len(self.source.rejects)
        stats.rejected = len(stats.rejects)
        return stats
