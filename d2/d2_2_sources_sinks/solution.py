"""D2.2 Core: streaming source and sink hierarchy.

Implementation for this task; run its adjacent pytest suite from the project root.
"""
from __future__ import annotations

import csv
import json
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from pathlib import Path
from common import json_default
from d1.d1_2_dates.solution import normalise_ts, partition_path


class Connector(ABC):
    def __enter__(self):
        try:
            self.open()
        except Exception:
            self.close()
            raise
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
        return False

    @abstractmethod
    def open(self): ...

    @abstractmethod
    def close(self): ...


class Source(Connector):
    registry = {}

    def __init_subclass__(cls, *, name=None, **kwargs):
        super().__init_subclass__(**kwargs)
        if name:
            if name in Source.registry:
                raise ValueError(f'duplicate source plugin: {name}')
            Source.registry[name] = cls

    def __init__(self):
        self.rejects = []

    @abstractmethod
    def read(self) -> Iterator[dict]: ...

    def __or__(self, transform):
        from d2.d2_3_transforms.solution import Pipeline
        return Pipeline(self) | transform


class CsvSource(Source, name='csv'):
    def __init__(self, path):
        super().__init__()
        self.path, self.stream = Path(path), None

    def open(self):
        self.rejects.clear()
        self.stream = self.path.open('rb')

    def close(self):
        if self.stream is not None:
            self.stream.close()
            self.stream = None

    def read(self):
        if self.stream is None:
            with self:
                yield from self.read()
            return
        def decoded_lines():
            for number, raw in enumerate(self.stream, 1):
                try:
                    line = raw.decode('utf-8')
                    yield line.lstrip('\ufeff') if number == 1 else line
                except UnicodeDecodeError as exc:
                    self.rejects.append(dict(record=raw.decode('utf-8', errors='replace'),
                                             line=number, reason=str(exc), file=str(self.path)))
        reader = csv.DictReader(decoded_lines(), strict=True)
        while True:
            try:
                row = next(reader)
            except StopIteration:
                break
            except csv.Error as exc:
                self.rejects.append(dict(record=None, reason=str(exc), line=reader.reader.line_num))
                continue
            if None in row or any(v is None for v in row.values()):
                self.rejects.append(dict(record=row, reason='CSV column count mismatch', line=reader.line_num))
                continue
            yield row


class JsonLinesSource(Source, name='jsonl'):
    def __init__(self, path):
        super().__init__()
        self.path, self.stream = Path(path), None

    def open(self):
        self.rejects.clear()
        self.stream = self.path.open('rb')

    def close(self):
        if self.stream is not None:
            self.stream.close()
            self.stream = None

    def read(self):
        if self.stream is None:
            with self:
                yield from self.read()
            return
        for number, raw in enumerate(self.stream, 1):
            try:
                record = json.loads(raw.decode('utf-8'))
                if not isinstance(record, dict):
                    raise ValueError('JSON record must be an object')
                yield record
            except (UnicodeDecodeError, ValueError, RecursionError) as exc:
                self.rejects.append(dict(record=raw.decode('utf-8', errors='replace').rstrip(),
                                         file=str(self.path), line=number, reason=str(exc)))


class InMemorySource(Source, name='memory'):
    def __init__(self, records=()):
        super().__init__()
        self.records = records

    def open(self):
        self.rejects.clear()

    def close(self):
        pass

    def read(self):
        for number, record in enumerate(self.records, 1):
            if not isinstance(record, dict):
                self.rejects.append(dict(record=record, line=number, reason='record must be a dict'))
                continue
            yield dict(record)


class Sink(Connector):
    """write accepts any iterable (including empty); returns int; flush always exists."""
    registry = {}

    def __init_subclass__(cls, *, name=None, **kwargs):
        super().__init_subclass__(**kwargs)
        if name:
            if name in Sink.registry:
                raise ValueError(f'duplicate sink plugin: {name}')
            Sink.registry[name] = cls

    @abstractmethod
    def write(self, records: Iterable[dict]) -> int: ...

    def flush(self):
        pass


class JsonLinesSink(Sink, name='jsonl'):
    def __init__(self, path):
        self.path, self.stream = Path(path), None

    def open(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = self.path.open('w', encoding='utf-8', newline='')

    def write(self, records):
        if self.stream is None:
            with self:
                return self.write(records)
        count = 0
        for record in records:
            payload = json.dumps(record, ensure_ascii=False, allow_nan=False, default=json_default)
            self.stream.write(payload + '\n')
            count += 1
        return count

    def flush(self):
        if self.stream is not None:
            self.stream.flush()

    def close(self):
        if self.stream is not None:
            try:
                self.flush()
            finally:
                self.stream.close()
                self.stream = None


class CsvSink(JsonLinesSink, name='csv'):
    def __init__(self, path, fieldnames=None):
        super().__init__(path)
        self.fieldnames = fieldnames
        self.writer = None

    def open(self):
        super().open()
        self.writer = None
        if self.fieldnames:
            self.writer = csv.DictWriter(self.stream, self.fieldnames)
            self.writer.writeheader()

    def write(self, records):
        if self.stream is None:
            with self:
                return self.write(records)
        count = 0
        for record in records:
            if self.writer is None:
                self.writer = csv.DictWriter(self.stream, list(record))
                self.writer.writeheader()
            self.writer.writerow(record)
            count += 1
        return count


class InMemorySink(Sink, name='memory'):
    def __init__(self):
        self.records = []

    def open(self):
        pass

    def close(self):
        self.flush()

    def write(self, records):
        count = 0
        for record in records:
            self.records.append(dict(record))
            count += 1
        return count


class PartitionedSink(Sink, name='partitioned'):
    def __init__(self, path, timestamp='ordered_at', source_tz='UTC'):
        self.path, self.timestamp, self.source_tz = Path(path), timestamp, source_tz
        self.sinks = {}

    def open(self):
        self.path.mkdir(parents=True, exist_ok=True)

    def write(self, records):
        count = 0
        for record in records:
            row = dict(record)
            row[self.timestamp] = normalise_ts(row[self.timestamp], self.source_tz)
            partition = partition_path(row[self.timestamp])
            if partition not in self.sinks:
                sink = JsonLinesSink(self.path / partition / 'records.jsonl')
                sink.open()
                self.sinks[partition] = sink
            count += self.sinks[partition].write([row])
        return count

    def flush(self):
        for sink in self.sinks.values():
            sink.flush()

    def close(self):
        try:
            for sink in self.sinks.values():
                sink.close()
        finally:
            self.sinks.clear()
