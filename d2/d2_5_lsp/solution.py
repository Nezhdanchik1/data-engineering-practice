"""D2.5 Advanced: substitutable Batch, DryRun and Warehouse sinks.

Implementation for this task; run its adjacent pytest suite from the project root.
"""
from __future__ import annotations

from d2.d2_2_sources_sinks.solution import Sink, InMemorySink


class BatchSink(InMemorySink, name='batch'):
    """Accept any iterable; buffer small batches instead of strengthening preconditions."""

    def __init__(self, batch_size=100):
        super().__init__()
        if not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError('batch_size must be positive')
        self.batch_size, self.buffer = batch_size, []

    def write(self, records):
        count = 0
        for record in records:
            self.buffer.append(dict(record))
            count += 1
            if len(self.buffer) >= self.batch_size:
                self.flush()
        return count

    def flush(self):
        self.records.extend(self.buffer)
        self.buffer.clear()


class DryRunSink(InMemorySink, name='dry_run'):
    """Preserve the integer-count postcondition even when no data is stored."""

    def write(self, records):
        return sum(1 for _ in records)


class WarehouseSink(InMemorySink, name='warehouse'):
    """Local warehouse stand-in; inherited flush is a supported no-op."""
