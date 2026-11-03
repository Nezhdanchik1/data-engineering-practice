"""Incremental config-driven ETL CLI with per-file staging and recovery."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import tempfile
import time

from d2.d2_6_registry.solution import ConfigError, build_pipeline, read_config
from d2.d2_2_sources_sinks.solution import Sink, PartitionedSink
from common import atomic_json, json_default, utc_now
from d1.d1_2_dates.solution import normalise_ts, partition_path
from d1.d1_5_retry.solution import retry
from d2.d2_4_quality.solution import QualitySuite

LOG = logging.getLogger(__name__)


def _hash_file(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _json_lines(path):
    with Path(path).open(encoding='utf-8') as stream:
        for line in stream:
            yield json.loads(line)


def discover(pattern):
    """Pathlib discovery, including glob expressions and directory sources."""
    path = Path(pattern)
    if path.is_file():
        return [path.resolve()]
    if path.is_dir():
        return sorted(p.resolve() for p in path.iterdir() if p.is_file())
    parts = path.parts
    index = next((i for i, part in enumerate(parts) if any(c in part for c in '*?[')), None)
    if index is None:
        raise FileNotFoundError(pattern)
    root = Path(*parts[:index]) if index else Path('.')
    return sorted(p.resolve() for p in root.glob('/'.join(parts[index:])) if p.is_file())


def _resolve(config, root):
    if not isinstance(config, dict):
        raise ConfigError('$: expected object')
    config = copy.deepcopy(config)
    for section, key in [('source', 'path'), ('sink', 'path')]:
        spec = config.get(section)
        if not isinstance(spec, dict):
            raise ConfigError(f'{section}: expected object')
        value = spec.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ConfigError(f'{section}.{key}: expected nonempty file path')
        path = Path(value)
        spec[key] = str(path if path.is_absolute() else root / path)
    incremental = config.setdefault('incremental', {})
    if not isinstance(incremental, dict):
        raise ConfigError('incremental: expected object')
    partition = config.get('partition', {})
    if not isinstance(partition, dict):
        raise ConfigError('partition: expected object')
    for key in ('timestamp', 'source_tz'):
        if key in partition and (not isinstance(partition[key], str) or not partition[key].strip()):
            raise ConfigError(f'partition.{key}: expected nonempty string')
    for key, default in [('watermark', 'work/watermark.json'), ('report', 'work/report.json'),
                         ('quarantine', 'work/quarantine.jsonl')]:
        value = incremental.get(key, default)
        if not isinstance(value, str) or not value.strip():
            raise ConfigError(f'incremental.{key}: expected nonempty file path')
        path = Path(value)
        incremental[key] = str(path if path.is_absolute() else root / path)
    return config


def _write_line(stream, record):
    stream.write(json.dumps(record, ensure_ascii=False, default=json_default, allow_nan=False) + '\n')


def _inside(path, root):
    path, root = Path(path).resolve(), Path(root).resolve()
    if not path.is_relative_to(root):
        raise ValueError(f'generated path escapes output root: {path}')
    return path


def run(config, *, base_dir=None, sleep=time.sleep, random_fn=None, commit_hook=None):
    """Run and return a JSON-compatible report; hooks inject deterministic test failures.

    File paths in configs are relative to the config directory (or base_dir for dicts).
    A single writer owns the watermark. Every published shard has a stable input ID;
    retrying an interrupted publication replaces that shard instead of appending.
    """
    root = Path(base_dir or '.').resolve()
    if isinstance(config, (str, Path)):
        config_path = Path(config).resolve()
        root, config = config_path.parent, read_config(config_path)
    config = _resolve(config, root)
    # Validate before any data or watermark mutation.
    build_pipeline(config)
    for section in ('source', 'sink'):
        if not isinstance(config[section].get('path'), str):
            raise ConfigError(f'{section}.path: capstone incremental mode requires a file connector')
    retry_options = config.get('retry', {})
    if not isinstance(retry_options, dict):
        raise ConfigError('retry: expected object')
    if set(retry_options) - {'max_attempts', 'base', 'jitter'}:
        raise ConfigError('retry: unknown parameter')
    retry_kwargs = dict(retry_options, sleep=sleep)
    if random_fn is not None:
        retry_kwargs['random_fn'] = random_fn
    try:
        retry_decorator = retry(**retry_kwargs)
    except (ValueError, TypeError) as exc:
        raise ConfigError(f'retry: {exc}') from exc
    incremental = config['incremental']
    watermark = Path(incremental['watermark'])
    report_path = Path(incremental['report'])
    quarantine = Path(incremental['quarantine'])
    work = watermark.parent / (watermark.stem + '_batches')
    output = Path(config['sink']['path'])
    timestamp = config.get('partition', {}).get('timestamp', 'ordered_at')
    source_tz = config.get('partition', {}).get('source_tz', 'UTC')
    if watermark.exists():
        try:
            state = json.loads(watermark.read_text(encoding='utf-8'))
        except (ValueError, UnicodeError) as exc:
            raise ConfigError(f'incremental.watermark: invalid JSON: {exc}') from exc
        if not isinstance(state, dict) or state.get('version') != 1 or not isinstance(state.get('files'), dict):
            raise ConfigError('incremental.watermark: unsupported state')
        for filename, entry in state['files'].items():
            if (not isinstance(entry, dict)
                    or not isinstance(entry.get('fingerprint'), str)
                    or not isinstance(entry.get('batch'), str)
                    or not isinstance(entry.get('rows_out'), int)
                    or not isinstance(entry.get('outputs'), list)
                    or not all(isinstance(path, str) for path in entry['outputs'])):
                raise ConfigError(f'incremental.watermark.files.{filename}: invalid file entry')
    else:
        state = {'version': 1, 'files': {}}
    # Changes to transforms/sink/checkpoint semantics require a distinct watermark.
    signature_config = {k: v for k, v in config.items() if k not in {'retry', 'incremental', 'checks'}}
    signature = hashlib.sha256(json.dumps(signature_config, sort_keys=True).encode()).hexdigest()
    if state.get('pipeline_signature', signature) != signature:
        raise ConfigError('incremental.watermark: pipeline changed; use a new watermark and output directory')
    state['pipeline_signature'] = signature
    start, started = utc_now(), time.monotonic()
    report = dict(start=start, end=None, duration_seconds=0., status='running', files_processed=0,
                  files_skipped=0, rows_in=0, rows_out=0, rejected=0, dropped=0, files=[], steps=[], checks=[])
    all_steps = {}
    output.mkdir(parents=True, exist_ok=True)
    work.mkdir(parents=True, exist_ok=True)

    @retry_decorator
    def process(path, identity, fingerprint):
        """A transient source error retries the full private stage, never public output."""
        stage = Path(tempfile.mkdtemp(prefix=identity + '-', dir=work))
        try:
            spec = copy.deepcopy(config)
            spec['source']['path'] = str(path)
            pipeline = build_pipeline(spec)
            steps = [dict(name=f'transforms[{i}].{type(t).__name__}', rows_in=0, rows_out=0,
                          rejected=0, dropped=0) for i, t in enumerate(pipeline.transforms)]
            partition_step = dict(name='partition', rows_in=0, rows_out=0, rejected=0, dropped=0)
            sink_step = dict(name='sink', rows_in=0, rows_out=0, rejected=0, dropped=0)
            metrics = dict(rows_in=0, rows_out=0, rejected=0, dropped=0,
                           steps=[dict(name='source', rows_in=0, rows_out=0, rejected=0, dropped=0)] + steps + [partition_step, sink_step])
            sinks = {}
            with (stage / 'accepted.jsonl').open('w', encoding='utf-8') as accepted, \
                 (stage / 'rejects.jsonl').open('w', encoding='utf-8') as rejected:
                def reject(record, reason, step, **metadata):
                    metrics['rejected'] += 1
                    _write_line(rejected, dict(record=record, reason=str(reason), step=step, file=str(path), **metadata))
                try:
                    with pipeline.source:
                        for original in pipeline.source.read():
                            metrics['rows_in'] += 1
                            row = dict(original)
                            for transform, step in zip(pipeline.transforms, steps):
                                step['rows_in'] += 1
                                try:
                                    row = transform.apply(row)
                                    if row is None:
                                        step['dropped'] += 1
                                        metrics['dropped'] += 1
                                        break
                                    if not isinstance(row, dict):
                                        raise TypeError('transform must return dict or None')
                                    step['rows_out'] += 1
                                except Exception as exc:
                                    step['rejected'] += 1
                                    reject(original, exc, step['name'])
                                    row = None
                                    break
                            if row is None:
                                continue
                            partition_step['rows_in'] += 1
                            try:
                                row[timestamp] = normalise_ts(row[timestamp], source_tz)
                                partition = partition_path(row[timestamp])
                                # Canonicalise Decimal/datetime for JSON reports and checks.
                                row = json.loads(json.dumps(row, default=json_default, allow_nan=False))
                                partition_step['rows_out'] += 1
                            except Exception as exc:
                                partition_step['rejected'] += 1
                                reject(original, exc, 'partition')
                                continue
                            sink_step['rows_in'] += 1
                            try:
                                if partition not in sinks:
                                    sink_spec = copy.deepcopy(config['sink'])
                                    artifact = {'csv':'records.csv'}.get(sink_spec['type'], 'records.jsonl')
                                    sink_spec['path'] = str(stage / 'output' / partition / artifact)
                                    from d2.d2_6_registry.solution import construct
                                    sink = construct(Sink, sink_spec, 'sink')
                                    if isinstance(sink, PartitionedSink):
                                        sink.path = stage / 'output'
                                        sink.timestamp, sink.source_tz = timestamp, 'UTC'
                                    sink.open()
                                    sinks[partition] = sink
                                written = sinks[partition].write([row])
                                if type(written) is not int or written != 1:
                                    raise TypeError('sink must report one written record')
                                _write_line(accepted, row)
                                metrics['rows_out'] += 1
                                sink_step['rows_out'] += 1
                            except (OSError, ConnectionError, TimeoutError):
                                raise
                            except Exception as exc:
                                sink_step['rejected'] += 1
                                reject(original, exc, 'sink')
                        for item in pipeline.source.rejects:
                            metrics['rows_in'] += 1
                            reject(item.get('record'), item['reason'], 'source', line=item.get('line'))
                finally:
                    for sink in sinks.values():
                        sink.close()
            metrics['steps'][0].update(rows_in=metrics['rows_in'],
                                       rows_out=metrics['rows_in']-len(pipeline.source.rejects),
                                       rejected=len(pipeline.source.rejects))
            if _hash_file(path) != fingerprint:
                raise ConnectionError('source changed during reading')
            atomic_json(stage / 'metrics.json', metrics)
            # Generation directory is recoverable; it becomes authoritative via watermark.
            # A short unique generation path avoids Windows MAX_PATH failures.
            # The full fingerprint remains in the manifest, not the folder name.
            batch = Path(tempfile.mkdtemp(prefix='batch-', dir=work))
            batch.rmdir()
            os.replace(stage, batch)
            return batch, metrics
        finally:
            if stage.exists():
                shutil.rmtree(_inside(stage, work))

    try:
        candidates = discover(config['source']['path'])
        # Do not accidentally ingest our own artifacts when source is a directory.
        for path in candidates:
            if path in {watermark.resolve(), report_path.resolve(), quarantine.resolve()}:
                continue
            identity = hashlib.sha256(str(path).encode('utf-8')).hexdigest()[:24]
            fingerprint = retry_decorator(_hash_file)(path)
            previous = state['files'].get(str(path), {})
            if previous.get('fingerprint') == fingerprint:
                report['files_skipped'] += 1
                continue
            batch, metrics = process(path, identity, fingerprint)
            published = []
            for artifact in sorted((batch / 'output').rglob('*')):
                if not artifact.is_file():
                    continue
                relative = artifact.relative_to(batch / 'output')
                target = _inside(output / relative.parent / (identity + '-' + relative.name), output)
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary = target.with_name(target.name + '.tmp')
                shutil.copyfile(artifact, temporary)
                os.replace(temporary, target)
                published.append(str(target))
                if commit_hook is not None:
                    commit_hook('published', path)
            # A source update may move records to another date; remove only its old shards.
            for old in set(previous.get('outputs', [])) - set(published):
                _inside(old, output).unlink(missing_ok=True)
            state['files'][str(path)] = dict(fingerprint=fingerprint, batch=str(batch),
                                            outputs=published, processed_at=utc_now(), **metrics)
            state['updated_at'] = utc_now()
            if commit_hook is not None:
                commit_hook('before_watermark', path)
            atomic_json(watermark, state)
            report['files_processed'] += 1
            report['files'].append(dict(file=str(path), **metrics))
            for key in ('rows_in', 'rows_out', 'rejected', 'dropped'):
                report[key] += metrics[key]
            for step in metrics['steps']:
                total = all_steps.setdefault(step['name'], dict(name=step['name'], rows_in=0, rows_out=0, rejected=0, dropped=0))
                for key in ('rows_in','rows_out','rejected','dropped'):
                    total[key] += step[key]
        # Quarantine is rebuilt from committed batches, so a re-run cannot append duplicates.
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        fd, temp = tempfile.mkstemp(dir=quarantine.parent, prefix='quarantine-', suffix='.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                for entry in state['files'].values():
                    for row in _json_lines(Path(entry['batch']) / 'rejects.jsonl'):
                        _write_line(stream, row)
            os.replace(temp, quarantine)
        finally:
            Path(temp).unlink(missing_ok=True)
        def all_records():
            for entry in state['files'].values():
                yield from _json_lines(Path(entry['batch']) / 'accepted.jsonl')
        checks = QualitySuite(build_pipeline(config).checks).run_factory(all_records)
        report['checks'] = checks['checks']
        report['quality_timestamp'] = checks['run_timestamp']
        report['quality_rows'] = sum(entry['rows_out'] for entry in state['files'].values())
        report['status'] = 'success' if checks['passed'] else 'failed'
    except Exception as exc:
        report['status'], report['error'] = 'failed', f'{type(exc).__name__}: {exc}'
        raise
    finally:
        report['end'] = utc_now()
        report['duration_seconds'] = max(0., time.monotonic() - started)
        report['steps'] = list(all_steps.values())
        atomic_json(report_path, report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(prog='minietl', description='Incremental UTC-partitioned ETL')
    parser.add_argument('config', type=Path, help='JSON config; paths relative to this file')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    try:
        report = run(args.config)
    except Exception as exc:
        print(json.dumps({'status':'failed', 'error':f'{type(exc).__name__}: {exc}'}, ensure_ascii=False))
        return 1
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report['status'] == 'success' else 1


if __name__ == '__main__':
    raise SystemExit(main())
