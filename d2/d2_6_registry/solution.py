"""D2.6 Challenge: plugin registries and JSON-driven pipelines.

Implementation for this task; run its adjacent pytest suite from the project root.
"""
from __future__ import annotations

import inspect
import json
from datetime import timedelta
from pathlib import Path
from d2.d2_2_sources_sinks.solution import Source, Sink
# Import optional sink implementations once to populate the shared registry.
from d2.d2_5_lsp.solution import BatchSink, DryRunSink, WarehouseSink
from d2.d2_3_transforms.solution import Transform, Pipeline, SCHEMAS, CONVERTERS
from d2.d2_4_quality.solution import Check


class ConfigError(ValueError):
    pass


PREDICATES = {'is_paid': lambda row: row.get('status') == 'paid',
              'always': lambda row: True}


def _cast(params, path):
    schema = params.get('schema')
    if isinstance(schema, str) and schema not in SCHEMAS:
        raise ConfigError(f'{path}.schema: unknown schema {schema!r}')
    if isinstance(schema, dict):
        for column, converter in schema.items():
            if not isinstance(converter, str) or converter not in CONVERTERS:
                raise ConfigError(f'{path}.schema.{column}: unknown converter {converter!r}')
    elif not isinstance(schema, str):
        raise ConfigError(f'{path}.schema: expected a name or object')
    return params


def _filter(params, path):
    if 'predicate' in params:
        name = params['predicate']
        try:
            params['predicate'] = PREDICATES[name]
        except (KeyError, TypeError) as exc:
            raise ConfigError(f'{path}.predicate: unknown predicate {name!r}') from exc
    return params


def _freshness(params, path):
    if 'max_age' in params:
        try:
            params['max_age'] = timedelta(seconds=float(params['max_age']))
        except (ValueError, TypeError, OverflowError) as exc:
            raise ConfigError(f'{path}.max_age: expected seconds') from exc
    return params


def _chain(params, path):
    if 'transforms' in params:
        if not isinstance(params['transforms'], list):
            raise ConfigError(f'{path}.transforms: expected list')
        params['transforms'] = [construct(Transform, value, f'{path}.transforms[{i}]')
                                for i, value in enumerate(params['transforms'])]
    return params


ADAPTERS = {(Transform, 'cast'): _cast, (Transform, 'filter'): _filter,
            (Transform, 'chain'): _chain, (Check, 'freshness'): _freshness}


def construct(base, spec, path):
    if not isinstance(spec, dict):
        raise ConfigError(f'{path}: expected object')
    if 'type' not in spec:
        raise ConfigError(f'{path}.type: missing parameter')
    try:
        cls = base.registry[spec['type']]
    except (KeyError, TypeError) as exc:
        raise ConfigError(f'{path}.type: unknown type {spec["type"]!r}') from exc
    params = {k: v for k, v in spec.items() if k != 'type'}
    signature = inspect.signature(cls)
    for name, parameter in signature.parameters.items():
        if parameter.default is inspect.Parameter.empty and parameter.kind not in {
                inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL} and name not in params:
            raise ConfigError(f'{path}.{name}: missing parameter')
    for name in params:
        if name not in signature.parameters:
            raise ConfigError(f'{path}.{name}: unknown parameter')
    params = ADAPTERS.get((base, spec['type']), lambda p, _: p)(params, path)
    try:
        signature.bind(**params)
        return cls(**params)
    except (TypeError, ValueError, KeyError, OverflowError, AttributeError) as exc:
        raise ConfigError(f'{path}: {exc}') from exc


def read_config(path):
    try:
        config = json.loads(Path(path).read_text(encoding='utf-8').lstrip('\ufeff'))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ConfigError(f'$: invalid JSON config: {exc}') from exc
    if not isinstance(config, dict):
        raise ConfigError('$: expected object')
    return config


def build_pipeline(config):
    if isinstance(config, (str, Path)):
        config = read_config(config)
    if not isinstance(config, dict):
        raise ConfigError('$: expected object')
    for key in ('source', 'sink'):
        if key not in config:
            raise ConfigError(f'{key}: missing parameter')
    for key in ('transforms', 'checks'):
        if not isinstance(config.get(key, []), list):
            raise ConfigError(f'{key}: expected list')
    return Pipeline(construct(Source, config['source'], 'source'),
                    [construct(Transform, s, f'transforms[{i}]') for i, s in enumerate(config.get('transforms', []))],
                    construct(Sink, config['sink'], 'sink'),
                    [construct(Check, s, f'checks[{i}]') for i, s in enumerate(config.get('checks', []))])
