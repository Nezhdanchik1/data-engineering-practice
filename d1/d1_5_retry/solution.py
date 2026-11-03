"""D1.5 Advanced: transient-error retry with exponential backoff.

Implementation for this task; run its adjacent pytest suite from the project root.
"""
from __future__ import annotations

import functools
import random
import time


class RetryExhausted(RuntimeError):
    def __init__(self, attempts, elapsed):
        self.attempts, self.elapsed = attempts, elapsed
        super().__init__(f'retry exhausted after {attempts} attempts; elapsed {elapsed:.6f}s')


def retry(*, max_attempts=3, base=0.1, jitter=0.1, sleep=time.sleep,
          random_fn=random.random, clock=time.monotonic):
    if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or max_attempts < 1:
        raise ValueError('max_attempts must be a positive integer')
    if base < 0 or jitter < 0:
        raise ValueError('base and jitter must be nonnegative')

    def decorate(function):
        @functools.wraps(function)
        def wrapped(*args, **kwargs):
            start = clock()
            for attempt in range(max_attempts):
                try:
                    return function(*args, **kwargs)
                except (ConnectionError, TimeoutError) as exc:
                    if attempt + 1 == max_attempts:
                        raise RetryExhausted(max_attempts, clock() - start) from exc
                    sleep(base * 2 ** attempt + jitter * random_fn())
        return wrapped
    return decorate


def flaky_source():
    value = random.random()
    if value < .25:
        raise ConnectionError('temporary connection failure')
    if value < .5:
        raise TimeoutError('temporary timeout')
    if value < .6:
        raise ValueError('bad request')
    return {'status': 'ok'}
