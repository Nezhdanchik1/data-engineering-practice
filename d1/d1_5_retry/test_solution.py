import pytest
from d1.d1_5_retry.solution import retry, RetryExhausted

def test_retry_success():
    calls, waits = [], []
    @retry(max_attempts=3, base=2, jitter=1, random_fn=lambda: .5, sleep=waits.append)
    def f():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError('temporary')
        return 42
    assert f() == 42
    assert waits == [2.5, 4.5]

@pytest.mark.parametrize('error', [ConnectionError, TimeoutError])
def test_exhausted(error):
    times = iter([10., 15.])
    @retry(max_attempts=2, sleep=lambda _: None, clock=lambda: next(times))
    def f():
        raise error('down')
    with pytest.raises(RetryExhausted) as caught:
        f()
    assert isinstance(caught.value.__cause__, error)
    assert caught.value.attempts == 2 and caught.value.elapsed == 5

def test_value_error_not_retried():
    waits = []
    @retry(sleep=waits.append)
    def f():
        raise ValueError('bad request')
    with pytest.raises(ValueError, match='bad request'):
        f()
    assert waits == []

@pytest.mark.parametrize('kwargs', [{'max_attempts': 0}, {'base': -1}, {'jitter': -1}])
def test_invalid_options(kwargs):
    with pytest.raises(ValueError):
        retry(**kwargs)
