# D1.5 · Advanced — Повторы с exponential backoff

Реализация: [solution.py](solution.py). Тесты: [test_solution.py](test_solution.py).

Запуск из корня проекта:

```powershell
python -m pytest d1/d1_5_retry -q
```

Решения других заданий переиспользуются через импорты; сохраняйте структуру проекта. На этой машине вместо `python` можно использовать `.\.tools\python311\python.exe`.

## Решения и ограничения

max_attempts включает первый вызов; задержки base * 2 ** attempt + jitter * random_fn(), attempt начинается с 0. Повторяются только ConnectionError/TimeoutError. RetryExhausted сохраняет причину, число попыток и фактическое monotonic-время; sleep, random_fn, clock внедряются в тестах.
