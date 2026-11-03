# D2.1 · Core — Типизированная модель Order

Реализация: [solution.py](solution.py). Тесты: [test_solution.py](test_solution.py).

Запуск из корня проекта:

```powershell
python -m pytest d2/d2_1_order_model -q
```

Решения других заданий переиспользуются через импорты; сохраняйте структуру проекта. На этой машине вместо `python` можно использовать `.\.tools\python311\python.exe`.

## Решения и ограничения

Order — frozen dataclass; __post_init__ проверяет Decimal > 0, конечность, валюту [A-Z]{3} и aware datetime, приводит его к UTC. from_row переиспользует D1.2; to_row возвращает строки. Дополнительно проверяются положительный ID и прагматичный email.
