# D2.2 · Core — Иерархии Source и Sink

Реализация: [solution.py](solution.py). Тесты: [test_solution.py](test_solution.py).

Запуск из корня проекта:

```powershell
python -m pytest d2/d2_2_sources_sinks -q
```

Решения других заданий переиспользуются через импорты; сохраняйте структуру проекта. На этой машине вместо `python` можно использовать `.\.tools\python311\python.exe`.

## Решения и ограничения

Source/Sink наследуют контекстный менеджер Connector: __enter__/__exit__ вызывает open/close. CSV/JSONL читаются по записи, без read_text/readlines. Источник сохраняет ошибки записей в rejects. Вся матрица 3 × 3 проверяется тестами. Повторный open файлового sink перезаписывает файл; для серии write используйте один with. CSV поддерживает фиксированную схему из fieldnames или первой строки.
