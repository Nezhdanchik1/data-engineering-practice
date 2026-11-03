# D2.6 · Challenge — Pipeline из JSON и реестры плагинов

Реализация: [solution.py](solution.py). Тесты: [test_solution.py](test_solution.py).

Запуск из корня проекта:

```powershell
python -m pytest d2/d2_6_registry -q
```

Решения других заданий переиспользуются через импорты; сохраняйте структуру проекта. На этой машине вместо `python` можно использовать `.\.tools\python311\python.exe`.

## Решения и ограничения

Source, Sink, Transform и Check регистрируют подклассы через __init_subclass__(name=...). construct использует registry и inspect.signature, без цепочки if по типам. JSON задаёт только разрешённые конвертеры/предикаты, eval не используется. Ошибки называют путь до неизвестного типа/отсутствующего параметра, например transforms[1].key. Freshness.max_age в JSON задаётся секундами. build_pipeline возвращает собранный объект; проверки запускаются QualitySuite, capstone делает это автоматически после загрузки.
