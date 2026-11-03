# Data Engineering Capstone — minietl

Полный код — [solution.py](solution.py); он импортирует решения D1/D2. Проверки — в tests, записка о дизайне — [DESIGN.md](DESIGN.md).

## Запуск

Из корня проекта, Python 3.11+:

```powershell
python -m pytest capstone/tests -q
python -m minietl capstone/configs/orders.json
python -m minietl capstone/configs/orders.json
python -m minietl capstone/configs/clicks.json
```

На этой машине вместо python можно использовать .\.tools\python311\python.exe -B.

Заказы: 45 прочитанных, 14 записанных, 24 отклонённых, 7 отброшенных повторов. Повторный запуск — 0 новых строк. Клики: 28 прочитанных, 21 записанная, 7 отклонённых. Результаты создаются в capstone/work.

Поздний файл после первого запуска:

```powershell
Copy-Item capstone/examples/late/orders_2026-10-25.jsonl capstone/examples/data/orders/
python -m minietl capstone/configs/orders.json
```

Загрузится один заказ из более раннего дня. Тесты работают с временными копиями примеров.

## Конфиг

Пути разрешаются относительно файла JSON. Компоненты создаются через реестры D2.6.

| Ключ | Назначение |
|---|---|
| source | Тип и путь файла, каталога или glob. |
| transforms | cast, rename, dedupe, filter, date или chain по порядку. |
| checks | not_null, unique, in_range, matches_pattern, freshness. Severity: error/warning; max_age — секунды. |
| sink | Файловый приёмник csv/jsonl/partitioned; path здесь является корнем партиций. |
| partition | timestamp — поле даты, source_tz — зона наивных дат. |
| incremental | Пути watermark, report, quarantine. |
| retry | max_attempts, base, jitter для D1.5. |

Cast поддерживает str, int, decimal, positive_amount, timestamp, email, phone, strip_html. Filter из JSON использует именованные is_paid/always. Произвольный код из конфига не выполняется.

## Надёжность и результаты

Watermark хранит путь и SHA-256 каждого подтверждённого файла. Неизменённый пропускается, изменённый заменяет свои результаты, поздняя дата не мешает загрузке нового файла.

Сначала файл обрабатывается временно. Затем публикуются UTC-партиции year=/month=/day=, после них атомарно обновляется watermark. Стабильные выходные имена позволяют повторить обработку после сбоя без дописывания дублей. ConnectionError/TimeoutError повторяют приватный этап через retry D1.5.

Неверные записи попадают в rejects.jsonl с причиной, шагом и файлом; ошибки источника — также с номером строки. Карантин пересобирается из подтверждённых файлов и не дублируется при повторе. Dropped означает намеренный пропуск.

После загрузки QualitySuite проверяет весь подтверждённый набор из дискового журнала. Error даёт failed и код CLI 1 без отката загрузки; warning сообщает. Повтор без новых файлов снова проверяет качество.

В work/orders или work/clicks находятся output, watermark.json, rejects.jsonl, report.json и внутренние watermark_batches. Отчёт содержит UTC-начало/конец, длительность, строки по файлам/шагам, check results. Счётчики относятся к текущей загрузке, quality_rows — ко всему набору. Decimal записывается строкой без потери точности.

## Ограничения

- Один писатель watermark; вход и выход разделены.
- Публикация нескольких файлов не является общей транзакцией; после сбоя требуется повтор для восстановления.
- Deduplicate действует внутри файла; межфайловые повторы обнаруживает Unique.
- Изменение source/transforms/sink/partition требует нового watermark и output. Checks/retry можно менять без повторной загрузки.
- Удаление входного файла не удаляет загруженные записи; старые поколения автоматически не очищаются.
- CLI использует файловые коннекторы. InMemory доступен в отдельных Pipeline и тестах.
- Маскирование PII и сессии реализованы отдельно в D1.3/D1.6, эти конфиги их не вызывают.
- Отдельных provided-файлов не было: недельные примеры созданы для учебной работы.
