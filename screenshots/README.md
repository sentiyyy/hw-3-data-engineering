Добавь сюда только реальные скриншоты своей локальной RustFS Console перед финальной генерацией PDF.

Рекомендуемые снимки:
1. Бакет `hh-lake` в консоли RustFS.
2. Объект `raw/analyst/vacancies.jsonl` (исходный снимок вакансий).
3. Объекты `results/mapreduce/chunk_100.json`, `chunk_500.json`, `chunk_1000.json`.
4. `results/duckdb/skill_counts.json`, `results/verification.json` и `reports/report.pdf`.

После добавления PNG/JPG запусти `docker compose run --rm pipeline run`; генератор автоматически вставит изображения в PDF. Не используй скриншоты чужого хранилища.
