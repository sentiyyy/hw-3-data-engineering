"""Collect current analyst vacancies through the public HeadHunter API."""
from __future__ import annotations

import logging
import math
import time
from datetime import datetime, timezone
from typing import Any

import requests

from .config import settings
from .normalize import is_analyst_title, normalize_dataset
from .storage import upload_jsonl

log = logging.getLogger(__name__)
HH_API = "https://api.hh.ru"
SEARCH_TERMS = ("аналитик", "analyst")


class HHAPIError(RuntimeError):
    pass


def _request_json(session: requests.Session, url: str, params: dict | None = None) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            response = session.get(url, params=params, timeout=(10, 45))
            if response.status_code == 404:
                raise HHAPIError(f"Вакансия или endpoint не найден (HTTP 404): {url}")
            if response.status_code == 429 or response.status_code >= 500:
                last_error = HHAPIError(f"HH API HTTP {response.status_code}")
                wait = min(2 ** (attempt - 1), 12)
                log.warning("HH API вернул %s; повтор через %s сек.", response.status_code, wait)
                time.sleep(wait)
                continue
            if response.status_code in (401, 403):
                raise HHAPIError(
                    f"HH API отказал в доступе (HTTP {response.status_code}). Возможно, требуется авторизация "
                    "или сработала капча. Можно импортировать выгрузку из ДЗ № 2: "
                    "docker compose run --rm pipeline import-file /app/data/vacancies.jsonl"
                )
            response.raise_for_status()
            try:
                payload = response.json()
            except ValueError as exc:
                raise HHAPIError("HH API вернул не JSON (возможна капча или страница ошибки).") from exc
            if not isinstance(payload, dict):
                raise HHAPIError("Неожиданный формат ответа HH API: ожидался JSON-объект.")
            return payload
        except HHAPIError:
            raise
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            wait = min(2 ** (attempt - 1), 12)
            if attempt < 5:
                log.warning("Ошибка запроса HH API (%s); повтор через %s сек.", exc, wait)
                time.sleep(wait)
    raise HHAPIError(f"Не удалось получить ответ от HH API после 5 попыток: {last_error}")


def collect_vacancies(limit: int = 1200) -> list[dict]:
    if limit < 1:
        raise ValueError("limit должен быть положительным")
    session = requests.Session()
    user_agent = settings.hh_user_agent
    session.headers.update({
        "User-Agent": user_agent,
        "HH-User-Agent": user_agent,
        "Accept": "application/json",
    })

    per_term = max(100, math.ceil(limit / len(SEARCH_TERMS)))
    found: dict[str, dict] = {}
    for term in SEARCH_TERMS:
        log.info("Поиск вакансий по названию: %r", term)
        term_found = 0
        page = 0
        while term_found < per_term and page < 20:
            params = {
                "text": term,
                "search_field": "name",
                "area": "113",  # Россия: даёт материал для сравнения регионов
                "per_page": 100,
                "page": page,
                "host": "hh.ru",
            }
            payload = _request_json(session, f"{HH_API}/vacancies", params=params)
            items = payload.get("items") or []
            if not items:
                break
            for item in items:
                title = str(item.get("name") or "")
                vacancy_id = str(item.get("id") or "")
                if not vacancy_id or not is_analyst_title(title):
                    continue
                if vacancy_id not in found:
                    found[vacancy_id] = item
                    term_found += 1
                if term_found >= per_term:
                    break
            pages = int(payload.get("pages") or 0)
            page += 1
            if pages and page >= pages:
                break
            time.sleep(max(0.0, settings.hh_delay))
        log.info("По запросу %r отобрано %d вакансий; всего уникальных %d", term, term_found, len(found))

    # Limit the selected list deterministically; newest postings first when dates are available.
    candidates = list(found.values())
    candidates.sort(key=lambda x: str(x.get("published_at") or ""), reverse=True)
    candidates = candidates[:limit]
    detailed: list[dict] = []
    for idx, item in enumerate(candidates, 1):
        vacancy_id = str(item["id"])
        try:
            details = _request_json(session, f"{HH_API}/vacancies/{vacancy_id}")
            # The detail endpoint has key_skills, employer, area and salary fields.
            detailed.append(details)
        except HHAPIError as exc:
            if "HTTP 401" in str(exc) or "HTTP 403" in str(exc) or "капча" in str(exc).lower():
                raise HHAPIError(
                    "HeadHunter заблокировал получение подробностей вакансий; без key_skills анализ будет неполным. "
                    "Используйте датасет из ДЗ № 2 через команду import-file. Исходная ошибка: " + str(exc)
                ) from exc
            log.warning("Не удалось получить детали вакансии %s (%s): %s", vacancy_id, item.get("name"), exc)
            # Keep the search result as a fallback for isolated missing/deleted vacancy details.
            detailed.append(item)
        if idx % 100 == 0 or idx == len(candidates):
            log.info("Получены детали %d/%d вакансий", idx, len(candidates))
        time.sleep(max(0.0, settings.hh_delay))

    normalized = normalize_dataset(detailed, only_analysts=True)
    if not normalized:
        raise HHAPIError(
            "После фильтрации не осталось вакансий аналитиков. Проверьте доступ к HH API "
            "или импортируйте файл из предыдущего домашнего задания."
        )
    log.info("Итоговый датасет: %d уникальных вакансий аналитиков", len(normalized))
    return normalized


def collect_and_upload(limit: int = 1200) -> list[dict]:
    rows = collect_vacancies(limit=limit)
    upload_jsonl(rows)
    from .storage import upload_json
    upload_json(
        "metadata/analyst_dataset.json",
        {
            "variant": 10,
            "segment": "analyst",
            "source": "HeadHunter public API",
            "snapshot_utc": datetime.now(timezone.utc).isoformat(),
            "vacancies": len(rows),
            "object": f"s3://{settings.bucket}/{settings.object_key}",
            "note": "Only published salary values in RUB/RUR are used for salary comparisons; missing salaries remain null.",
        },
    )
    return rows
