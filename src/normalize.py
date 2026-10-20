"""Normalize public vacancy records into a stable schema used by both engines."""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Any, Iterable

ANALYST_TITLE_RE = re.compile(r"аналитик|analyst", re.IGNORECASE)

SKILL_ALIASES = {
    "microsoft excel": "excel",
    "ms excel": "excel",
    "power bi desktop": "power bi",
    "microsoft power bi": "power bi",
    "postgresql": "postgresql",
    "ms sql server": "sql server",
    "microsoft sql server": "sql server",
}


def _dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _first(mapping: dict, *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None and mapping[key] != "":
            return mapping[key]
    return default


def is_analyst_title(title: Any) -> bool:
    return bool(ANALYST_TITLE_RE.search(str(title or "")))


def normalize_skill(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("name") or value.get("skill") or value.get("title") or ""
    text = unicodedata.normalize("NFKC", str(value or "")).strip().lower()
    text = text.replace("ё", "е")
    text = re.sub(r"[\t\r\n]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    text = text.strip(" ,;|\u2022")
    return SKILL_ALIASES.get(text, text)


def normalize_skills(raw: Any) -> list[str]:
    if raw is None:
        values: list[Any] = []
    elif isinstance(raw, list):
        values = raw
    elif isinstance(raw, str):
        # Accept either a JSON-encoded list or a delimited string from CSV exports.
        stripped = raw.strip()
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
                values = parsed if isinstance(parsed, list) else [raw]
            except json.JSONDecodeError:
                values = re.split(r"[;,|\n]+", raw)
        else:
            values = re.split(r"[;,|\n]+", raw)
    else:
        values = [raw]
    normalized = {normalize_skill(item) for item in values}
    normalized.discard("")
    return sorted(normalized)


def _salary_fields(raw: dict) -> tuple[float | None, float | None, str | None, bool | None, float | None]:
    salary = _dict(raw.get("salary"))
    low = _first(raw, "salary_from", "salary_min", default=salary.get("from"))
    high = _first(raw, "salary_to", "salary_max", default=salary.get("to"))
    currency = _first(raw, "salary_currency", "currency", default=salary.get("currency"))
    gross = _first(raw, "salary_gross", "gross", default=salary.get("gross"))

    def as_float(value: Any) -> float | None:
        if value is None or value == "":
            return None
        try:
            if isinstance(value, str):
                cleaned = value.strip().replace("\u00a0", "").replace("\u202f", "").replace(" ", "")
                if "," in cleaned and "." in cleaned:
                    cleaned = cleaned.replace(",", "")
                elif "," in cleaned:
                    left, right = cleaned.rsplit(",", 1)
                    cleaned = left + right if len(right) == 3 and left.lstrip("-").isdigit() else left + "." + right
                return float(cleaned)
            return float(value)
        except (TypeError, ValueError):
            return None

    low_f, high_f = as_float(low), as_float(high)
    currency_s = str(currency).upper() if currency else None
    gross_b = gross if isinstance(gross, bool) else None

    # Only RUB/RUR amounts are comparable in this project. We do not silently mix currencies.
    value = None
    if currency_s in (None, "RUB", "RUR"):
        known = [x for x in (low_f, high_f) if x is not None]
        if known:
            value = sum(known) / len(known)
    return low_f, high_f, currency_s, gross_b, value


def normalize_vacancy(raw: dict) -> dict:
    """Accept HH API objects and common flat CSV/JSONL homework datasets."""
    raw = _dict(raw)
    name = str(_first(raw, "name", "title", "vacancy_name", "position", default="")).strip()
    raw_area = raw.get("area")
    area_obj = _dict(raw_area)
    default_area = area_obj.get("name") if area_obj else (raw_area if isinstance(raw_area, str) else "Не указан")
    area_name = str(_first(raw, "area_name", "region", "city", default=default_area or "Не указан")).strip()
    area_id = _first(raw, "area_id", default=area_obj.get("id"))
    employer = _dict(raw.get("employer"))
    employer_name = str(_first(raw, "employer_name", "company", default=employer.get("name") or "Не указан")).strip()

    raw_skills = _first(raw, "skills", "key_skills", "skill_list", "hard_skills", default=[])
    skills = normalize_skills(raw_skills)
    salary_from, salary_to, salary_currency, salary_gross, salary_value = _salary_fields(raw)
    # Already-normalized records may supply a single computed salary value.
    existing_value = raw.get("salary_value")
    if existing_value is None and not isinstance(raw.get("salary"), dict):
        existing_value = raw.get("salary")
    if salary_value is None and existing_value is not None and str(raw.get("salary_currency") or "RUB").upper() in ("RUB", "RUR"):
        try:
            cleaned_value = str(existing_value).strip().replace("\u00a0", "").replace("\u202f", "").replace(" ", "").replace(",", "")
            salary_value = float(cleaned_value)
            salary_currency = str(raw.get("salary_currency") or "RUB").upper()
        except (TypeError, ValueError):
            salary_value = None

    vacancy_id = _first(raw, "id", "vacancy_id", "hh_id")
    url = str(_first(raw, "alternate_url", "url", "link", default=""))
    published_at = str(_first(raw, "published_at", "date_published", "published", default=""))
    if vacancy_id is None:
        stable = "|".join((name, area_name, url, published_at))
        vacancy_id = hashlib.sha1(stable.encode("utf-8")).hexdigest()[:16]

    return {
        "id": str(vacancy_id),
        "name": name,
        "employer_name": employer_name,
        "area_id": str(area_id) if area_id is not None else "",
        "area": area_name or "Не указан",
        "salary_from": salary_from,
        "salary_to": salary_to,
        "salary_currency": salary_currency,
        "salary_gross": salary_gross,
        "salary_value": round(salary_value, 2) if salary_value is not None else None,
        "skills": skills,
        "published_at": published_at,
        "url": url,
    }


def normalize_dataset(rows: Iterable[dict], only_analysts: bool = True) -> list[dict]:
    """Normalize and deduplicate records by ID; preserve the first occurrence."""
    result: list[dict] = []
    seen: dict[str, int] = {}
    for raw in rows:
        row = normalize_vacancy(raw)
        if only_analysts and not is_analyst_title(row["name"]):
            continue
        if row["id"] in seen:
            existing = result[seen[row["id"]]]
            if not existing["skills"] and row["skills"]:
                existing["skills"] = row["skills"]
            for field in ("area_id", "area", "employer_name", "salary_from", "salary_to", "salary_currency", "salary_gross", "salary_value", "published_at", "url"):
                if existing.get(field) in (None, "", "Не указан") and row.get(field) not in (None, "", "Не указан"):
                    existing[field] = row[field]
            continue
        seen[row["id"]] = len(result)
        result.append(row)
    return result


def read_local_dataset(path: str) -> list[dict]:
    """Read JSONL, JSON array/object, or CSV from a local homework-2 export."""
    from pathlib import Path
    import csv

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Файл с исходными вакансиями не найден: {p}")
    suffix = p.suffix.lower()
    if suffix in (".jsonl", ".ndjson"):
        rows = []
        with p.open("r", encoding="utf-8-sig") as f:
            for line_no, line in enumerate(f, 1):
                if line.strip():
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError as exc:
                        raise ValueError(f"Некорректный JSON в строке {line_no}: {exc}") from exc
        return normalize_dataset(rows)
    if suffix == ".json":
        data = json.loads(p.read_text(encoding="utf-8-sig"))
        if isinstance(data, dict):
            data = data.get("items") or data.get("vacancies") or data.get("data") or [data]
        if not isinstance(data, list):
            raise ValueError("JSON должен содержать массив вакансий или объект с items/vacancies/data")
        return normalize_dataset(data)
    if suffix == ".csv":
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        return normalize_dataset(rows)
    raise ValueError("Поддерживаются только .jsonl, .ndjson, .json и .csv")
