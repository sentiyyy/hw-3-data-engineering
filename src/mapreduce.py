"""Streaming MapReduce implementation for skill, pair and triple frequencies."""
from __future__ import annotations

from collections import Counter
from itertools import combinations
from typing import Iterable

from .normalize import is_analyst_title, normalize_skills
from .storage import iter_chunks, iter_jsonl_from_s3


def _map_batch(batch: list[dict]) -> tuple[Counter, Counter, Counter, int, int]:
    single_counts: Counter[str] = Counter()
    pair_counts: Counter[tuple[str, str]] = Counter()
    triple_counts: Counter[tuple[str, str, str]] = Counter()
    analyst_count = 0
    with_skills = 0

    for row in batch:
        if not is_analyst_title(row.get("name")):
            continue
        analyst_count += 1
        skills = normalize_skills(row.get("skills", []))
        if not skills:
            continue
        with_skills += 1
        # One skill can contribute at most once per vacancy; combinations use sorted unique skills.
        single_counts.update(skills)
        pair_counts.update(combinations(skills, 2))
        triple_counts.update(combinations(skills, 3))
    return single_counts, pair_counts, triple_counts, analyst_count, with_skills


def _pack_result(single: Counter, pairs: Counter, triples: Counter, vacancies: int, with_skills: int, chunk_size: int | None) -> dict:
    return {
        "variant": 10,
        "segment": "analyst",
        "chunk_size": chunk_size,
        "vacancies_processed": vacancies,
        "vacancies_with_skills": with_skills,
        "distinct_skills": len(single),
        "distinct_pairs": len(pairs),
        "distinct_triples": len(triples),
        "skills": [
            {"skill": skill, "vacancy_count": int(count)}
            for skill, count in sorted(single.items(), key=lambda x: (-x[1], x[0]))
        ],
        "pairs": [
            {"skill_1": combo[0], "skill_2": combo[1], "vacancy_count": int(count)}
            for combo, count in sorted(pairs.items(), key=lambda x: (-x[1], x[0]))
        ],
        "triples": [
            {"skill_1": combo[0], "skill_2": combo[1], "skill_3": combo[2], "vacancy_count": int(count)}
            for combo, count in sorted(triples.items(), key=lambda x: (-x[1], x[0]))
        ],
    }


def mapreduce_from_records(records: Iterable[dict], chunk_size: int) -> dict:
    if chunk_size < 1:
        raise ValueError("chunk_size должен быть положительным")
    single_total: Counter = Counter()
    pair_total: Counter = Counter()
    triple_total: Counter = Counter()
    vacancy_total = 0
    with_skills_total = 0
    for batch in iter_chunks(records, chunk_size):
        single, pairs, triples, vacancy_count, with_skills = _map_batch(batch)
        # Reducer: merge the counters emitted by each mapper/chunk.
        single_total.update(single)
        pair_total.update(pairs)
        triple_total.update(triples)
        vacancy_total += vacancy_count
        with_skills_total += with_skills
    return _pack_result(single_total, pair_total, triple_total, vacancy_total, with_skills_total, chunk_size)


def mapreduce_from_s3(key: str, chunk_size: int, client=None) -> dict:
    """Read the same NDJSON object from RustFS incrementally, in record chunks."""
    return mapreduce_from_records(iter_jsonl_from_s3(key=key, client=client), chunk_size)


def mapreduce_full_memory_baseline(records: list[dict]) -> dict:
    """Deliberately simple baseline for the bonus comparison: materialize all rows first."""
    return mapreduce_from_records(records, max(1, len(records)))


def count_maps(result: dict) -> dict:
    """Convert a packed result to canonical maps for exact comparison with DuckDB."""
    return {
        "skills": {row["skill"]: int(row["vacancy_count"]) for row in result["skills"]},
        "pairs": {(row["skill_1"], row["skill_2"]): int(row["vacancy_count"]) for row in result["pairs"]},
        "triples": {(row["skill_1"], row["skill_2"], row["skill_3"]): int(row["vacancy_count"]) for row in result["triples"]},
    }
