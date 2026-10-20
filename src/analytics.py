"""Salary and regional analytics over the same normalized vacancy snapshot."""
from __future__ import annotations

import csv
import json
import logging
import statistics
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any

from .config import settings
from .normalize import is_analyst_title, normalize_skills

log = logging.getLogger(__name__)


def _median(values: list[float]) -> float | None:
    return round(float(statistics.median(values)), 2) if values else None


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if fieldnames:
            writer.writeheader()
            writer.writerows(rows)


def export_frequency_tables(result: dict, output_dir: Path) -> None:
    _write_csv(output_dir / "skills.csv", result["skills"], ["skill", "vacancy_count"])
    _write_csv(output_dir / "pairs.csv", result["pairs"], ["skill_1", "skill_2", "vacancy_count"])
    _write_csv(output_dir / "triples.csv", result["triples"], ["skill_1", "skill_2", "skill_3", "vacancy_count"])


def _valid_salary(row: dict) -> float | None:
    value = row.get("salary_value")
    currency = str(row.get("salary_currency") or "RUB").upper()
    if value is None or currency not in ("RUB", "RUR"):
        return None
    try:
        value = float(value)
        if value <= 0:
            return None
        return value
    except (TypeError, ValueError):
        return None


def analyze_salary_and_regions(records: list[dict], output_dir: str | Path, min_combo_support: int | None = None) -> dict:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    min_support = min_combo_support or settings.min_combo_support
    vacancies = [r for r in records if is_analyst_title(r.get("name"))]
    for row in vacancies:
        row["skills"] = normalize_skills(row.get("skills", []))

    salary_values = [_valid_salary(row) for row in vacancies]
    known_salaries = [value for value in salary_values if value is not None]
    overall_median = _median(known_salaries)
    salary_missing = len(vacancies) - len(known_salaries)

    skill_vacancy_counts: Counter[str] = Counter()
    skill_salary_values: dict[str, list[float]] = defaultdict(list)
    pair_vacancy_counts: Counter[tuple[str, str]] = Counter()
    triple_vacancy_counts: Counter[tuple[str, str, str]] = Counter()
    pair_salary_values: dict[tuple[str, str], list[float]] = defaultdict(list)
    triple_salary_values: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    skill_counts_by_vacancy = []

    for row, salary in zip(vacancies, salary_values):
        skills = row["skills"]
        skill_counts_by_vacancy.append(len(skills))
        skill_vacancy_counts.update(skills)
        if salary is not None:
            for skill in skills:
                skill_salary_values[skill].append(salary)
        pairs = list(combinations(skills, 2))
        triples = list(combinations(skills, 3))
        pair_vacancy_counts.update(pairs)
        triple_vacancy_counts.update(triples)
        if salary is not None:
            for combo in pairs:
                pair_salary_values[combo].append(salary)
            for combo in triples:
                triple_salary_values[combo].append(salary)

    single_rows = []
    for skill, values in skill_salary_values.items():
        if len(values) < min_support:
            continue
        single_rows.append({
            "skill": skill,
            "vacancy_count_all": int(skill_vacancy_counts[skill]),
            "salary_sample_count": len(values),
            "median_salary_rub": _median(values),
            "delta_vs_overall_median_rub": round(_median(values) - overall_median, 2) if overall_median is not None else None,
        })
    single_rows.sort(key=lambda r: (-(r["median_salary_rub"] or 0), -r["salary_sample_count"], r["skill"]))
    _write_csv(out / "salary_single.csv", single_rows)

    combo_rows: list[dict] = []
    for combo_type, combo_salary_map, combo_count_map in (
        ("pair", pair_salary_values, pair_vacancy_counts),
        ("triple", triple_salary_values, triple_vacancy_counts),
    ):
        for combo, values in combo_salary_map.items():
            if len(values) < min_support:
                continue
            # Compare the combination median with the median for its most frequently occurring individual skill.
            base_skill = sorted(combo, key=lambda s: (-skill_vacancy_counts[s], s))[0]
            base_values = skill_salary_values.get(base_skill, [])
            base_median = _median(base_values)
            combo_median = _median(values)
            delta = round(combo_median - base_median, 2) if base_median is not None else None
            delta_pct = round((delta / base_median) * 100, 2) if delta is not None and base_median else None
            combo_rows.append({
                "combination_type": combo_type,
                "combination": " + ".join(combo),
                "skills": json.dumps(list(combo), ensure_ascii=False),
                "vacancy_count_all": int(combo_count_map[combo]),
                "salary_sample_count": len(values),
                "median_salary_rub": combo_median,
                "baseline_skill": base_skill,
                "baseline_skill_vacancy_count": int(skill_vacancy_counts[base_skill]),
                "baseline_salary_sample_count": len(base_values),
                "baseline_median_salary_rub": base_median,
                "delta_vs_baseline_rub": delta,
                "delta_vs_baseline_pct": delta_pct,
            })
    combo_rows.sort(key=lambda r: (r["combination_type"], -(r["vacancy_count_all"]), r["combination"]))
    _write_csv(out / "salary_combinations.csv", combo_rows)
    increases = sorted([r for r in combo_rows if r["delta_vs_baseline_rub"] is not None], key=lambda r: (-r["delta_vs_baseline_rub"], -r["salary_sample_count"], r["combination"]))[:10]
    decreases = sorted([r for r in combo_rows if r["delta_vs_baseline_rub"] is not None], key=lambda r: (r["delta_vs_baseline_rub"], -r["salary_sample_count"], r["combination"]))[:10]
    _write_csv(out / "salary_increases.csv", increases)
    _write_csv(out / "salary_decreases.csv", decreases)

    # Select the three regions with the most analyst vacancies; this makes selection reproducible.
    region_counts = Counter(str(r.get("area") or "Не указан") for r in vacancies)
    ranked_regions = sorted(
        [(name, count) for name, count in region_counts.items() if name and name != "Не указан"],
        key=lambda item: (-item[1], item[0]),
    )[:3]
    if len(ranked_regions) < 3 and "Не указан" in region_counts:
        ranked_regions.append(("Не указан", region_counts["Не указан"]))
    top_regions = [name for name, _ in ranked_regions]

    region_summary = []
    region_skills_rows = []
    region_combo_rows = []
    for region in top_regions:
        region_vacancies = [r for r in vacancies if str(r.get("area") or "Не указан") == region]
        region_salaries = [v for r in region_vacancies if (v := _valid_salary(r)) is not None]
        region_skill_counts: Counter[str] = Counter()
        region_pairs: Counter[tuple[str, str]] = Counter()
        region_triples: Counter[tuple[str, str, str]] = Counter()
        region_pair_salaries: dict[tuple[str, str], list[float]] = defaultdict(list)
        region_triple_salaries: dict[tuple[str, str, str], list[float]] = defaultdict(list)
        for row in region_vacancies:
            skills = row["skills"]
            salary = _valid_salary(row)
            region_skill_counts.update(skills)
            pairs = list(combinations(skills, 2))
            triples = list(combinations(skills, 3))
            region_pairs.update(pairs)
            region_triples.update(triples)
            if salary is not None:
                for combo in pairs:
                    region_pair_salaries[combo].append(salary)
                for combo in triples:
                    region_triple_salaries[combo].append(salary)

        region_summary.append({
            "region": region,
            "vacancy_count": len(region_vacancies),
            "salary_sample_count": len(region_salaries),
            "salary_coverage_pct": round(100 * len(region_salaries) / len(region_vacancies), 2) if region_vacancies else 0,
            "median_salary_rub": _median(region_salaries),
            "median_skills_per_vacancy": _median([float(len(r["skills"])) for r in region_vacancies]),
            "vacancies_with_skills": sum(bool(r["skills"]) for r in region_vacancies),
            "distinct_skills": len(region_skill_counts),
            "distinct_pairs": len(region_pairs),
            "distinct_triples": len(region_triples),
        })
        for skill, count in sorted(region_skill_counts.items(), key=lambda item: (-item[1], item[0]))[:10]:
            region_skills_rows.append({
                "region": region,
                "skill": skill,
                "vacancy_count": count,
                "share_of_all_region_vacancies_pct": round(100 * count / len(region_vacancies), 2) if region_vacancies else 0,
            })
        combo_candidates = []
        for kind, counter, salary_map in (
            ("pair", region_pairs, region_pair_salaries),
            ("triple", region_triples, region_triple_salaries),
        ):
            for combo, count in counter.items():
                combo_candidates.append({
                    "region": region,
                    "combination_type": kind,
                    "combination": " + ".join(combo),
                    "vacancy_count": count,
                    "salary_sample_count": len(salary_map.get(combo, [])),
                    "median_salary_rub": _median(salary_map.get(combo, [])),
                })
        combo_candidates.sort(key=lambda r: (-r["vacancy_count"], r["combination_type"], r["combination"]))
        # Keep top five pairs and top five triples separately, not only the globally common type.
        for kind in ("pair", "triple"):
            region_combo_rows.extend([r for r in combo_candidates if r["combination_type"] == kind][:5])

    _write_csv(out / "regions_summary.csv", region_summary)
    _write_csv(out / "region_skills.csv", region_skills_rows)
    _write_csv(out / "region_combinations.csv", region_combo_rows)

    skills_with_data = sum(bool(r["skills"]) for r in vacancies)
    total_pair_occurrences = sum(pair_vacancy_counts.values())
    total_triple_occurrences = sum(triple_vacancy_counts.values())
    skill_counts_nonempty = [len(r["skills"]) for r in vacancies if r["skills"]]
    meta = {
        "variant": 10,
        "segment": "analyst",
        "vacancies_total": len(vacancies),
        "vacancies_with_skills": skills_with_data,
        "vacancies_with_salary_rub": len(known_salaries),
        "vacancies_without_comparable_salary": salary_missing,
        "overall_median_salary_rub": overall_median,
        "mean_skills_per_vacancy_with_skills": round(statistics.mean(skill_counts_nonempty), 2) if skill_counts_nonempty else 0,
        "total_skill_occurrences": sum(skill_vacancy_counts.values()),
        "distinct_skills": len(skill_vacancy_counts),
        "total_pair_occurrences": total_pair_occurrences,
        "distinct_pairs": len(pair_vacancy_counts),
        "total_triple_occurrences": total_triple_occurrences,
        "distinct_triples": len(triple_vacancy_counts),
        "min_combo_support": min_support,
        "top_regions": top_regions,
        "salary_method": "midpoint of published range; if one boundary exists, that boundary; RUB/RUR only",
        "salary_limitations": "Gross/net tax basis is not harmonized; salary differences are descriptive, not causal.",
    }
    (out / "analysis_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(
        "Аналитика: вакансий=%d, зарплат=%d, навыков=%d, уникальных пар=%d, троек=%d, регионов=%s",
        len(vacancies), len(known_salaries), len(skill_vacancy_counts), len(pair_vacancy_counts), len(triple_vacancy_counts), top_regions,
    )
    return meta
