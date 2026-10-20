from __future__ import annotations

import json
from pathlib import Path

from src.analytics import analyze_salary_and_regions
from src.mapreduce import count_maps, mapreduce_from_records
from src.normalize import normalize_dataset, normalize_skills

FIXTURE = Path(__file__).parent / "fixtures" / "sample_vacancies.jsonl"


def raw_rows():
    return [json.loads(line) for line in FIXTURE.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_skills_are_normalized_deduplicated_and_sorted():
    assert normalize_skills([" SQL ", "sql", "Microsoft Excel", "EXCEL", "Power BI Desktop"]) == ["excel", "power bi", "sql"]


def test_normalizer_filters_non_analysts_and_deduplicates_ids():
    rows = raw_rows()
    rows.append(dict(rows[0]))
    normalized = normalize_dataset(rows)
    assert len(normalized) == 13
    assert all("analyst" in row["name"].lower() or "аналитик" in row["name"].lower() for row in normalized)
    assert len({row["id"] for row in normalized}) == len(normalized)


def test_mapreduce_is_independent_of_chunk_size_and_counts_combinations():
    rows = raw_rows()
    expected = mapreduce_from_records(rows, 100)
    for size in (1, 3, 5, 100):
        actual = mapreduce_from_records(rows, size)
        assert count_maps(actual) == count_maps(expected)
        assert actual["vacancies_processed"] == expected["vacancies_processed"] == 13
        assert actual["distinct_pairs"] > actual["distinct_skills"]
        assert actual["distinct_triples"] > 0
    python_sql_pair = count_maps(expected)["pairs"].get(("python", "sql"))
    assert python_sql_pair == 5


def test_salary_and_region_tables_are_created(tmp_path):
    rows = normalize_dataset(raw_rows())
    meta = analyze_salary_and_regions(rows, tmp_path, min_combo_support=2)
    assert meta["vacancies_total"] == 13
    assert meta["vacancies_with_salary_rub"] == 13
    assert len(meta["top_regions"]) == 3
    for filename in ("salary_single.csv", "salary_combinations.csv", "salary_increases.csv", "salary_decreases.csv", "regions_summary.csv", "region_skills.csv", "region_combinations.csv", "analysis_meta.json"):
        assert (tmp_path / filename).exists(), filename


def test_pdf_report_is_generated_from_measured_artifacts(tmp_path):
    import json
    from src.analytics import export_frequency_tables
    from src.report import build_report

    rows = normalize_dataset(raw_rows())
    result = mapreduce_from_records(rows, 5)
    analyze_salary_and_regions(rows, tmp_path, min_combo_support=2)
    export_frequency_tables(result, tmp_path)
    (tmp_path / "mapreduce_chunk_1000.json").write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    (tmp_path / "duckdb_result.json").write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    verification = {
        "all_equal": True, "vacancies_equal": True, "skills_equal": True, "pairs_equal": True, "triples_equal": True,
        "mapreduce_vacancies": result["vacancies_processed"], "duckdb_vacancies": result["vacancies_processed"],
        "mapreduce_distinct_skills": result["distinct_skills"], "duckdb_distinct_skills": result["distinct_skills"],
        "mapreduce_distinct_pairs": result["distinct_pairs"], "duckdb_distinct_pairs": result["distinct_pairs"],
        "mapreduce_distinct_triples": result["distinct_triples"], "duckdb_distinct_triples": result["distinct_triples"],
    }
    (tmp_path / "verification.json").write_text(json.dumps(verification), encoding="utf-8")
    pdf_path = build_report(tmp_path)
    assert pdf_path.exists()
    assert pdf_path.stat().st_size > 10_000


def test_mapper_failure_stops_the_run_without_returning_partial_counts(monkeypatch):
    import pytest
    import src.mapreduce as mr_module

    rows = raw_rows()
    original_mapper = mr_module._map_batch
    calls = {"n": 0}

    def failing_mapper(batch):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("simulated mapper failure")
        return original_mapper(batch)

    monkeypatch.setattr(mr_module, "_map_batch", failing_mapper)
    with pytest.raises(RuntimeError, match="simulated mapper failure"):
        mr_module.mapreduce_from_records(rows, chunk_size=3)
    assert calls["n"] == 2
