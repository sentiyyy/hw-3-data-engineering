"""Command-line entry point for the complete variant 10 analyst pipeline."""
from __future__ import annotations

import argparse
import csv
import json
import logging
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from .analytics import analyze_salary_and_regions, export_frequency_tables
from .collect_hh import HHAPIError, collect_and_upload
from .config import settings
from .duckdb_sql import duckdb_count_skills, prepare_httpfs
from .mapreduce import count_maps, mapreduce_from_s3, mapreduce_full_memory_baseline
from .normalize import read_local_dataset
from .profiling import profile_call
from .report import build_report
from .storage import (
    ensure_bucket, iter_jsonl_from_s3, list_objects, load_all_records,
    s3_client, upload_file, upload_json, upload_jsonl,
)


def setup_logging() -> Path:
    artifacts = Path(settings.artifacts_dir)
    artifacts.mkdir(parents=True, exist_ok=True)
    log_path = artifacts / "run.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path, mode="w", encoding="utf-8")],
        force=True,
    )
    return log_path


def _save_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _save_csv(path: Path, rows: list[dict], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if fields:
            writer.writeheader()
            writer.writerows(rows)


def _canonical_diff(left: dict, right: dict, left_name: str, right_name: str) -> tuple[bool, list[dict]]:
    left_maps, right_maps = count_maps(left), count_maps(right)
    differences: list[dict] = []
    for category in ("skills", "pairs", "triples"):
        left_map, right_map = left_maps[category], right_maps[category]
        for key in sorted(set(left_map) | set(right_map), key=lambda x: str(x)):
            a, b = left_map.get(key, 0), right_map.get(key, 0)
            if a != b:
                differences.append({
                    "category": category,
                    "key": list(key) if isinstance(key, tuple) else key,
                    left_name: a,
                    right_name: b,
                })
                if len(differences) >= 100:
                    break
        if len(differences) >= 100:
            break
    metadata_equal = (
        left.get("vacancies_processed") == right.get("vacancies_processed")
        and left.get("vacancies_with_skills") == right.get("vacancies_with_skills")
    )
    if not metadata_equal:
        differences.insert(0, {
            "category": "metadata",
            "key": "vacancy counts",
            left_name: [left.get("vacancies_processed"), left.get("vacancies_with_skills")],
            right_name: [right.get("vacancies_processed"), right.get("vacancies_with_skills")],
        })
    return metadata_equal and not differences, differences


def _profile_summary(metrics: list[dict]) -> list[dict]:
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in metrics:
        groups[(str(row["method"]), str(row.get("chunk_size") if row.get("chunk_size") is not None else ""))].append(row)
    result = []
    for (method, chunk), rows in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1])):
        times = [float(r["elapsed_seconds"]) for r in rows]
        result.append({
            "method": method,
            "chunk_size": chunk,
            "attempts": len(rows),
            "mean_elapsed_seconds": round(statistics.mean(times), 6),
            "min_elapsed_seconds": round(min(times), 6),
            "max_elapsed_seconds": round(max(times), 6),
            "max_rss_peak_mb": max(float(r["rss_peak_mb"]) for r in rows),
            "max_rss_peak_delta_mb": max(float(r["rss_peak_delta_mb"]) for r in rows),
            "max_tracemalloc_peak_mb": max(float(r["tracemalloc_peak_mb"]) for r in rows),
        })
    return result


def _optimization_table(metrics: list[dict], largest_chunk: int) -> list[dict]:
    before = [r for r in metrics if r["method"] == "Baseline (full memory)"]
    after = [r for r in metrics if r["method"] == "MapReduce" and r.get("chunk_size") == largest_chunk]
    if not before or not after:
        return []
    before_time = statistics.mean(float(r["elapsed_seconds"]) for r in before)
    after_time = statistics.mean(float(r["elapsed_seconds"]) for r in after)
    before_rss = max(float(r["rss_peak_delta_mb"]) for r in before)
    after_rss = max(float(r["rss_peak_delta_mb"]) for r in after)
    before_python = max(float(r["tracemalloc_peak_mb"]) for r in before)
    after_python = max(float(r["tracemalloc_peak_mb"]) for r in after)
    return [
        {
            "approach": "До оптимизации: полная загрузка объекта в память",
            "mean_elapsed_seconds": round(before_time, 6),
            "max_rss_peak_mb": round(max(float(r["rss_peak_mb"]) for r in before), 3),
            "max_rss_peak_delta_mb": round(before_rss, 3),
            "max_tracemalloc_peak_mb": round(before_python, 3),
            "time_improvement_pct": 0,
            "rss_improvement_pct": 0,
        },
        {
            "approach": f"После оптимизации: потоковый MapReduce, чанк {largest_chunk}",
            "mean_elapsed_seconds": round(after_time, 6),
            "max_rss_peak_mb": round(max(float(r["rss_peak_mb"]) for r in after), 3),
            "max_rss_peak_delta_mb": round(after_rss, 3),
            "max_tracemalloc_peak_mb": round(after_python, 3),
            "time_improvement_pct": round((before_time - after_time) / before_time * 100, 2) if before_time else 0,
            "rss_improvement_pct": round((before_rss - after_rss) / before_rss * 100, 2) if before_rss else 0,
        },
    ]


def _save_and_upload_result(result: dict, size: int, artifacts: Path, client) -> None:
    local_path = artifacts / f"mapreduce_chunk_{size}.json"
    _save_json(local_path, result)
    upload_json(f"results/mapreduce/chunk_{size}.json", result, client=client)


def run_pipeline(chunk_sizes: tuple[int, ...] | None = None, attempts: int | None = None) -> dict:
    logger = logging.getLogger(__name__)
    artifacts = Path(settings.artifacts_dir)
    artifacts.mkdir(parents=True, exist_ok=True)
    chunk_sizes = tuple(chunk_sizes or settings.chunk_sizes)
    attempts = max(3, attempts or settings.profile_attempts)
    if len(chunk_sizes) < 3:
        raise ValueError("Для задания нужны три размера чанка, например --chunk-sizes 100,500,1000")
    client = s3_client()
    ensure_bucket(client)
    try:
        source_response = client.get_object(Bucket=settings.bucket, Key="metadata/analyst_dataset.json")
        source_metadata = json.loads(source_response["Body"].read().decode("utf-8"))
        _save_json(artifacts / "source_metadata.json", source_metadata)
    except Exception as exc:
        logger.warning("Не удалось прочитать metadata/analyst_dataset.json: %s", exc)

    logger.info("Этап 1/7: подготовка DuckDB httpfs")
    prepare_httpfs()

    logger.info("Этап 2/7: потоковый MapReduce из s3://%s/%s", settings.bucket, settings.object_key)
    profiling: list[dict] = []
    mr_results: dict[int, dict] = {}
    for size in chunk_sizes:
        reference_result = None
        for attempt in range(1, attempts + 1):
            result, metrics = profile_call(
                lambda size=size: mapreduce_from_s3(settings.object_key, size, client=client),
                attempt=attempt, method="MapReduce", chunk_size=size,
            )
            profiling.append(metrics)
            if reference_result is None:
                reference_result = result
            else:
                equal, diff = _canonical_diff(reference_result, result, "first_run", "current_run")
                if not equal:
                    _save_json(artifacts / "mapreduce_repeat_mismatch.json", {"chunk_size": size, "differences": diff})
                    raise RuntimeError(f"MapReduce дал разные результаты на повторных запусках для чанка {size}")
        assert reference_result is not None
        mr_results[size] = reference_result
        _save_and_upload_result(reference_result, size, artifacts, client)
        logger.info("MapReduce чанк=%d: навыков=%d, пар=%d, троек=%d", size, reference_result["distinct_skills"], reference_result["distinct_pairs"], reference_result["distinct_triples"])

    logger.info("Этап 3/7: независимый расчёт DuckDB SQL по объекту в RustFS")
    duck_result = None
    for attempt in range(1, attempts + 1):
        result, metrics = profile_call(
            lambda: duckdb_count_skills(settings.object_key),
            attempt=attempt, method="DuckDB SQL", chunk_size=None,
        )
        profiling.append(metrics)
        if duck_result is None:
            duck_result = result
        else:
            equal, diff = _canonical_diff(duck_result, result, "first_run", "current_run")
            if not equal:
                _save_json(artifacts / "duckdb_repeat_mismatch.json", {"differences": diff})
                raise RuntimeError("DuckDB дал разные результаты на повторных запусках")
    assert duck_result is not None
    _save_json(artifacts / "duckdb_result.json", duck_result)
    upload_json("results/duckdb/skill_counts.json", duck_result, client=client)

    logger.info("Этап 4/7: контрольное сравнение MapReduce и DuckDB")
    largest_chunk = max(chunk_sizes)
    mr_reference = mr_results[largest_chunk]
    mr_duck_equal, differences = _canonical_diff(mr_reference, duck_result, "mapreduce", "duckdb")
    chunk_checks = {}
    for size, result in mr_results.items():
        same, chunk_diff = _canonical_diff(mr_reference, result, "reference", "chunk")
        chunk_checks[str(size)] = {"equal_to_reference": same, "differences": chunk_diff[:10]}
    verification = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "source_object": f"s3://{settings.bucket}/{settings.object_key}",
        "variant": 10,
        "segment": "analyst",
        "all_equal": mr_duck_equal and all(x["equal_to_reference"] for x in chunk_checks.values()),
        "vacancies_equal": mr_reference.get("vacancies_processed") == duck_result.get("vacancies_processed"),
        "skills_equal": count_maps(mr_reference)["skills"] == count_maps(duck_result)["skills"],
        "pairs_equal": count_maps(mr_reference)["pairs"] == count_maps(duck_result)["pairs"],
        "triples_equal": count_maps(mr_reference)["triples"] == count_maps(duck_result)["triples"],
        "mapreduce_vacancies": mr_reference.get("vacancies_processed"),
        "duckdb_vacancies": duck_result.get("vacancies_processed"),
        "mapreduce_distinct_skills": mr_reference.get("distinct_skills"),
        "duckdb_distinct_skills": duck_result.get("distinct_skills"),
        "mapreduce_distinct_pairs": mr_reference.get("distinct_pairs"),
        "duckdb_distinct_pairs": duck_result.get("distinct_pairs"),
        "mapreduce_distinct_triples": mr_reference.get("distinct_triples"),
        "duckdb_distinct_triples": duck_result.get("distinct_triples"),
        "chunk_checks": chunk_checks,
        "differences_sample": differences[:100],
    }
    _save_json(artifacts / "verification.json", verification)
    upload_json("results/verification.json", verification, client=client)

    logger.info("Этап 5/7: бонусное сравнение full-memory baseline и потоковой обработки")
    baseline_reference = None
    for attempt in range(1, attempts + 1):
        def baseline_operation():
            all_rows = list(iter_jsonl_from_s3(key=settings.object_key, client=client))
            return mapreduce_full_memory_baseline(all_rows)
        result, metrics = profile_call(baseline_operation, attempt=attempt, method="Baseline (full memory)", chunk_size=None)
        profiling.append(metrics)
        if baseline_reference is None:
            baseline_reference = result
        else:
            equal, diff = _canonical_diff(baseline_reference, result, "first_run", "current_run")
            if not equal:
                raise RuntimeError("Baseline дал разные результаты на повторных запусках")
    assert baseline_reference is not None
    baseline_equal, baseline_diff = _canonical_diff(mr_reference, baseline_reference, "streaming", "baseline")
    verification["baseline_equal_to_streaming"] = baseline_equal
    if not baseline_equal:
        verification["baseline_differences_sample"] = baseline_diff[:100]
        verification["all_equal"] = False
    _save_json(artifacts / "verification.json", verification)
    upload_json("results/verification.json", verification, client=client)
    _save_json(artifacts / "baseline_result.json", baseline_reference)
    upload_json("results/mapreduce/baseline_full_memory.json", baseline_reference, client=client)

    logger.info("Этап 6/7: анализ зарплат и регионов")
    records = load_all_records(key=settings.object_key, client=client)
    meta = analyze_salary_and_regions(records, artifacts, settings.min_combo_support)
    export_frequency_tables(mr_reference, artifacts)

    _save_csv(artifacts / "profiling.csv", profiling, [
        "method", "chunk_size", "attempt", "elapsed_seconds", "rss_start_mb", "rss_peak_mb", "rss_peak_delta_mb", "tracemalloc_peak_mb",
    ])
    summary = _profile_summary(profiling)
    _save_csv(artifacts / "profiling_summary.csv", summary, [
        "method", "chunk_size", "attempts", "mean_elapsed_seconds", "min_elapsed_seconds", "max_elapsed_seconds",
        "max_rss_peak_mb", "max_rss_peak_delta_mb", "max_tracemalloc_peak_mb",
    ])
    optimization = _optimization_table(profiling, largest_chunk)
    _save_csv(artifacts / "optimization.csv", optimization, [
        "approach", "mean_elapsed_seconds", "max_rss_peak_mb", "max_rss_peak_delta_mb", "max_tracemalloc_peak_mb",
        "time_improvement_pct", "rss_improvement_pct",
    ])
    upload_json("results/profiling/profile_runs.json", profiling, client=client)
    upload_json("results/profiling/profile_summary.json", summary, client=client)
    upload_json("results/analysis/analysis_meta.json", meta, client=client)

    # Upload analytical tables so they are available both locally and in RustFS.
    table_files = [
        "skills.csv", "pairs.csv", "triples.csv", "salary_single.csv", "salary_combinations.csv",
        "salary_increases.csv", "salary_decreases.csv", "regions_summary.csv", "region_skills.csv",
        "region_combinations.csv", "profiling.csv", "profiling_summary.csv", "optimization.csv",
    ]
    for filename in table_files:
        path = artifacts / filename
        if path.exists():
            upload_file(f"results/analysis/{filename}", str(path), "text/csv; charset=utf-8", client=client)

    logger.info("Этап 7/7: генерация PDF-отчёта")
    # Store a manifest before PDF generation so the report can list the verified object keys.
    object_list = list_objects(prefix="", client=client)
    _save_json(artifacts / "object_store_manifest.json", object_list)
    upload_json("results/object_store_manifest.json", {"objects": object_list}, client=client)
    pdf_path = build_report(artifacts, artifacts / "report.pdf")
    upload_file("reports/report.pdf", str(pdf_path), "application/pdf", client=client)
    # Refresh the manifest one last time so it also includes the final PDF.
    object_list = list_objects(prefix="", client=client)
    _save_json(artifacts / "object_store_manifest.json", object_list)
    upload_json("results/object_store_manifest.json", {"objects": object_list}, client=client)

    logger.info("Готово. PDF: %s", pdf_path)
    logger.info("Объекты RustFS: http://localhost:9001 (бакет %s)", settings.bucket)
    if not verification.get("all_equal"):
        logger.error("ВНИМАНИЕ: проверка результатов не пройдена; смотрите artifacts/verification.json")
        raise RuntimeError("Результаты MapReduce и DuckDB не совпали. Отчёт и диагностические файлы сохранены.")
    return {"verification": verification, "analysis": meta, "report": str(pdf_path), "profiling": profiling}


def _write_import_metadata(count: int, source: str) -> None:
    upload_json("metadata/analyst_dataset.json", {
        "variant": 10,
        "segment": "analyst",
        "source": source,
        "snapshot_utc": datetime.now(timezone.utc).isoformat(),
        "vacancies": count,
        "object": f"s3://{settings.bucket}/{settings.object_key}",
        "note": "Only published salary values in RUB/RUR are used for salary comparisons; missing salaries remain null.",
    })


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    parser = argparse.ArgumentParser(description="Вариант 10 — аналитик: RustFS + MapReduce + DuckDB")
    sub = parser.add_subparsers(dest="command", required=True)

    all_cmd = sub.add_parser("all", help="Собрать вакансии из HH API, загрузить в RustFS и выполнить весь анализ")
    all_cmd.add_argument("--limit", type=int, default=1200, help="Максимум вакансий для сбора; по умолчанию 1200")
    all_cmd.add_argument("--chunk-sizes", default=",".join(map(str, settings.chunk_sizes)), help="Три размера чанка, например 100,500,1000")
    all_cmd.add_argument("--attempts", type=int, default=settings.profile_attempts, help="Число повторов; минимум 3")

    import_cmd = sub.add_parser("import-file", help="Импортировать датасет из ДЗ № 2 или локальной выгрузки")
    import_cmd.add_argument("path", help="Путь к .jsonl, .ndjson, .json или .csv внутри контейнера")

    run_cmd = sub.add_parser("run", help="Запустить анализ уже загруженного объекта в RustFS")
    run_cmd.add_argument("--chunk-sizes", default=",".join(map(str, settings.chunk_sizes)), help="Три размера чанка, например 100,500,1000")
    run_cmd.add_argument("--attempts", type=int, default=settings.profile_attempts, help="Число повторов; минимум 3")

    sub.add_parser("list-objects", help="Показать объекты бакета hh-lake")
    args = parser.parse_args(argv)

    try:
        if args.command == "all":
            logging.getLogger(__name__).info("Сбор актуальных вакансий HeadHunter")
            collect_and_upload(limit=args.limit)
            sizes = tuple(int(x.strip()) for x in args.chunk_sizes.split(",") if x.strip())
            run_pipeline(sizes, args.attempts)
        elif args.command == "import-file":
            rows = read_local_dataset(args.path)
            if not rows:
                raise ValueError("После фильтрации в файле не осталось вакансий аналитиков")
            upload_jsonl(rows)
            _write_import_metadata(len(rows), f"local import: {Path(args.path).name}")
            logging.getLogger(__name__).info("Импортировано %d вакансий аналитиков", len(rows))
            logging.getLogger(__name__).info("Теперь запустите: docker compose run --rm pipeline run")
        elif args.command == "run":
            sizes = tuple(int(x.strip()) for x in args.chunk_sizes.split(",") if x.strip())
            run_pipeline(sizes, args.attempts)
        elif args.command == "list-objects":
            for item in list_objects():
                print(f"{item['key']}\t{item['size']} bytes\t{item['last_modified']}")
        return 0
    except (Exception, KeyboardInterrupt) as exc:
        logging.getLogger(__name__).exception("Пайплайн завершился с ошибкой: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
