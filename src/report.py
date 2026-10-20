"""Build a PDF report from measured pipeline artifacts (never hard-code results)."""
from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from .config import settings

from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
    Image, KeepTogether, HRFlowable,
)

log = logging.getLogger(__name__)


def _load_json(path: Path, default=None):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _font_setup() -> tuple[str, str]:
    candidates = [
        ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
        ("/usr/share/fonts/truetype/lato/Lato-Regular.ttf", "/usr/share/fonts/truetype/lato/Lato-Bold.ttf"),
        ("/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf", "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf"),
    ]
    for regular, bold in candidates:
        if Path(regular).exists() and Path(bold).exists():
            pdfmetrics.registerFont(TTFont("ReportSans", regular))
            pdfmetrics.registerFont(TTFont("ReportSans-Bold", bold))
            return "ReportSans", "ReportSans-Bold"
    raise FileNotFoundError("Не найден шрифт с поддержкой кириллицы. В Docker-образе должен быть fonts-dejavu-core.")


def _num(value, digits: int = 0) -> str:
    if value is None or value == "":
        return "—"
    try:
        number = float(value)
        text = f"{number:,.{digits}f}".replace(",", " ")
        return text
    except (TypeError, ValueError):
        return str(value)


def _rub(value) -> str:
    return "—" if value is None or value == "" else f"{_num(value, 0)} ₽"


def _p(text, styles, style="BodyText"):
    return Paragraph(escape(str(text)).replace("\n", "<br/>"), styles[style])


def _table(data: list[list], widths: list[float] | None, font_name: str, font_bold: str, font_size: float = 7.2, header: bool = True) -> Table:
    cooked = []
    for row_idx, row in enumerate(data):
        cooked_row = []
        for cell in row:
            if isinstance(cell, Paragraph):
                cooked_row.append(cell)
            else:
                style = ParagraphStyle(
                    name=f"cell_{row_idx}_{len(cooked_row)}",
                    fontName=font_bold if header and row_idx == 0 else font_name,
                    fontSize=font_size,
                    leading=font_size + 1.7,
                    textColor=colors.HexColor("#202938"),
                    alignment=TA_LEFT,
                    wordWrap="CJK",
                )
                cooked_row.append(Paragraph(escape(str(cell if cell is not None else "—")), style))
        cooked.append(cooked_row)
    tbl = Table(cooked, colWidths=widths, repeatRows=1 if header else 0, hAlign="LEFT")
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e9eef5")) if header else ("BACKGROUND", (0, 0), (-1, -1), colors.white),
        ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#202938")),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
    ]))
    return tbl


def _top_frequency_table(rows: list[dict], kind: str, limit: int = 12) -> list[list]:
    if kind == "skill":
        data = [["Навык", "Вакансий"]]
        data.extend([[r.get("skill", ""), _num(r.get("vacancy_count"))] for r in rows[:limit]])
        return data
    if kind == "pair":
        data = [["Пара навыков", "Вакансий"]]
        data.extend([[f"{r.get('skill_1', '')} + {r.get('skill_2', '')}", _num(r.get("vacancy_count"))] for r in rows[:limit]])
        return data
    data = [["Тройка навыков", "Вакансий"]]
    data.extend([[f"{r.get('skill_1', '')} + {r.get('skill_2', '')} + {r.get('skill_3', '')}", _num(r.get("vacancy_count"))] for r in rows[:limit]])
    return data


def build_report(artifacts_dir: str | Path, output_path: str | Path | None = None) -> Path:
    out_dir = Path(artifacts_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = Path(output_path) if output_path else out_dir / "report.pdf"
    meta = _load_json(out_dir / "analysis_meta.json", {}) or {}
    source_meta = _load_json(out_dir / "source_metadata.json", {}) or {}
    verification = _load_json(out_dir / "verification.json", {}) or {}
    duck = _load_json(out_dir / "duckdb_result.json", {}) or {}
    mr = _load_json(out_dir / "mapreduce_chunk_1000.json", {}) or _load_json(out_dir / "mapreduce_chunk_500.json", {}) or {}
    chunk_results = []
    for chunk_path in out_dir.glob("mapreduce_chunk_*.json"):
        item = _load_json(chunk_path, {}) or {}
        if item.get("chunk_size") is not None:
            chunk_results.append(item)
    chunk_results.sort(key=lambda item: int(item.get("chunk_size", 0)))
    object_manifest = _load_json(out_dir / "object_store_manifest.json", []) or []
    run_log_path = out_dir / "run.log"
    run_log_lines = run_log_path.read_text(encoding="utf-8", errors="replace").splitlines() if run_log_path.exists() else []
    profiling = _load_csv(out_dir / "profiling.csv")
    profiling_summary = _load_csv(out_dir / "profiling_summary.csv")
    optimization = _load_csv(out_dir / "optimization.csv")
    skills = _load_csv(out_dir / "skills.csv")
    pairs = _load_csv(out_dir / "pairs.csv")
    triples = _load_csv(out_dir / "triples.csv")
    salary_up = _load_csv(out_dir / "salary_increases.csv")
    salary_down = _load_csv(out_dir / "salary_decreases.csv")
    regions = _load_csv(out_dir / "regions_summary.csv")
    region_skills = _load_csv(out_dir / "region_skills.csv")
    region_combos = _load_csv(out_dir / "region_combinations.csv")

    regular_font, bold_font = _font_setup()
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="CoverTitle", fontName=bold_font, fontSize=23, leading=29, alignment=TA_CENTER, textColor=colors.HexColor("#172554"), spaceAfter=12))
    styles.add(ParagraphStyle(name="CoverSub", fontName=regular_font, fontSize=12, leading=17, alignment=TA_CENTER, textColor=colors.HexColor("#475569"), spaceAfter=8))
    styles.add(ParagraphStyle(name="H1ru", fontName=bold_font, fontSize=15, leading=19, textColor=colors.HexColor("#17365d"), spaceBefore=10, spaceAfter=7, keepWithNext=True))
    styles.add(ParagraphStyle(name="H2ru", fontName=bold_font, fontSize=10.5, leading=14, textColor=colors.HexColor("#334155"), spaceBefore=7, spaceAfter=4, keepWithNext=True))
    styles.add(ParagraphStyle(name="BodyRU", fontName=regular_font, fontSize=8.7, leading=12, textColor=colors.HexColor("#202938"), spaceAfter=5))
    styles.add(ParagraphStyle(name="Small", fontName=regular_font, fontSize=7.2, leading=9.2, textColor=colors.HexColor("#475569"), spaceAfter=4))
    styles.add(ParagraphStyle(name="Warning", fontName=bold_font, fontSize=8.5, leading=12, textColor=colors.HexColor("#92400e"), backColor=colors.HexColor("#fff7ed"), borderColor=colors.HexColor("#fed7aa"), borderWidth=0.5, borderPadding=7, spaceBefore=5, spaceAfter=8))

    doc = SimpleDocTemplate(
        str(pdf_path), pagesize=A4, rightMargin=17 * mm, leftMargin=17 * mm,
        topMargin=16 * mm, bottomMargin=17 * mm, title="Анализ вакансий аналитиков — вариант 10",
        author="Автономова Ксения Витальевна",
    )
    story = []
    story.extend([Spacer(1, 34 * mm), Paragraph("АНАЛИЗ IT-ВАКАНСИЙ", styles["CoverTitle"]), Paragraph("Вариант 10 — «Аналитик»", styles["CoverSub"]), Spacer(1, 7 * mm)])
    story.append(HRFlowable(width="62%", thickness=1.2, color=colors.HexColor("#3b82f6"), hAlign="CENTER", spaceBefore=4, spaceAfter=14))
    story.append(Paragraph("Домашнее задание: объектное хранилище RustFS, MapReduce, DuckDB и аналитика заработных плат", styles["CoverSub"]))
    story.append(Spacer(1, 18 * mm))
    story.append(Paragraph("Выполнила: Автономова Ксения Витальевна", styles["CoverSub"]))
    story.append(Paragraph("Дата формирования отчёта: " + datetime.now().strftime("%d.%m.%Y %H:%M"), styles["CoverSub"]))
    story.append(Spacer(1, 20 * mm))
    if not verification.get("all_equal", False):
        story.append(Paragraph("ВНИМАНИЕ: проверка MapReduce и DuckDB не пройдена. В отчёте приведены диагностические результаты; перед сдачей необходимо устранить расхождение.", styles["Warning"]))
    elif not meta:
        story.append(Paragraph("ВНИМАНИЕ: отчёт сформирован без полного набора измеренных артефактов. Запустите весь пайплайн для получения фактических результатов.", styles["Warning"]))
    story.append(PageBreak())

    story.append(Paragraph("1. Цель и состав работы", styles["H1ru"]))
    story.append(Paragraph(
        "Цель работы — построить воспроизводимый аналитический пайплайн для вакансий аналитиков: сохранить данные в S3-совместимом объектном хранилище RustFS, посчитать частоты навыков, пар и троек двумя независимыми способами, проверить совпадение результатов, изучить связь навыков с зарплатой и сравнить регионы.", styles["BodyRU"]))
    story.append(Paragraph(
        "Сегмент определяется по названию вакансии: в выборку входят заголовки, содержащие «аналитик» или «analyst». Сначала навыки приводятся к единому регистру, нормализуются пробелы и удаляются повторы внутри одной вакансии. Поэтому одна вакансия учитывается не более одного раза для каждого отдельного навыка и каждой комбинации.", styles["BodyRU"]))

    story.append(Paragraph("2. Данные и объектное хранилище", styles["H1ru"]))
    data_rows = [
        ["Показатель", "Результат"],
        ["Вариант", "10 — Аналитик"],
        ["Источник данных", source_meta.get("source", "Не указан")],
        ["Снимок данных (UTC)", source_meta.get("snapshot_utc", "Не указан")],
        ["Вакансий в выборке", _num(meta.get("vacancies_total"))],
        ["Вакансий хотя бы с одним навыком", _num(meta.get("vacancies_with_skills"))],
        ["Вакансий с сопоставимой зарплатой в RUB/RUR", _num(meta.get("vacancies_with_salary_rub"))],
        ["Медианная зарплата по выборке", _rub(meta.get("overall_median_salary_rub"))],
        ["Бакет", "hh-lake"],
        ["Объект с исходным снимком", "raw/analyst/vacancies.jsonl"],
    ]
    story.append(_table(data_rows, [75 * mm, 95 * mm], regular_font, bold_font, 8.2))
    story.append(Spacer(1, 3 * mm))
    story.append(Paragraph(
        "Для зарплат использовано среднее между нижней и верхней границей опубликованного диапазона; если указана только одна граница, использована она. В расчёты включены только суммы в рублях (RUB/RUR), чтобы не смешивать валюты. Значения до/после налогообложения не приводились к единой базе, поэтому зарплатные различия следует трактовать как описательные, а не причинные.", styles["BodyRU"]))

    story.append(Paragraph("3. Расчёт частот и верификация", styles["H1ru"]))
    verify_rows = [
        ["Контроль", "MapReduce", "DuckDB", "Совпадение"],
        ["Вакансии сегмента", _num(verification.get("mapreduce_vacancies")), _num(verification.get("duckdb_vacancies")), "Да" if verification.get("vacancies_equal") else "Нет"],
        ["Различные навыки", _num(verification.get("mapreduce_distinct_skills")), _num(verification.get("duckdb_distinct_skills")), "Да" if verification.get("skills_equal") else "Нет"],
        ["Различные пары", _num(verification.get("mapreduce_distinct_pairs")), _num(verification.get("duckdb_distinct_pairs")), "Да" if verification.get("pairs_equal") else "Нет"],
        ["Различные тройки", _num(verification.get("mapreduce_distinct_triples")), _num(verification.get("duckdb_distinct_triples")), "Да" if verification.get("triples_equal") else "Нет"],
    ]
    story.append(_table(verify_rows, [48 * mm, 35 * mm, 35 * mm, 28 * mm], regular_font, bold_font, 7.5))
    story.append(Spacer(1, 3 * mm))
    if verification.get("all_equal"):
        story.append(Paragraph("Результаты MapReduce и DuckDB совпали по всем трём уровням агрегации. Это подтверждает, что оба способа используют одинаковые правила фильтрации, нормализации и подсчёта.", styles["BodyRU"]))
    else:
        story.append(Paragraph("Результаты двух способов не совпали. Сначала нужно проверить названия полей, фильтр вакансий, нормализацию skills и обработку повторов; автоматически считать результаты верными нельзя.", styles["Warning"]))

    story.append(Paragraph("3.1. Частоты отдельных навыков", styles["H2ru"]))
    if skills:
        story.append(_table(_top_frequency_table(skills, "skill", 15), [125 * mm, 30 * mm], regular_font, bold_font, 7.5))
    else:
        story.append(Paragraph("Таблица будет заполнена после запуска пайплайна.", styles["Small"]))
    story.append(Paragraph("3.2. Наиболее частые пары навыков", styles["H2ru"]))
    if pairs:
        story.append(_table(_top_frequency_table(pairs, "pair", 12), [125 * mm, 30 * mm], regular_font, bold_font, 7.2))
    else:
        story.append(Paragraph("Таблица будет заполнена после запуска пайплайна.", styles["Small"]))
    story.append(Paragraph("3.3. Наиболее частые тройки навыков", styles["H2ru"]))
    if triples:
        story.append(_table(_top_frequency_table(triples, "triple", 10), [125 * mm, 30 * mm], regular_font, bold_font, 7.0))
    else:
        story.append(Paragraph("Таблица будет заполнена после запуска пайплайна.", styles["Small"]))
    story.append(Paragraph("3.4. Результаты для трёх размеров чанка", styles["H2ru"]))
    if chunk_results:
        rows = [["Размер чанка", "Вакансий", "Навыков", "Пар", "Троек", "Совпадает с контрольным", "Ключ в RustFS"]]
        for item in chunk_results:
            size = int(item.get("chunk_size", 0))
            check = (verification.get("chunk_checks") or {}).get(str(size), {}).get("equal_to_reference", False)
            rows.append([_num(size), _num(item.get("vacancies_processed")), _num(item.get("distinct_skills")), _num(item.get("distinct_pairs")), _num(item.get("distinct_triples")), "Да" if check else "Нет", f"results/mapreduce/chunk_{size}.json"])
        story.append(_table(rows, [19 * mm, 18 * mm, 17 * mm, 14 * mm, 15 * mm, 38 * mm, 42 * mm], regular_font, bold_font, 6.2))
    else:
        story.append(Paragraph("Результаты трёх размеров чанка будут добавлены после запуска пайплайна.", styles["Small"]))
    volume_rows = [
        ["Метрика", "Всего встреч", "Уникальных значений"],
        ["Навыки", _num(meta.get("total_skill_occurrences")), _num(meta.get("distinct_skills"))],
        ["Пары навыков", _num(meta.get("total_pair_occurrences")), _num(meta.get("distinct_pairs"))],
        ["Тройки навыков", _num(meta.get("total_triple_occurrences")), _num(meta.get("distinct_triples"))],
    ]
    story.append(Spacer(1, 2 * mm))
    story.append(_table(volume_rows, [58 * mm, 45 * mm, 50 * mm], regular_font, bold_font, 7.5))
    story.append(Spacer(1, 2 * mm))
    story.append(Paragraph(
        "Рост числа комбинаций объясняется комбинаторикой: для вакансии с n различными навыками существует C(n,2)=n(n−1)/2 пар и C(n,3)=n(n−1)(n−2)/6 троек. Чем больше навыков указано в вакансии, тем быстрее растёт число комбинаций. В сводных данных различаются число всех встреч комбинаций и число уникальных комбинаций.", styles["BodyRU"]))

    story.append(PageBreak())
    story.append(Paragraph("4. Связь навыков с заработной платой", styles["H1ru"]))
    story.append(Paragraph(
        f"В зарплатном сравнении оставлены сочетания, встречающиеся не менее чем в {meta.get('min_combo_support', settings.min_combo_support)} вакансиях с опубликованной сопоставимой зарплатой. Для каждой пары или тройки медианная зарплата сравнивается с медианной зарплатой наиболее часто встречающегося одиночного навыка из этой комбинации. Такой базовый уровень выбран для более содержательного сравнения, однако он не устраняет влияние должности, опыта, региона и отрасли.", styles["BodyRU"]))
    story.append(Paragraph("4.1. Сочетания с наибольшей положительной разницей", styles["H2ru"]))
    if salary_up:
        rows = [["Тип", "Комбинация", "N", "Медиана", "Базовый навык", "Медиана базы", "Разница"]]
        for r in salary_up[:10]:
            rows.append([r.get("combination_type"), r.get("combination"), _num(r.get("salary_sample_count")), _rub(r.get("median_salary_rub")), r.get("baseline_skill"), _rub(r.get("baseline_median_salary_rub")), _rub(r.get("delta_vs_baseline_rub"))])
        story.append(_table(rows, [15 * mm, 45 * mm, 10 * mm, 22 * mm, 25 * mm, 25 * mm, 22 * mm], regular_font, bold_font, 6.4))
    else:
        story.append(Paragraph("Нет сочетаний, прошедших установленный порог выборки; после проверки объёма данных можно снизить порог поддержки.", styles["Small"]))
    story.append(Paragraph("4.2. Сочетания с наибольшей отрицательной разницей", styles["H2ru"]))
    if salary_down:
        rows = [["Тип", "Комбинация", "N", "Медиана", "Базовый навык", "Медиана базы", "Разница"]]
        for r in salary_down[:10]:
            rows.append([r.get("combination_type"), r.get("combination"), _num(r.get("salary_sample_count")), _rub(r.get("median_salary_rub")), r.get("baseline_skill"), _rub(r.get("baseline_median_salary_rub")), _rub(r.get("delta_vs_baseline_rub"))])
        story.append(_table(rows, [15 * mm, 45 * mm, 10 * mm, 22 * mm, 25 * mm, 25 * mm, 22 * mm], regular_font, bold_font, 6.4))
    else:
        story.append(Paragraph("Нет сочетаний, прошедших установленный порог выборки; после проверки объёма данных можно снизить порог поддержки.", styles["Small"]))
    story.append(Paragraph(
        "Интерпретация: положительная разница означает, что медианная зарплата вакансий с комбинацией выше медианы вакансий, где присутствует базовый навык; отрицательная — ниже. Это не доказывает, что отдельный навык сам по себе повышает или снижает зарплату: различия могут объясняться уровнем специалиста, специализацией и географией вакансий.", styles["BodyRU"]))

    story.append(Paragraph("5. Региональное сравнение", styles["H1ru"]))
    story.append(Paragraph("Для воспроизводимости выбраны три региона с наибольшим количеством вакансий аналитиков в собранном снимке данных. Зарплатная медиана рассчитана только по вакансиям с опубликованной зарплатой в RUB/RUR; востребованность навыка — число вакансий региона, где навык указан.", styles["BodyRU"]))
    if regions:
        rows = [["Регион", "Вакансий", "Зарплат", "Покрытие, %", "Медиана, ₽", "Медиана навыков", "Навыков / пар / троек"]]
        for r in regions:
            rows.append([r.get("region"), _num(r.get("vacancy_count")), _num(r.get("salary_sample_count")), _num(r.get("salary_coverage_pct"), 1), _rub(r.get("median_salary_rub")), _num(r.get("median_skills_per_vacancy"), 1), f"{r.get('distinct_skills', '—')} / {r.get('distinct_pairs', '—')} / {r.get('distinct_triples', '—')}"])
        story.append(_table(rows, [36 * mm, 17 * mm, 17 * mm, 20 * mm, 25 * mm, 20 * mm, 30 * mm], regular_font, bold_font, 6.3))
    else:
        story.append(Paragraph("Региональная таблица будет сформирована после запуска пайплайна.", styles["Small"]))
    story.append(Paragraph("5.1. Наиболее востребованные навыки по регионам", styles["H2ru"]))
    if region_skills:
        rows = [["Регион", "Навык", "Вакансий", "Доля от всех вакансий региона, %"]]
        for r in region_skills[:30]:
            rows.append([r.get("region"), r.get("skill"), _num(r.get("vacancy_count")), _num(r.get("share_of_all_region_vacancies_pct"), 1)])
        story.append(_table(rows, [42 * mm, 63 * mm, 25 * mm, 35 * mm], regular_font, bold_font, 6.8))
    story.append(Paragraph("5.2. Частые пары и тройки по регионам", styles["H2ru"]))
    if region_combos:
        rows = [["Регион", "Тип", "Комбинация", "Вакансий", "Медиана, ₽"]]
        for r in region_combos[:30]:
            rows.append([r.get("region"), r.get("combination_type"), r.get("combination"), _num(r.get("vacancy_count")), _rub(r.get("median_salary_rub"))])
        story.append(_table(rows, [35 * mm, 15 * mm, 70 * mm, 20 * mm, 25 * mm], regular_font, bold_font, 6.4))

    story.append(PageBreak())
    story.append(Paragraph("6. Профилирование времени и памяти", styles["H1ru"]))
    story.append(Paragraph("Каждый измеряемый вариант запускался не менее трёх раз. Время измерялось через wall-clock time; пиковая память процесса — выборкой RSS через psutil, а пиковые Python-аллокации — через tracemalloc. Установку расширения httpfs DuckDB выполняет до замеров, чтобы не смешивать скачивание расширения с временем запросов.", styles["BodyRU"]))
    if profiling_summary:
        rows = [["Метод", "Размер чанка", "Попыток", "Среднее, с", "Мин., с", "Макс., с", "Пик RSS, MiB", "Пик Python, MiB"]]
        for r in profiling_summary:
            rows.append([r.get("method"), r.get("chunk_size") or "—", _num(r.get("attempts")), _num(r.get("mean_elapsed_seconds"), 3), _num(r.get("min_elapsed_seconds"), 3), _num(r.get("max_elapsed_seconds"), 3), _num(r.get("max_rss_peak_delta_mb"), 2), _num(r.get("max_tracemalloc_peak_mb"), 2)])
        story.append(_table(rows, [35 * mm, 18 * mm, 14 * mm, 20 * mm, 17 * mm, 17 * mm, 25 * mm, 27 * mm], regular_font, bold_font, 6.1))
    else:
        story.append(Paragraph("Профилирование будет добавлено после запуска пайплайна.", styles["Small"]))
    story.append(Paragraph("6.1. Результаты отдельных попыток", styles["H2ru"]))
    if profiling:
        rows = [["Метод", "Чанк", "№", "Время, с", "Пик RSS, MiB", "Пик Python, MiB"]]
        for r in profiling:
            rows.append([r.get("method"), r.get("chunk_size") or "—", r.get("attempt"), _num(r.get("elapsed_seconds"), 3), _num(r.get("rss_peak_mb"), 2), _num(r.get("tracemalloc_peak_mb"), 2)])
        story.append(_table(rows, [52 * mm, 18 * mm, 12 * mm, 25 * mm, 28 * mm, 30 * mm], regular_font, bold_font, 6.6))
    story.append(Paragraph("6.2. Бонус: сравнение до и после оптимизации", styles["H2ru"]))
    if optimization:
        rows = [["Подход", "Среднее время, с", "Пик RSS сверх старта, MiB", "Максимальный Python peak, MiB"]]
        for r in optimization:
            rows.append([r.get("approach"), _num(r.get("mean_elapsed_seconds"), 3), _num(r.get("max_rss_peak_delta_mb"), 2), _num(r.get("max_tracemalloc_peak_mb"), 2)])
        story.append(_table(rows, [50 * mm, 35 * mm, 40 * mm, 40 * mm], regular_font, bold_font, 7.0))
        if len(optimization) >= 2:
            before, after = optimization[0], optimization[1]
            story.append(Paragraph(
                f"Изменение среднего времени: {_num(after.get('time_improvement_pct'), 1)}%; изменение пикового RSS: {_num(after.get('rss_improvement_pct'), 1)}%. Положительное значение означает снижение ресурса после оптимизации, отрицательное — ухудшение. Для небольших наборов накладные расходы потокового чтения могут нивелировать выигрыш по времени.", styles["BodyRU"]))
    else:
        story.append(Paragraph("Сравнение baseline и потоковой обработки появится после запуска.", styles["Small"]))

    story.append(Paragraph("7. Проверка объектного хранилища и скриншоты", styles["H1ru"]))
    story.append(Paragraph("Пайплайн создаёт бакет hh-lake при необходимости и сохраняет исходный снимок, результаты трёх размеров чанка, контроль DuckDB и MapReduce, таблицы аналитики, профилирование и этот PDF в RustFS. Для доказательства в отчёте добавьте реальные скриншоты консоли RustFS в папку screenshots/ и повторно запустите генерацию отчёта.", styles["BodyRU"]))
    stored_items = [item for item in object_manifest if isinstance(item, dict)]
    if stored_items:
        story.append(Paragraph("7.1. Объекты, найденные в бакете hh-lake", styles["H2ru"]))
        important = [item for item in stored_items if item.get("key", "").startswith(("raw/analyst/", "results/mapreduce/", "results/duckdb/", "results/verification.json", "results/profiling/", "reports/"))]
        rows = [["Ключ объекта", "Размер, байт"]]
        for item in important[:18]:
            rows.append([item.get("key", ""), _num(item.get("size"))])
        story.append(_table(rows, [125 * mm, 30 * mm], regular_font, bold_font, 6.8))
    log_matches = [line for line in run_log_lines if any(term in line for term in ("Этап", "MapReduce чанк", "DuckDB:", "Загружен объект", "PDF-отчёт сформирован", "Готово."))]
    if log_matches:
        story.append(Paragraph("7.2. Фрагмент вывода команд", styles["H2ru"]))
        for line in log_matches[-12:]:
            story.append(Paragraph(escape(line), styles["Small"]))
    screenshot_dir = Path(os.getenv("SCREENSHOTS_DIR", "screenshots"))
    if not screenshot_dir.exists():
        screenshot_dir = Path("screenshots")
    screenshots = sorted([p for p in screenshot_dir.glob("*") if p.suffix.lower() in (".png", ".jpg", ".jpeg")]) if screenshot_dir.exists() else []
    if screenshots:
        for image_path in screenshots:
            try:
                img = Image(str(image_path), width=150 * mm, height=85 * mm, kind="bound")
                story.append(KeepTogether([Paragraph(escape(image_path.name), styles["H2ru"]), img, Spacer(1, 4 * mm)]))
            except Exception as exc:
                log.warning("Не удалось вставить скриншот %s: %s", image_path, exc)
    else:
        story.append(Paragraph("Скриншоты ещё не добавлены. Снимите экран с бакетом hh-lake и списком объектов raw/analyst и results/ в RustFS Console (http://localhost:9001). Не вставляйте чужие скриншоты.", styles["Warning"]))

    story.append(Paragraph("8. Выводы и ограничения", styles["H1ru"]))
    if meta:
        story.append(Paragraph(
            f"В исследовании обработано { _num(meta.get('vacancies_total')) } вакансий; навыки указаны в { _num(meta.get('vacancies_with_skills')) } вакансиях. Получено { _num(meta.get('distinct_skills')) } различных навыков, { _num(meta.get('distinct_pairs')) } уникальных пар и { _num(meta.get('distinct_triples')) } уникальных троек. В зарплатном сравнении использовано { _num(meta.get('vacancies_with_salary_rub')) } опубликованных зарплат в рублях. Региональные выводы относятся к трём регионам с наибольшим числом вакансий в этом снимке.", styles["BodyRU"]))
    else:
        story.append(Paragraph("После выполнения пайплайна этот раздел заполнится измеренными показателями и региональными наблюдениями.", styles["BodyRU"]))
    story.append(Paragraph("Ограничения: публичные вакансии являются моментальным снимком рынка; часть работодателей не указывает зарплату и структурированные навыки. Сравнение медиан не учитывает опыт, уровень позиции, отрасль, тип занятости и налогообложение, поэтому не доказывает причинный эффект навыков на заработок.", styles["BodyRU"]))
    story.append(Paragraph("Источники документации: HeadHunter API — https://api.hh.ru/openapi/redoc; RustFS — https://docs.rustfs.com/en/installation/container/docker; DuckDB S3 — https://duckdb.org/docs/stable/guides/network_cloud_storage/s3_import.", styles["Small"]))

    def page_footer(canvas, document):
        canvas.saveState()
        canvas.setFont(regular_font, 7)
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.drawString(17 * mm, 9 * mm, "Автономова Ксения Витальевна · Вариант 10 · Аналитик")
        canvas.drawRightString(A4[0] - 17 * mm, 9 * mm, f"Стр. {document.page}")
        canvas.restoreState()

    doc.build(story, onFirstPage=page_footer, onLaterPages=page_footer)
    log.info("PDF-отчёт сформирован: %s", pdf_path)
    return pdf_path
