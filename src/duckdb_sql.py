"""Independent SQL implementation in DuckDB over the same S3 object."""
from __future__ import annotations

import logging

import duckdb

from .config import settings

log = logging.getLogger(__name__)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def prepare_httpfs() -> None:
    """Install httpfs before profiling so extension installation is not timed as query work."""
    con = duckdb.connect(database=":memory:")
    try:
        try:
            con.execute("LOAD httpfs")
        except Exception:
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")
    finally:
        con.close()


def duckdb_count_skills(key: str) -> dict:
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("LOAD httpfs")
        endpoint = settings.duckdb_endpoint
        use_ssl = "true" if settings.duckdb_use_ssl else "false"
        con.execute(
            "CREATE OR REPLACE SECRET rustfs_secret ("
            "TYPE S3, "
            f"KEY_ID {_sql_string(settings.access_key)}, "
            f"SECRET {_sql_string(settings.secret_key)}, "
            f"REGION {_sql_string(settings.region)}, "
            f"ENDPOINT {_sql_string(endpoint)}, "
            f"USE_SSL {use_ssl}, URL_STYLE 'path')"
        )
        uri = f"s3://{settings.bucket}/{key}"
        con.execute(
            "CREATE TEMP TABLE eligible_vacancies AS "
            "SELECT CAST(id AS VARCHAR) AS id, name, area, area_id, salary_value, salary_currency, "
            "salary_gross, skills, published_at "
            f"FROM read_ndjson_auto({_sql_string(uri)}) "
            "WHERE regexp_matches(lower(coalesce(name, '')), 'аналитик|analyst')"
        )
        total, with_skills = con.execute(
            "SELECT count(*), count(*) FILTER (WHERE len(skills) > 0) FROM eligible_vacancies"
        ).fetchone()

        skill_rows = con.execute(
            "SELECT u.skill, count(DISTINCT v.id) AS vacancy_count "
            "FROM eligible_vacancies v CROSS JOIN UNNEST(v.skills) AS u(skill) "
            "GROUP BY u.skill ORDER BY vacancy_count DESC, u.skill"
        ).fetchall()
        pair_rows = con.execute(
            "SELECT a.skill AS skill_1, b.skill AS skill_2, count(DISTINCT v.id) AS vacancy_count "
            "FROM eligible_vacancies v "
            "CROSS JOIN UNNEST(v.skills) AS a(skill) "
            "CROSS JOIN UNNEST(v.skills) AS b(skill) "
            "WHERE a.skill < b.skill "
            "GROUP BY a.skill, b.skill ORDER BY vacancy_count DESC, skill_1, skill_2"
        ).fetchall()
        triple_rows = con.execute(
            "SELECT a.skill AS skill_1, b.skill AS skill_2, c.skill AS skill_3, "
            "count(DISTINCT v.id) AS vacancy_count "
            "FROM eligible_vacancies v "
            "CROSS JOIN UNNEST(v.skills) AS a(skill) "
            "CROSS JOIN UNNEST(v.skills) AS b(skill) "
            "CROSS JOIN UNNEST(v.skills) AS c(skill) "
            "WHERE a.skill < b.skill AND b.skill < c.skill "
            "GROUP BY a.skill, b.skill, c.skill "
            "ORDER BY vacancy_count DESC, skill_1, skill_2, skill_3"
        ).fetchall()

        result = {
            "variant": 10,
            "segment": "analyst",
            "chunk_size": None,
            "vacancies_processed": int(total or 0),
            "vacancies_with_skills": int(with_skills or 0),
            "distinct_skills": len(skill_rows),
            "distinct_pairs": len(pair_rows),
            "distinct_triples": len(triple_rows),
            "skills": [{"skill": row[0], "vacancy_count": int(row[1])} for row in skill_rows],
            "pairs": [{"skill_1": row[0], "skill_2": row[1], "vacancy_count": int(row[2])} for row in pair_rows],
            "triples": [{"skill_1": row[0], "skill_2": row[1], "skill_3": row[2], "vacancy_count": int(row[3])} for row in triple_rows],
        }
        log.info("DuckDB: вакансий=%d, навыков=%d, пар=%d, троек=%d", total, len(skill_rows), len(pair_rows), len(triple_rows))
        return result
    finally:
        con.close()
