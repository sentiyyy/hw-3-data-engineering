-- Variant 10: analyst. Connection to RustFS is configured by src/duckdb_sql.py.
-- Run against the same NDJSON object as the streaming MapReduce implementation.
CREATE OR REPLACE TEMP TABLE eligible_vacancies AS
SELECT
    CAST(id AS VARCHAR) AS id,
    name,
    area,
    area_id,
    salary_value,
    salary_currency,
    salary_gross,
    skills,
    published_at
FROM read_ndjson_auto('s3://hh-lake/raw/analyst/vacancies.jsonl')
WHERE regexp_matches(lower(coalesce(name, '')), 'аналитик|analyst');

-- 1. Frequencies of individual skills (a skill is counted at most once per vacancy).
SELECT u.skill, count(DISTINCT v.id) AS vacancy_count
FROM eligible_vacancies v
CROSS JOIN UNNEST(v.skills) AS u(skill)
GROUP BY u.skill
ORDER BY vacancy_count DESC, u.skill;

-- 2. Frequencies of skill pairs. Lexicographic ordering prevents duplicate pair permutations.
SELECT a.skill AS skill_1, b.skill AS skill_2, count(DISTINCT v.id) AS vacancy_count
FROM eligible_vacancies v
CROSS JOIN UNNEST(v.skills) AS a(skill)
CROSS JOIN UNNEST(v.skills) AS b(skill)
WHERE a.skill < b.skill
GROUP BY a.skill, b.skill
ORDER BY vacancy_count DESC, skill_1, skill_2;

-- 3. Frequencies of skill triples. Strict ordering prevents duplicate permutations.
SELECT a.skill AS skill_1, b.skill AS skill_2, c.skill AS skill_3,
       count(DISTINCT v.id) AS vacancy_count
FROM eligible_vacancies v
CROSS JOIN UNNEST(v.skills) AS a(skill)
CROSS JOIN UNNEST(v.skills) AS b(skill)
CROSS JOIN UNNEST(v.skills) AS c(skill)
WHERE a.skill < b.skill AND b.skill < c.skill
GROUP BY a.skill, b.skill, c.skill
ORDER BY vacancy_count DESC, skill_1, skill_2, skill_3;
