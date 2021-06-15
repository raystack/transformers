CREATE TEMP FUNCTION standardRule(fieldContent STRING, rules ARRAY<STRING>)
RETURNS STRING
LANGUAGE js AS """
return standardizedRule(fieldContent, rules)
"""
OPTIONS (library="gs://bi_playground_eoch7goo/project/bq_lib/standardizedRule.js");

WITH
dedup_source AS (
  SELECT DISTINCT 
    method,
    type,
    accuracy,
    total_sample
  FROM
    `g-project.playground.gsheet_log`
)

SELECT
    standardRule(method, ['cleanup']) AS method_name,
    standardRule(type, ['cleanup']) AS type,
    accuracy,
    CAST(total_sample AS NUMERIC) AS total_sample_count
FROM dedup_source
