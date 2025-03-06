{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH partsupp_source AS (
    SELECT
        PARTKEY,
        SUPPKEY,
        AVAILQTY,
        SUPPLYCOST,
        COMMENT,
        LOAD_TIMESTAMP
    FROM {{ source('TPCH_SF1', 'PARTSUPP') }}
)

SELECT *
FROM partsupp_source
