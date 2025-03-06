{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH partsupp_source AS (
    SELECT
	PS_PARTKEY,
	PS_SUPPKEY,
	PS_AVAILQTY,
	PS_SUPPLYCOST,
	PS_COMMENT,
    FROM {{ source('TPCH_SF1', 'PARTSUPP') }}
),

stg_partsupp AS (
    SELECT
        PS_PARTKEY AS PARTKEY,
        PS_SUPPKEY AS SUPPKEY,
        PS_AVAILQTY AS AVAILQTY,
        PS_SUPPLYCOST AS SUPPLYCOST,
        PS_COMMENT AS COMMENT,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP 
FROM partsupp_source
)

SELECT *
FROM stg_partsupp
