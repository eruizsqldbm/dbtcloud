{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH part_source AS (
    SELECT
        PARTKEY,
        NAME,
        MFGR,
        BRAND,
        TYPE,
        SIZE,
        CONTAINER,
        RETAILPRICE,
        COMMENT,
        LOAD_TIMESTAMP
    FROM {{ source('TPCH_SF1', 'PART') }} -- Replace with your actual source
)

stg_part AS (
    SELECT
        PARTKEY,
        NAME,
        MFGR,
        BRAND,
        TYPE,
        SIZE,
        CONTAINER,
        RETAILPRICE,
        COMMENT,
        LOAD_TIMESTAMP
FROM part_source
)
SELECT *
FROM stg_part