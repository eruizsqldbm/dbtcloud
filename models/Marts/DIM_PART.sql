{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH STG_PART AS (
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
    FROM {{ source('STAGING', 'STG_PART') }}
),

DIM_PART AS (
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
    FROM STG_PART
)
SELECT *
FROM DIM_PART