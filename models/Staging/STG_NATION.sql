{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH nation_source AS (
    SELECT
        NATIONKEY,
        NAME,
        REGIONKEY,
        COMMENT,
        LOAD_TIMESTAMP
    FROM {{ source('TPCH_SF1', 'REGION') }}
),

stg_nation AS (
    SELECT
        NATIONKEY,
        NAME,
        REGIONKEY,
        COMMENT,
        LOAD_TIMESTAMP
    FROM nation_source
)
SELCT *
FROM stg_nation