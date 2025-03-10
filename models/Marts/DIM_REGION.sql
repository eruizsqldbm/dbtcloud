{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH STG_REGION AS (
    SELECT
        REGIONKEY,
        REGION_NAME,
        REGION_COMMENT,
        LOAD_TIMESTAMP
    FROM {{ source('STAGING', 'STG_REGION') }}
),

DIM_REGION AS (
    SELECT
        REGIONKEY,
        REGION_NAME,
        REGION_COMMENT,
        LOAD_TIMESTAMP
    FROM STG_REGION
)

SELECT *
FROM DIM_REGION