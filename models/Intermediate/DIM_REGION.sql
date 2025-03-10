{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH STG_REGION AS (
    SELECT
        REGIONKEY,
        NAME,
        COMMENT,
        LOAD_TIMESTAMP
    FROM {{ ref('STG_REGION') }}
),

DIM_REGION AS (
    SELECT
        REGIONKEY,
        NAME,
        COMMENT,
        LOAD_TIMESTAMP
    FROM STG_REGION
)

SELECT *
FROM DIM_REGION