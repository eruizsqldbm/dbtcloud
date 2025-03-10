{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH STG_NATION AS (
    SELECT
        NATIONKEY,
        NAME,
        REGIONKEY,
        COMMENT,
        LOAD_TIMESTAMP
    FROM {{ source('STAGING', 'STG_NATION') }}
),

DIM_NATION AS (
    SELECT
        NATIONKEY,
        NAME,
        REGIONKEY,
        COMMENT,
        LOAD_TIMESTAMP
    FROM STG_NATION
)

SELECT *
FROM DIM_NATION