{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}


WITH nation_source AS (
    SELECT
        N_NATIONKEY,
        N_NAME,
        N_REGIONKEY,
        N_COMMENT
    FROM {{ source('TPCH_SF1', 'NATION') }}
),

stg_nation AS (
    SELECT
        N_NATIONKEY,
        TRIM(N_NAME) AS NATION_NAME,
        N_REGIONKEY,
        UPPER(TRIM(N_COMMENT)) AS COMMENT,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
    FROM nation_source
)

SELECT *
FROM stg_nation
