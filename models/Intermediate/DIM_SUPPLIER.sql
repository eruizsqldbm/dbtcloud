{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH STG_SUPPLIER AS (
    SELECT
        SUPPLIERKEY,
        SUPPLIERNAME,
        ADDRESS,
        NATIONKEY,
        PHONE,
        ACCOUNTBALANCE,
        COMMENT,
        LOAD_TIMESTAMP
    FROM {{ source('STAGING', 'STG_SUPPLIER') }}
),

DIM_SUPPLIER AS (
    SELECT
        SUPPLIERKEY,
        UPPER(SUPPLIERNAME) AS SUPPLIERNAME,
        TRIM(ADDRESS) AS ADDRESS,
        NATIONKEY,
        REGEXP_REPLACE(PHONE, '[^0-9]', '') AS PHONE,
        ROUND(ACCOUNTBALANCE, 2) AS ACCOUNTBALANCE,
        COMMENT,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
    FROM STG_SUPPLIER
)
SELECT *
FROM DIM_SUPPLIER