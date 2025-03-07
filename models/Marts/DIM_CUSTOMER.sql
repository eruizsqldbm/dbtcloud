{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH STG_CUSTOMER AS (
    SELECT
        CUSTKEY,
        NAME,
        ADDRESS,
        NATIONKEY,
        PHONE,
        ACCTBAL,
        MKTSEGMENT,
        COMMENT,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
    FROM {{ source('STAGING', 'STG_CUSTOMER') }}
),

DIM_CUSTOMER AS (
    SELECT
        CUSTKEY,
        UPPER(NAME) AS CUSTOMER_NAME,
        TRIM(ADDRESS) AS ADDRESS,
        NATIONKEY,
        REGEXP_REPLACE(PHONE, '[^0-9]', '') AS PHONE,
        ROUND(ACCTBAL, 2) AS ACCOUNT_BALANCE,
        MKTSEGMENT AS MARKET_SEGMENT,
        COMMENT,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
    FROM STG_CUSTOMER
)
SELECT *
FROM DIM_CUSTOMER   