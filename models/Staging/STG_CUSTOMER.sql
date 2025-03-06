{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH customer_source AS (
    SELECT
        CUSTKEY,
        NAME,
        ADDRESS,
        NATIONKEY,
        PHONE,
        ACCTBAL,
        MKTSEGMENT,
        COMMENT,
        LOAD_TIMESTAMP
    FROM {{ source('TPCH_SF1', 'CUSTOMER') }}

),

stg_customer AS (
    SELECT
        CUSTKEY,
        NAME,
        ADDRESS,
        NATIONKEY,
        PHONE,
        ACCTBAL,
        MKTSEGMENT,
        COMMENT,
        LOAD_TIMESTAMP
    FROM customer_source
)
SELECT *
FROM stg_customer