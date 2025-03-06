{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}


WITH supplier_source AS (
    SELECT
        S_SUPPKEY,
        S_NAME,
        S_ADDRESS,
        S_NATIONKEY,
        S_PHONE,
        S_ACCTBAL,
        S_COMMENT
    FROM {{ source('TPCH_SF1', 'SUPPLIER') }}
),

stg_supplier AS (
    SELECT
        S_SUPPKEY AS SUPPLIERKEY,
        TRIM(S_NAME) AS SUPPLIERNAME,
        TRIM(S_ADDRESS) AS ADDRESS,
        S_NATIONKEY AS NATIONKEY,
        REGEXP_REPLACE(S_PHONE, '[^0-9]', '') AS PHONENUMBER,
        ROUND(S_ACCTBAL, 2) AS ACCOUNTBALANCE,
        UPPER(S_COMMENT) AS COMMENT,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
    FROM supplier_source
)

SELECT *
FROM stg_supplier