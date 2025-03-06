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
        SUPPLIERKEY,
        TRIM(SUPPLIERNAME) AS SUPPLIERNAME,
        TRIM(ADDRESS) AS ADDRESS,
        NATIONKEY,
        REGEXP_REPLACE(PHONE, '[^0-9]', '') AS PHONENUMBER,
        ROUND(ACCOUNTBALANCE, 2) AS ACCOUNTBALANCE,
        UPPER(COMMENT) AS COMMENT,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
    FROM supplier_source
)

SELECT *
FROM stg_supplier