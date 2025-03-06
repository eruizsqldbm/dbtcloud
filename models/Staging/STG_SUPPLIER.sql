{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}


WITH supplier_source AS (
    SELECT
        S_SUPPKEY AS SUPPLIERKEY,
        S_NAME AS SUPPLIERNAME,
        S_ADDRESS AS ADDRESS,
        S_NATIONKEY AS NATIONKEY,
        S_PHONE AS PHONE,
        S_ACCTBAL AS ACCOUNTBALANCE,
        S_COMMENT AS COMMENT
    FROM {{ source('TPCH_SF1', 'SUPPLIER') }}
),

stg_supplier AS (
    SELECT
        SUPPLIERKEY,
        TRIM(SUPPLIERNAME),
        TRIM(ADDRESS),
        NATIONKEY,
        REGEXP_REPLACE(PHONE '[^0-9]', ''),
        ROUND(ACCOUNTBALANCE, 2),
        UPPER(COMMENT) AS COMMENT,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
    FROM supplier_source
)

SELECT *
FROM stg_supplier