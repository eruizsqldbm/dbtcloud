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
    FROM {{ source('SNOWFLAKE_SAMPLE_DATA', 'TPCH_SF1') }}
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