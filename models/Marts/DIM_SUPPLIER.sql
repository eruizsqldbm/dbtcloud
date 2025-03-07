{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH STG_SUPPLIER AS (
    SELECT
        SUPPLIERKEY           AS SUPPLIER_KEY,
        SUPPLIERNAME          AS SUPPLIER_NAME,
        ADDRESS               AS ADDRESS,
        NATIONKEY             AS NATION_KEY,
        PHONE                 AS PHONE,
        ACCOUNTBALANCE        AS ACCOUNT_BALANCE,
        COMMENT               AS COMMENT,
        LOAD_TIMESTAMP        AS LOAD_TIMESTAMP
    FROM {{ source('STAGING', 'DBT') }}
)

DIM_SUPPLIER AS (
    SELECT
        C_CUSTKEY AS CUSTKEY,
	    C_NAME AS NAME,
	    C_ADDRESS AS ADDRESS,
	    C_NATIONKEY NATIONKEY,
	    C_PHONE AS PHONE,
	    C_ACCTBAL AS ACCTBAL,
	    C_MKTSEGMENT AS MKTSEGMENT,
	    C_COMMENT AS COMMENT,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
    FROM STG_SUPPLIER
)
SELECT *
FROM DIM_SUPPLIER