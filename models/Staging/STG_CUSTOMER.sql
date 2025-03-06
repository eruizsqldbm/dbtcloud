{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH customer_source AS (
    SELECT
	    C_CUSTKEY,
	    C_NAME,
	    C_ADDRESS,
	    C_NATIONKEY,
	    C_PHONE,
	    C_ACCTBAL,
	    C_MKTSEGMENT,
	    C_COMMENT
    FROM {{ source('TPCH_SF1', 'CUSTOMER') }}

),

stg_customer AS (
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
    FROM customer_source
)
SELECT *
FROM stg_customer