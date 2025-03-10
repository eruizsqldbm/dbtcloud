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
STG_REGION AS (
    SELECT
        REGIONKEY,
        REGION_NAME,
        REGION_COMMENT,
        LOAD_TIMESTAMP
    FROM {{ source('STAGING', 'STG_REGION') }}
),
STG_PART AS (
    SELECT
        PARTKEY,
        NAME,
        MFGR,
        BRAND,
        TYPE,
        SIZE,
        CONTAINER,
        RETAILPRICE,
        COMMENT,
        LOAD_TIMESTAMP
    FROM {{ source('STAGING', 'STG_PART') }}
),
STG_CUSTOMER AS (
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

FACT_LINEITEM AS (
    SELECT
        ORDERKEY,
        LINENUMBER,
        PARTKEY,
        CUSTKEY,
        SUPPLIERKEY,
        DATEKEY,
        REGIONKEY,
        QUANTITY,
        EXTENDEDPRICE,
        DISCOUNT,
        TAX,
        RETURNFLAG,
        LINESTATUS,
        SHIPDATE,
        COMMITDATE,
        RECEIPTDATE,
        SHIPINSTRUCT,
        SHIPMODE,
        COMMENT,
        LOAD_TIMESTAMP
    FROM STG_LINEITEM
LEFT JOIN STG_SUPPLIER ON FL.SUPPLIERKEY = S.SUPPLIERKEY
LEFT JOIN STG_REGION R ON FL.REGIONKEY = R.REGIONKEY
LEFT JOIN STG_PART P ON FL.PARTKEY = P.PARTKEY
LEFT JOIN STG_CUSTOMER C ON FL.CUSTKEY = C.CUSTKEY
)

SELECT *
FROM FACT_LINEITEM
