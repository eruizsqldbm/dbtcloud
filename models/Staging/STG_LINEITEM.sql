{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH lineitem_source AS (
    SELECT
        ORDERKEY,
        LINENUMBER,
        PARTKEY,
        SUPPKEY,
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
    FROM {{ source('TPCH_SF1', 'LINEITEM') }}
),


stg_lineitem AS (
    SELECT
        ORDERKEY,
        LINENUMBER,
        PARTKEY,
        SUPPKEY,
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
FROM lineitem_source
)
SELECT *
FROM stg_lineitem