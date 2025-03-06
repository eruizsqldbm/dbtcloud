{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH orders_source AS (
    SELECT
	O_ORDERKEY,
	O_CUSTKEY,
	O_ORDERSTATUS,
	O_TOTALPRICE,
	O_ORDERDATE,
	O_ORDERPRIORITY,
	O_CLERK,
	O_SHIPPRIORITY,
	O_COMMENT
    FROM {{ source('TPCH_SF1', 'ORDERS') }}
),

stg_orders AS ( 
    SELECT
        O_ORDERKEY AS ORDERKEY,
        O_CUSTKEY AS CUSTKEY,
        O_ORDERSTATUS AS ORDERSTATUS,
        O_TOTALPRICE AS TOTALPRICE,
        O_ORDERDATE AS ORDERDATE,
        O_ORDERPRIORITY AS ORDERPRIORITY,
        O_CLERK AS CLERK,
        O_SHIPPRIORITY AS SHIPPRIORITY,
        O_COMMENT AS COMMENT,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP 
FROM orders_source
)
SELECT *
FROM stg_orders