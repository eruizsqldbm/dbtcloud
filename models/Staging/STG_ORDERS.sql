WITH orders_source AS (
    SELECT
        ORDERKEY,
        CUSTKEY,
        ORDERSTATUS,
        TOTALPRICE,
        ORDERDATE,
        ORDERPRIORITY,
        CLERK,
        SHIPPRIORITY,
        COMMENT,
        LOAD_TIMESTAMP
    FROM {{ source('TPCH_SF1', 'ORDERS') }}
)

stg_region AS ( 
    SELECT
        ORDERKEY,
        CUSTKEY,
        ORDERSTATUS,
        TOTALPRICE,
        ORDERDATE,
        ORDERPRIORITY,
        CLERK,
        SHIPPRIORITY,
        COMMENT,
        LOAD_TIMESTAMP
FROM orders_source
)
SELECT *
FROM stg_orders