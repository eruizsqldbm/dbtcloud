{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH stg_lineitem AS (
    SELECT
	    ORDERKEY,
	    PARTKEY,
	    SUPPKEY,
	    LINENUMBER,
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
	    COMMENT
    FROM {{ ref( "STG_LINEITEM" ) }}
),


dim_lineitem AS (
    SELECT
	    ORDERKEY,
	    PARTKEY,
	    SUPPKEY,
	    LINENUMBER,
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
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
FROM stg_lineitem
)
SELECT *
FROM dim_lineitem