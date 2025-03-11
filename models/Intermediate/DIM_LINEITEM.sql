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
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
        
        -- Calculate the total price (Quantity * ExtendedPrice)
        QUANTITY * EXTENDEDPRICE AS TOTAL_PRICE,
        
        -- Apply discount to extended price (assuming DISCOUNT is a percentage)
        EXTENDEDPRICE * (1 - DISCOUNT / 100) AS ADJUSTED_PRICE,
        
        -- Filter: only include rows where SHIPDATE is not null and QUANTITY > 0
        CASE
            WHEN SHIPDATE IS NOT NULL AND QUANTITY > 0 THEN 'Valid'
            ELSE 'Invalid'
        END AS ROW_STATUS,

        -- Categorize line item by LINESTATUS
        CASE
            WHEN LINESTATUS = 'O' THEN 'Open'
            WHEN LINESTATUS = 'C' THEN 'Complete'
            ELSE 'Pending'
        END AS LINEITEM_STATUS

    FROM stg_lineitem
)
SELECT *
FROM dim_lineitem
WHERE ROW_STATUS = 'Valid'