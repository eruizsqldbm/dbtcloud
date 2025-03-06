{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH region_source AS (
    SELECT
	    R_REGIONKEY,
	    R_NAME,
	    R_COMMENT
    FROM {{ source('TPCH_SF1', 'REGION') }}
),

stg_region AS (
    SELECT
        R_REGIONKEY AS REGIONKEY,
        R_NAME AS REGION_NAME,
        R_COMMENT AS REGION_COMMENT,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
    FROM region_source
)
SELECT *
FROM stg_region