{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

WITH part_source AS (
    SELECT
	P_PARTKEY,
	P_NAME,
	P_MFGR,
	P_BRAND,
	P_TYPE,
	P_SIZE,
	P_CONTAINER,
	P_RETAILPRICE,
	P_COMMENT
    FROM {{ source('TPCH_SF1', 'PART') }} 
),

stg_part AS (
    SELECT
        P_PARTKEY AS PARTKEY,
        P_NAME AS NAME,
        P_MFGR AS MFGR ,
        P_BRAND AS BRAND,
        P_TYPE AS TYPE,
        P_SIZE AS SIZE,
        P_CONTAINER AS CONTAINER,
        P_RETAILPRICE AS RETAILPRICE,
        P_COMMENT AS COMMENT,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP 
FROM part_source
)
SELECT *
FROM stg_part