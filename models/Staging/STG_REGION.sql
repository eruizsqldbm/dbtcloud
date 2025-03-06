

region_source AS (
    SELECT
        REGIONKEY,
        TRIM(NAME) AS REGION_NAME,
        UPPER(TRIM(COMMENT)) AS REGION_COMMENT,
        CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
    FROM {{ source('TPCH_SF1', 'REGION') }}
),

stg_region AS (
    SELECT
        REGIONKEY,
        REGION_NAME,
        REGION_COMMENT,
        LOAD_TIMESTAMP
    FROM region_source
)
SELECT *
FROM stg_region