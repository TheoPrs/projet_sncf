{{ config(materialized='view') }}

SELECT
    *
FROM {{ source('sncf_dataset','raw_streaming') }}
UNION ALL 
SELECT
    *
FROM {{ source('sncf_dataset','raw_batch') }}