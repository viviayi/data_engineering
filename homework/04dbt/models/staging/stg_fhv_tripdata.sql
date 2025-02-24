WITH fhv_data AS (
    SELECT
        dispatching_base_num,
        SAFE_CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        SAFE_CAST(dropOff_datetime AS TIMESTAMP) AS dropoff_datetime,
        SAFE_CAST(PUlocationID AS INT64) AS pickup_location_id,
        SAFE_CAST(DOlocationID AS INT64) AS dropoff_location_id,
        SAFE_CAST(SR_Flag AS INT64) AS sr_flag,
        affiliated_base_number
    FROM {{ source('staging', 'external_fhv_2019') }}
    WHERE dispatching_base_num IS NOT NULL
)

SELECT * FROM fhv_data
