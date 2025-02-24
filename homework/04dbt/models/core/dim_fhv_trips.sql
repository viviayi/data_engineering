WITH base_fhv_trips AS (
    SELECT 
        stg.*,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month
    FROM {{ ref('stg_fhv_tripdata') }} stg
),

fhv_with_zones AS (
    SELECT 
        base.*,
        pickup_zone.borough AS pickup_borough,
        pickup_zone.zone AS pickup_zone_name,
        dropoff_zone.borough AS dropoff_borough,
        dropoff_zone.zone AS dropoff_zone_name
    FROM base_fhv_trips base
    LEFT JOIN {{ ref('dim_zones') }} pickup_zone
        ON base.pickup_location_id = pickup_zone.locationid
    LEFT JOIN {{ ref('dim_zones') }} dropoff_zone
        ON base.dropoff_location_id = dropoff_zone.locationid
)

SELECT * FROM fhv_with_zones
