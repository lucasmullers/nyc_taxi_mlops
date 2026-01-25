{{ config(materialized='incremental') }}

SELECT
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    passenger_count,
    trip_distance,
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    fare_amount,
    tip_amount,
    total_amount,
    -- Calculate trip duration in minutes
    EPOCH(tpep_dropoff_datetime - tpep_pickup_datetime) / 60.0 as trip_duration_minutes
FROM read_parquet('s3://nyc-mlops-data/landing_zone/*.parquet')
WHERE 
    trip_distance > 0
    AND fare_amount > 0
    AND passenger_count BETWEEN 1 AND 6
    AND trip_duration_minutes > 1 
    AND trip_duration_minutes < 180  -- Less than 3 hours
    {% if is_incremental() %}
    AND pickup_datetime >= date('{{ var("data_interval_start") }}')
    AND pickup_datetime < date('{{ var("data_interval_end") }}')
    {% endif %}
