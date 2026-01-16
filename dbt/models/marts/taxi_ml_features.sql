{{ config(materialized='incremental') }}

SELECT
    pickup_datetime,
    trip_duration_minutes as target,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    pickup_hour,
    dropoff_hour,
    pickup_day_of_week,
    pickup_month,
    is_weekend,
    rush_hour
FROM {{ ref('int_taxi_time_features') }}

{% if is_incremental() %}
    WHERE pickup_datetime >= date('{{ var("data_interval_start") }}')
    AND pickup_datetime < date('{{ var("data_interval_end") }}')
{% endif %}