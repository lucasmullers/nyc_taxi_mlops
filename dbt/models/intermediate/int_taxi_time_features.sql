{{ config(materialized='table') }}

SELECT
    *,
    EXTRACT(hour FROM pickup_datetime) as pickup_hour,
    EXTRACT(hour FROM dropoff_datetime) as dropoff_hour,
    EXTRACT(dayofweek FROM pickup_datetime) as pickup_day_of_week,
    EXTRACT(month FROM pickup_datetime) as pickup_month,
    CASE 
        WHEN EXTRACT(dayofweek FROM pickup_datetime) IN (6, 7) THEN 1 
        ELSE 0 
    END as is_weekend,
    CASE
        WHEN EXTRACT(hour FROM pickup_datetime) IN (7, 8, 9, 10) OR EXTRACT(hour FROM pickup_datetime) IN (16, 17, 18, 19, 20) AND EXTRACT(dayofweek FROM pickup_datetime) NOT IN (6, 7)
        THEN 1
        ELSE 0 
    END as rush_hour
FROM {{ ref('stg_taxi_trips') }}