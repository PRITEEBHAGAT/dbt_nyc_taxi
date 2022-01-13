{{
  config(
    materialized = "table",
  )
}}
with drop as (

  select * from {{ ref ('drop_taxi')}}
),
pickup as (

  select * from {{ ref ('pickup_taxi')}}
),
final as(
    select
            pickup.trip_id,
            pickup.pickup_longitude,
            pickup.pickup_latitude,
            pickup.pickup_region_code,
            pickup.pickup_region,
            pickup.pickup_country_code,
            pickup.pickup_country_name,
            pickup.payment_mode,
            pickup.pickup_zone,
            pickup.pickup_service_zone,
            pickup.pickup_borough,
            pickup.vendor_name,
            pickup.vendor_id,
            pickup.pu_location_id,
            pickup.do_location_id,
            pickup.tpep_pickup_datetime,

            drop.drop_longitude,
            drop.drop_latitude,
            drop.drop_region_code,
            drop.drop_region,
            drop.drop_country_code,
            drop.drop_country_name,
            drop.drop_zone,
            drop.drop_service_zone,
            drop.drop_borough,
            drop.tpep_dropoff_datetime

            from pickup
            inner join drop on drop.trip_id= pickup.trip_id




)
select * from final