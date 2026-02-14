-- Deduplicate: if multiple trips match (same vendor, second, location, service), keep first

select
    -- Generate unique trip identifier (surrogate key pattern)
        {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime', 'pickup_location_id', 'service_type']) }} as trip_id,

        -- Identifiers
        vendor_id,
        service_type,
        rate_code_id,

        -- Location IDs
        pickup_location_id,
        dropoff_location_id,

        -- Timestamps
        pickup_datetime,
        dropoff_datetime,

        -- Trip details
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,

        -- Payment breakdown
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        -- ehail_fee,
        improvement_surcharge,
        total_amount,

        -- Enrich with payment type description
        coalesce(payment_type, 0) as payment_type

from {{ ref('int_trips_unioned')}}
qualify row_number() over(
    partition by vendor_id, pickup_datetime, pickup_location_id, service_type
    order by dropoff_datetime
) = 1