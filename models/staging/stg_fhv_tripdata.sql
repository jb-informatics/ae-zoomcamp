with source as (
    select * 
    from  {{ source('raw_data', 'fhv_tripdata_2019')}}
)

select
    -- identifiers
    dispatching_base_num,
    pickup_datetime,
    drop_off_datetime as dropoff_datetime,
    cast(p_ulocation_id as integer) as pickup_location_id,
    cast(d_olocation_id as integer) as dropoff_location_id,
    cast(sr_flag as integer) as sr_flag,
    affiliated_base_number
from source
where dispatching_base_num is not null