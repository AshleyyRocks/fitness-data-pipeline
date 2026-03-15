with activities as (

    select *
    from FIVETRAN_DATABASE.S3.ACTIVITIES

),

weekly as (

    select
        date_trunc('week', start_date) as week_start,
        count(*) as total_activities,
        sum(distance_meters) as total_distance_meters,
        sum(moving_time_seconds) as total_moving_time_seconds,
        avg(distance_meters) as avg_distance_meters
    from activities
    group by 1

)

select *
from weekly
order by week_start