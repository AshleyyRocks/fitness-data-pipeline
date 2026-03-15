select
    type as activity_type,
    count(*) as total_activities,
    sum(distance_meters) as total_distance_meters,
    sum(moving_time_seconds) as total_moving_time_seconds,
    avg(distance_meters) as avg_distance_meters
from FIVETRAN_DATABASE.S3.ACTIVITIES
group by 1
order by total_distance_meters desc