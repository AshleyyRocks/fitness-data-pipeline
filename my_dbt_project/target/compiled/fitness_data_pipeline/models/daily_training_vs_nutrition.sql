with strava_daily as (

    select
        cast(start_date as date) as activity_date,
        count(*) as total_activities,
        sum(distance_meters) as total_distance_meters,
        sum(moving_time_seconds) as total_moving_time_seconds,
        avg(average_heartrate) as avg_heart_rate
    from FIVETRAN_DATABASE.S3.ACTIVITIES
    group by 1

),

nutrition_daily as (

    select
        date as nutrition_date,
        sum(calories) as calories_consumed,
        sum(protein_g) as protein_g,
        sum(carbohydrates_g) as carbohydrates_g,
        sum(fat_g) as fat_g
    from FIVETRAN_DATABASE.PUBLIC.MFP_NUTRITION_RAW
    group by 1

),

measurements_daily as (

    select
        date as measurement_date,
        avg(weight) as weight
    from FIVETRAN_DATABASE.PUBLIC.MFP_MEASUREMENTS_RAW
    group by 1

)

select
    coalesce(s.activity_date, n.nutrition_date, m.measurement_date) as date,
    s.total_activities,
    s.total_distance_meters,
    s.total_moving_time_seconds,
    s.avg_heart_rate,
    n.calories_consumed,
    n.protein_g,
    n.carbohydrates_g,
    n.fat_g,
    m.weight
from strava_daily s
full outer join nutrition_daily n
    on s.activity_date = n.nutrition_date
full outer join measurements_daily m
    on coalesce(s.activity_date, n.nutrition_date) = m.measurement_date
order by date