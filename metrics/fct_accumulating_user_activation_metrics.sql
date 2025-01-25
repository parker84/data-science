

with 

daily_totals as (
    select 
        du.user_id,
        date(du.first_trial_at) as first_trial_at,
        ------------------------ 30 day metrics
        count(
            distinct 
            case 
                when du.first_trial_at is not null
                and datediff('day', du.first_trial_at, current_date()) > 30 -- see which customers joined at least 30 days ago
            then du.user_id end
        ) as count_trials_in_first_30d,
        count(
            distinct 
            case 
                when du.first_customer_at is not null 
                and datediff('day', du.first_trial_at, du.first_customer_at) <= 30 -- see which customers converted within 30 days
                and datediff('day', du.first_trial_at, current_date()) > 30 -- see which customers joined at least 30 days ago
            then du.user_id end
        ) as count_customers_in_first_30d
    from {{ ref('dim_users') }} as du
    left join {{ ref('fct_accumulating_users') }} as ua on du.user_id = ua.user_id
    where true 
        and first_trial_at is not null
    group by 1,2
)

select * from daily_totals