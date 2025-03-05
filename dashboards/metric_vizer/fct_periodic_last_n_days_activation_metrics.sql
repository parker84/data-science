

with filtered_daily_totals as (
    select 
        date(fpd.first_trial_at) as date,
        du.{var_to_group_by},
        --30d
        sum(count_trials_in_first_30d) as count_trials_in_first_30d,
        sum(count_customers_in_first_30d) as count_customers_in_first_30d
    from {DB_NAME}.{DB_SCHEMA}.fct_periodic_daily_user_trial_activation_metrics as fpd
    join {DB_NAME}.{DB_SCHEMA}.dim_users as du on fpd.user_id = du.user_id
    where true 
        {filters}
    group by 1,2
)

, date_group_combos as (
    select distinct d.date, v.{var_to_group_by} 
    from (select distinct date from filtered_daily_totals) as d
    cross join (select distinct {var_to_group_by} from filtered_daily_totals) as v
)

, daily_totals_filled as (
    select 
        dgc.date,
        dgc.{var_to_group_by},
        coalesce(fdt.count_trials_in_first_30d, 0) as count_trials_in_first_30d,
        coalesce(fdt.count_customers_in_first_30d, 0) as count_customers_in_first_30d
    from date_group_combos as dgc
    left join filtered_daily_totals as fdt on 
        dgc.date = fdt.date and 
        dgc.{var_to_group_by} = fdt.{var_to_group_by}
)

, last_n_days_totals as (
    select 
        date,
        {var_to_group_by},
        ----------30d
        sum(count_trials_in_first_30d) over (
            partition by {var_to_group_by}
            order by date 
            rows between {total_metrics_by_last_n_days} preceding and current row
        ) as count_trials_in_first_30d_last_n_days_totals,
        sum(count_customers_in_first_30d) over (
            partition by {var_to_group_by}
            order by date 
            rows between {total_metrics_by_last_n_days} preceding and current row
        ) as count_customers_in_first_30d_last_n_days_totals
    from filtered_daily_totals
)

, rates as (
    select 
        *,
        ------------30d
        case 
            when datediff('day', date, current_date()) > 30
            then div0(count_customers_in_first_30d_last_n_days_totals, count_trials_in_first_30d_last_n_days_totals)
        end as trial_to_customer_rate_30d
    from last_n_days_totals
    order by 1 desc
)


select * from rates
where date between date('{start_date}') and date('{end_date}')