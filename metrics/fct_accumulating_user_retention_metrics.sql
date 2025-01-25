with totals as (
    {% set periods = range(30, 900, 30) %}
    select 
        date(du.first_customer_at) first_customer_at
        ,du.user_id
        {% for n_days in periods %}
        ,count(
            distinct 
            case 
                when du.first_customer_at is not null
                and datediff('day', du.first_customer_at, current_date()) > {{ n_days }} -- see which customers joined at least {{ n_days }} ago
            then du.user_id else null end
        ) as count_customers_{{ n_days }}d_denominator
        ,count(
            distinct 
            case 
                when du.first_customer_at is not null
                and datediff('day', du.first_customer_at, current_date()) > {{ n_days }} -- see which customers joined at least {{ n_days }} ago
                and (
                    du.first_cancelled_at is null -- either haven't cancelled
                    or datediff('day', du.first_customer_at, du.first_cancelled_at) > {{ n_days }} -- or they cancelled more than n days after
                    or (-- or they reactivated once and didn't cancel within 30 days
                        datediff('day', du.first_customer_at, du.first_reactivated_at) <= {{ n_days }}
                        and (du.second_cancelled_at is null or datediff('day', du.first_customer_at, du.second_cancelled_at) <= {{ n_days }})
                    )
                )
            then du.user_id else null end
        ) as count_retained_customers_for_{{ n_days }}d
        {% endfor %}
    from {{ ref('dim_users') }} as du
    where true 
        and first_customer_at is not null
    group by 1,2
)


select * from totals