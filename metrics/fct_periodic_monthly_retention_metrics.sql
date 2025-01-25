
{% set periods = range(30, 900, 30) %}

with monthly_totals as (
    select 
        date_trunc('month', du.first_customer_at) as cohort_month
        ,max(date(du.first_customer_at)) as last_date_in_cohort
        {% for n_days in periods %}
        {% set prev_n_days = n_days - 30 %}
        ,sum(fpd.count_retained_customers_for_{{ n_days }}d) as count_retained_customers_for_{{ n_days }}d
        ,sum(fpd.count_customers_{{ n_days }}d_denominator) as count_customers_{{ n_days }}d_denominator
        {% endfor %}
    from {{ ref('fct_accumulating_user_retention_metrics') }} as fpd
    join {{ ref('dim_users') }} as du on fpd.user_id = du.user_id
    group by 1
)

select 
    *
    ,1 as retention_0d
    {% for n_days in periods %}
    {% set prev_n_days = n_days - 30 %}
    ,case 
        when datediff('day', last_date_in_cohort, current_date()) > {{ n_days }}
        then div0(count_retained_customers_for_{{ n_days }}d, count_customers_{{ n_days }}d_denominator)
    end as retention_{{ n_days }}d
    {% if n_days > 30 %}
    ,case 
        when datediff('day', last_date_in_cohort, current_date()) > {{ n_days }}
        then retention_{{ prev_n_days }}d - retention_{{ n_days }}d
    end as churn_{{ n_days }}d
    {% else %}
    ,case 
        when datediff('day', last_date_in_cohort, current_date()) > {{ n_days }}
        then 1 - retention_{{ n_days }}d
    end as churn_{{ n_days }}d
    {% endif %}
    {% endfor %}
from monthly_totals
order by 1 desc