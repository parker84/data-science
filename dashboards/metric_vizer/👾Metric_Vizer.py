import streamlit as st
from decouple import config
import coloredlogs, logging
import snowflake.connector
import pandas as pd
import plotly.express as px
from query_runners import (
    get_parameter_options,
    get_retention_metric,
    get_trial_activation_metrics_by_group,
    get_customer_success_metrics_by_group,
    get_acquisition_metrics_by_group,
    get_user_metrics_by_group,
    get_active_customer_rate_metrics
)
from utils.helpers import convert_df, login
from datetime import datetime, timedelta
logger = logging.getLogger(__name__)
coloredlogs.install(level=config('LOG_LEVEL'))

# --------------setup
st.set_page_config(
    page_title='Metric Vizer', 
    page_icon='👾', 
    layout="wide", 
    initial_sidebar_state="auto", 
    menu_items=None
)
st.title("👾 Metric Vizer")

# --------------helpers

ctx = snowflake.connector.connect(
    user=config('DB_USER'),
    password=config('DB_PASSWORD'),
    account=config('DB_ACCOUNT'),
    client_session_keep_alive=True
)

def get_filter_query_from_filter_dict(filters_dict, prefix='du'):
    if len(filters_dict) == 0:
        return 'and true'
    else:
        filter_query = ''
        for filter_name in filters_dict:
            filter_query += 'and '
            filter_query += 'true \n' if 'Select All' in filters_dict[filter_name] else f"{prefix}.{filter_name} in " + list_to_str_for_sql(filters_dict[filter_name]) + '\n'
        return filter_query

def get_results_from_query(filename:str, parameters:dict, logger) -> pd.DataFrame:
    with open(filename, 'r') as f:
        query = f.read()
        query = query.format(**parameters)
        logger.info(f'{filename} query: \n{query}')
    df = pd.read_sql(query, ctx)
    df.columns = [
        col.lower()
        for col in df.columns
    ]
    return df


@st.cache_data()
def get_trial_activation_metrics_by_group(
    start_date,
    end_date,
    total_metrics_by_last_n_days,
    var_to_group_by,
    filters_dict={}
):
    filter_query = get_filter_query_from_filter_dict(filters_dict)
    parameters = dict(
        DB_NAME=config('DB_NAME'),
        DB_SCHEMA=config('DB_SCHEMA'),
        start_date=start_date, 
        end_date=end_date, 
        var_to_group_by=var_to_group_by,
        total_metrics_by_last_n_days=total_metrics_by_last_n_days,
        filters=filter_query,
    )
    return get_results_from_query(
        './sql/status/metric_vizer/get_trial_activation_metrics_by_group.sql',
        parameters, logger
    )

def plot_rate_metric(
        total_metrics_by_last_n_days, 
        var_to_group_by_col, 
        metric_col, 
        metric_df, 
        hover_data=[], 
        decimals=1
    ):
    col1, col2 = st.columns(2)
    with col1:
        p = px.line(
                metric_df,
                x='Date',
                y=metric_col,
                color=var_to_group_by_col,
                title=f'{metric_col} by {var_to_group_by_col} ({total_metrics_by_last_n_days}d rolling window)',
                hover_data=hover_data
            )
        p.update_layout(yaxis_tickformat=f'.{decimals}%')
        st.plotly_chart(p, use_container_width=True)
    with col2:
        bar_metric_df = metric_df[metric_df[metric_col].isnull() == False]
        bar_metric_df = bar_metric_df[bar_metric_df['Date'] == bar_metric_df['Date'].max()]
        bar_metric_df = bar_metric_df.sort_values(by=metric_col, ascending=True)
        bar_metric_df[metric_col + '_str'] = [
                str(round(100 * number, decimals)) + '%' for number in
                bar_metric_df[metric_col]
            ]
        p = px.bar(
                bar_metric_df,
                y=var_to_group_by_col,
                x=metric_col,
                orientation='h',
                title=f'{metric_col} by {var_to_group_by_col} (last {total_metrics_by_last_n_days}d)',
                text=metric_col + '_str',
                hover_data=hover_data,
            )
        p.update_layout(xaxis_tickformat=f'.{decimals}%')
        st.plotly_chart(p, use_container_width=True)

def plot_totals_metric(
        total_metrics_by_last_n_days, 
        var_to_group_by_col, 
        metric_col, 
        metric_df, 
        hover_data=[],
        order_legend_by='totals',
        format_pct=False
    ):
    col1, col2 = st.columns(2)
    donut_metric_df = metric_df[metric_df[metric_col].isnull() == False]
    donut_metric_df = donut_metric_df[
        donut_metric_df['Date'] == donut_metric_df['Date'].max()
    ]
    donut_metric_df = donut_metric_df.sort_values(by=metric_col, ascending=False)
    if order_legend_by == 'totals':
        category_orders={
            var_to_group_by_col: donut_metric_df[var_to_group_by_col].tolist()
        }
    elif order_legend_by == 'alphabetical':
        category_orders={
            var_to_group_by_col: sorted(donut_metric_df[var_to_group_by_col].dropna().tolist())
        }
    else:
        raise NotImplementedError(f'order_legend_by {order_legend_by} not implemented yet')
    with col1:
        p = px.line(
                metric_df,
                x='Date',
                y=metric_col,
                color=var_to_group_by_col,
                title=f'{metric_col} by {var_to_group_by_col} ({total_metrics_by_last_n_days}d rolling window)',
                hover_data=hover_data,
                category_orders=category_orders
            )
        if format_pct:
            p.update_layout(yaxis_tickformat='.0%')
        st.plotly_chart(p, use_container_width=True)
    with col2:
        p = px.pie(
            donut_metric_df,
            names=var_to_group_by_col,
            values=metric_col,
            title=f'{metric_col} by {var_to_group_by_col} (last {total_metrics_by_last_n_days}d)',
            hole=0.4,
            category_orders=category_orders,
            hover_data=hover_data
        )
        p.update_traces(textinfo='percent', textposition='inside')
        st.plotly_chart(p, use_container_width=True)

def plot_avg_metric(
    total_metrics_by_last_n_days,
    var_to_group_by_col,
    metric_col,
    metric_df,
    hover_data=[]
):
    col1, col2 = st.columns(2)
    bar_metric_df = metric_df[metric_df[metric_col].isnull() == False]
    bar_metric_df = bar_metric_df[bar_metric_df['Date'] == bar_metric_df['Date'].max()]
    bar_metric_df = bar_metric_df.sort_values(by=metric_col, ascending=True)
    if bar_metric_df[metric_col].max() > 1:
        decimals = 1
    elif bar_metric_df[metric_col].max() > 0.1:
        decimals = 2
    else:
        decimals = 3
    bar_metric_df[metric_col + '_str'] = [
        str(round(number, decimals)) for number in
        bar_metric_df[metric_col]
    ]
    with col1:
        p = px.line(
            metric_df,
            x='Date',
            y=metric_col,
            color=var_to_group_by_col,
            title=f'{metric_col} by {var_to_group_by_col} ({total_metrics_by_last_n_days}d rolling window)',
            hover_data=hover_data,
            category_orders={
                var_to_group_by_col: bar_metric_df[var_to_group_by_col].tolist()
            }
        )
        st.plotly_chart(p)
    with col2:
        p = px.bar(
            bar_metric_df,
            y=var_to_group_by_col,
            x=metric_col,
            orientation='h',
            title=f'{metric_col} by {var_to_group_by_col} (last {total_metrics_by_last_n_days}d)',
            text=metric_col + '_str',
            hover_data=hover_data,
        )
        p.update_layout(xaxis_tickformat=f'.{decimals}')
        st.plotly_chart(p)


def show_raw_data(total_metrics_by_last_n_days, var_to_group_by_col, metric_col, metric_df):
    with st.expander('Raw 🥩 Data', expanded=False):
        st.dataframe(metric_df)
        csv = convert_df(metric_df)
        st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"{metric_col}_by_{var_to_group_by_col}_{total_metrics_by_last_n_days}d_rolling_window{str(datetime.now().date())}.csv",
                mime="text/csv"
            )
    
def create_variable_mapper_and_inverse_mapper(list_of_cols):
    mapper = {}
    inverse_mapper = {}
    for col in list_of_cols:
        mapper[col] = col.replace('_', ' ').title()
        inverse_mapper[col.replace('_', ' ').title()] = col
    return mapper, inverse_mapper

def get_ltv_metric_df(
        start_date,
        end_date,
        total_metrics_by_last_n_days,
        var_to_group_by,
        filters_dict,
        metric,
        metric_n_days
    ):
        metric_dfs = {}
        for n_days in range(30, metric_n_days + 1, 30):
            metric_df = get_retention_metric(
                start_date=start_date,
                end_date=end_date,
                total_metrics_by_last_n_days=total_metrics_by_last_n_days,
                var_to_group_by=var_to_group_by,
                metric_n_days=n_days,
                filters_dict=filters_dict
            ).rename (
                columns={
                    'date': 'Date',
                    var_to_group_by: var_to_group_by_col,
                    f'count_retained_customers_for_{n_days}d_last_n_days_totals': f'Count Customers Retained {n_days}d'
                }
            )
            metric_dfs[n_days] = metric_df
        retention_df = metric_dfs[30]
        for n_days in range(60, metric_n_days + 1, 30):
            retention_df = retention_df.merge(
                metric_dfs[n_days],
                on=['Date', var_to_group_by_col],
                how='left'
            )
        metric_col = metric.replace('ltv_', 'LTV ')
        retention_metrics = retention_df[[f'retention_{n_days}d' for n_days in range(30, metric_n_days + 1, 30)]]
        retention_df[metric_col] = ((
            1 + retention_metrics.sum(axis=1)
        ) * 29).round(0) # TODO: factor in Creator Pro as well
        # ) * (29 * 0.9 + 99 * 0.1)).round(0) # TODO: factor in Creator Pro as well
        retention_df[f'Retention {metric_n_days}d'] = retention_df[f'retention_{metric_n_days}d']
        retention_df.dropna(subset=[f'retention_{metric_n_days}d'], inplace=True)
        return retention_df, metric_col

#---------constants
RETENTION_METRICS = [
    'retention_180d',
    'retention_30d',
    'retention_60d',
    'retention_360d',
]
LTV_METRICS = [
    'ltv_180d',
    'ltv_360d',
]
TRIAL_ACTIVATION_METRICS = [
    'trial_to_customer_rate_30d',
    'trial_to_live_rate_1d',
    ...

]
CUSTOMER_SUCCESS_METRICS_unflattened = [
    [
        f'customer_to_at_least_100_gmv_rate_in_{first_n_days}d',
        ...
    ]
    for first_n_days in [30, 60, 180]
]
CUSTOMER_SUCCESS_METRICS = [
    item for sublist in CUSTOMER_SUCCESS_METRICS_unflattened 
    for item in sublist
]
ACQUISITION_METRICS = [
    'new_trials',
    'new_customers',
]
AVG_USER_METRICS = [
    'avg_referrals_per_user',
    'avg_gmv_per_user',
    'avg_leads_per_user',
    ...
]
TOTALS_USER_METRICS = [
    'store_visits',
    'leads',
    'gmv',
    ...
]
RATE_USER_METRICS = [
    'store_visits_to_referrals',
]
ACTIVE_CUSTOMER_RATE_METRICS = [
    'churn_rate'
]
USER_METRICS = AVG_USER_METRICS + TOTALS_USER_METRICS + RATE_USER_METRICS
METRIC_OPTIONS = (
    RETENTION_METRICS + 
    TRIAL_ACTIVATION_METRICS + 
    ACQUISITION_METRICS + 
    CUSTOMER_SUCCESS_METRICS + 
    USER_METRICS + 
    LTV_METRICS + 
    ACTIVE_CUSTOMER_RATE_METRICS
)
(
    METRIC_OPTIONS_RAW_TO_CLEAN_MAPPER, 
    METRIC_OPTIONS_CLEAN_TO_RAW_MAPPER
) = create_variable_mapper_and_inverse_mapper(METRIC_OPTIONS)

VAR_TO_GROUP_BY_OPTIONS = [
    'all_users',
    'niche',
    'attribution',
    'total_gmv_in_first_30d_binned',
    'count_unique_store_visits_in_first_30d_binned',
    'ideal_user_status',
    'stan_goal_multiple_choice',
    'country',
    ...
]

(
    VAR_TO_GROUP_BY_OPTIONS_RAW_TO_CLEAN_MAPPER,
    VAR_TO_GROUP_BY_OPTIONS_CLEAN_TO_RAW_MAPPER
) = create_variable_mapper_and_inverse_mapper(VAR_TO_GROUP_BY_OPTIONS)

if login():

    col1, col2 = st.columns(2)
    with col1:
        metric_col = st.selectbox(
            'Metric 📈',
            METRIC_OPTIONS_RAW_TO_CLEAN_MAPPER.values()
        )
    with col2:
        var_to_group_by_col = st.selectbox(
            'Group By 🧨',
            VAR_TO_GROUP_BY_OPTIONS_RAW_TO_CLEAN_MAPPER.values()
        )
    var_to_group_by = VAR_TO_GROUP_BY_OPTIONS_CLEAN_TO_RAW_MAPPER[var_to_group_by_col]
    metric = METRIC_OPTIONS_CLEAN_TO_RAW_MAPPER[metric_col]

    # --------------parameters
    total_metrics_by_last_n_days = st.sidebar.selectbox(
        'Rolling Window (Last N Days)', options=[30,7,1], help='30 => metrics will be aggregated based on the last 30 days before a given date. 30 is more stable but slower to react than 7 days.'
    )

    if total_metrics_by_last_n_days == 30:
        day_diff_back = 361 #+ 90
    elif total_metrics_by_last_n_days == 7:
        day_diff_back = 91

    start_date = st.sidebar.date_input(
        'Start Date', 
        datetime.today().date() - timedelta(days=day_diff_back),
        help='Start Date of Charts'
    )
    end_date = st.sidebar.date_input(
        'End Date',
        datetime.today().date() - timedelta(days=1),
        help='End Date of Charts / Metrics'
    )

    filters_dict = {}
    with st.sidebar.expander('User Filters', expanded=False):
        CHOSEN_USER_FILTERS = [ # these need to equal their name on dim_users
            'ideal_user_status',
            'stan_customer_status',
            'niche',
            ...
  
        ]
        parameter_options = get_parameter_options(CHOSEN_USER_FILTERS)
        for filter_name in CHOSEN_USER_FILTERS:
            default_options = (
                parameter_options.groupby(filter_name)['count_users'].sum().reset_index()
                .sort_values('count_users', ascending=False)
                .dropna(subset=[filter_name])
                [filter_name].tolist()
            )
            filters_dict[filter_name] = st.multiselect(
                label=filter_name.replace('_', ' ').title(),
                options = ['Select All'] + default_options,
                default = ['Select All']
            )

    order_legend_by = 'totals'

    if metric.startswith('retention'):
        metric_n_days = int(metric.split('_')[-1].strip('d'))
        count_customers_retained_col = f'Count Customers Retained {metric_n_days}d'
        metric_df = get_retention_metric(
            start_date=start_date,
            end_date=end_date,
            total_metrics_by_last_n_days=total_metrics_by_last_n_days,
            var_to_group_by=var_to_group_by,
            metric_n_days=metric_n_days,
            filters_dict=filters_dict
        ).rename (
            columns={
                'date': 'Date',
                metric: metric_col,
                var_to_group_by: var_to_group_by_col,
                f'count_retained_customers_for_{metric_n_days}d_last_n_days_totals': count_customers_retained_col
            }
        )

        plot_rate_metric(
            total_metrics_by_last_n_days, 
            var_to_group_by_col, 
            metric_col, 
            metric_df,
            hover_data=[
                    count_customers_retained_col
                ]
        )

        show_raw_data(total_metrics_by_last_n_days, var_to_group_by_col, metric_col, metric_df)
    
    elif metric.startswith('trial_to'):
        metric_df = get_trial_activation_metrics_by_group(
            start_date=start_date,
            end_date=end_date,
            total_metrics_by_last_n_days=total_metrics_by_last_n_days,
            var_to_group_by=var_to_group_by, 
            filters_dict=filters_dict
        ).rename (
            columns={
                'date': 'Date',
                metric: metric_col,
                var_to_group_by: var_to_group_by_col,
                'count_trials_in_first_14d_last_n_days_totals': 'Count Trials',
            }
        )

        plot_rate_metric(
            total_metrics_by_last_n_days, 
            var_to_group_by_col, 
            metric_col, 
            metric_df,
            hover_data=[
                    'Count Trials'
            ]
        )

        with st.expander('Change 📉 Breakdown'):
            col1, col2 = st.columns(2)
            with col2:
                end_date_default = metric_df[metric_df[metric_col].isnull() == False]['Date'].max()
                end_date_change = st.date_input(
                    'End Date for Change',
                    end_date_default,
                    help='Date to end calculating change from'
                )
            with col1:
                start_date_change = st.date_input(
                    'Start Date for Change', 
                    end_date_default - timedelta(days=30),
                    help='Date to start calculating change from'
                )
            end_metric_df = metric_df[metric_df['Date'] == end_date_change]
            start_metric_df = metric_df[metric_df['Date'] == start_date_change]
            change_df = end_metric_df.merge(
                start_metric_df,
                on=var_to_group_by_col,
                suffixes=('_end', '_start')
            )
            change_df['relative_change'] = (
                100 * ((change_df[metric_col + '_end'] - change_df[metric_col + '_start']) / change_df[metric_col + '_start'])
            )
            change_df['absolute_change'] = (
                100 * (change_df[metric_col + '_end'] - change_df[metric_col + '_start'])
            )
            change_df['avg_trials'] = (
                (change_df['Count Trials_end'] + change_df['Count Trials_start']) / 2
            )
            change_df['weight'] = (
                change_df['avg_trials'] / change_df['avg_trials'].sum()
            )
            change_df['weighted_relative_change'] = (
                change_df['weight'] * change_df['relative_change']
            )
            change_df['weighted_absolute_change'] = (
                change_df['weight'] * change_df['absolute_change']
            )
            change_df['pct_of_weighted_absolute_change'] = (
                100 * ((change_df['weighted_absolute_change'] / change_df['weighted_absolute_change'].sum()))
            )
            change_df['weight_100'] = change_df['weight'] * 100

            total_metric_start = (change_df[metric_col + '_start'] * change_df['Count Trials_start']).sum() / change_df['Count Trials_start'].sum()
            total_metric_end = (change_df[metric_col + '_end'] * change_df['Count Trials_end']).sum() / change_df['Count Trials_end'].sum()
            total_metric_change = total_metric_end - total_metric_start
            total_metric_relative_change = total_metric_change / total_metric_start

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric(
                    label=f'Start {metric_col}',
                    value='{:,}%'.format((total_metric_start*100).round(1))
                )
            with col2:
                st.metric(
                    label=f'End {metric_col}',
                    value='{:,}%'.format((total_metric_end*100).round(1))
                )
            with col3:
                st.metric(
                    label='Relative Change',
                    value='{:,}%'.format((total_metric_relative_change*100).round(1))
                )
            with col4:
                st.metric(
                    label='Absolute Change',
                    value='{:,}%'.format((total_metric_change*100).round(1))
                )

            viz_df = change_df[[
                var_to_group_by_col,
                'pct_of_weighted_absolute_change',
                'relative_change',
                'absolute_change',
                'weight_100',
                metric_col + '_start',
                metric_col + '_end',
            ]]
            viz_df.index = viz_df[var_to_group_by_col]
            viz_df.drop(columns=[var_to_group_by_col], inplace=True)
            viz_df.sort_values(by='pct_of_weighted_absolute_change', ascending=False, inplace=True)
            viz_df[metric_col + '_start'] = 100 * viz_df[metric_col + '_start']
            viz_df[metric_col + '_end'] = 100 * viz_df[metric_col + '_end']
            for col in viz_df.columns:
                viz_df[col] = viz_df[col].astype(float)
            st.dataframe(
                viz_df, 
                column_config={
                    'pct_of_weighted_absolute_change': st.column_config.ProgressColumn(
                        "% of Absolute Change 📊",
                        format="%.1f%%",
                        width="medium",
                        min_value=0,
                        max_value=viz_df['pct_of_weighted_absolute_change'].max(),
                    ),
                    'weight_100': st.column_config.ProgressColumn(
                        "% of Trials",
                        format="%.1f%%",
                        width="medium",
                        min_value=0,
                        max_value=viz_df['weight_100'].max(),
                    ),
                    'absolute_change': st.column_config.NumberColumn(
                        "Absolute Change",
                        format="%.1f%%"
                    ),
                    'relative_change': st.column_config.NumberColumn(
                        "Relative Change",
                        format="%.1f%%"
                    ),
                    metric_col + '_start': st.column_config.ProgressColumn(
                        f'Start {metric_col}',
                        format="%.0f%%",
                        width="medium",
                        min_value=0,
                        max_value=viz_df[metric_col + '_start'].max(),
                    ),
                    metric_col + '_end': st.column_config.ProgressColumn(
                        f'End {metric_col}',
                        format="%.0f%%",
                        width="medium",
                        min_value=0,
                        max_value=viz_df[metric_col + '_end'].max(),
                    ),
                },
                use_container_width=True
            )

            st.caption(
                f'Total Weighted Absolute Change: `{change_df["weighted_absolute_change"].sum():.1f}%`' +
                ' (should be similar to the total absolute change above - the gap represents mix-shifts in the underlying groups driving the change)'
            )




        show_raw_data(
            total_metrics_by_last_n_days, 
            var_to_group_by_col, 
            metric_col, 
            metric_df
        )

    elif metric.startswith('customer_to'):
        first_n_days = int(metric.split('_')[-1].strip('d'))
        metric_df = get_customer_success_metrics_by_group(
            start_date=start_date,
            end_date=end_date,
            total_metrics_by_last_n_days=total_metrics_by_last_n_days,
            var_to_group_by=var_to_group_by, 
            first_n_days=first_n_days,
            filters_dict=filters_dict
        ).rename (
            columns={
                'date': 'Date',
                metric: metric_col,
                var_to_group_by: var_to_group_by_col,
                'count_customers_in_first_n_days_last_n_days_totals': 'Count Customers',
            }
        )

        plot_rate_metric(
            total_metrics_by_last_n_days, 
            var_to_group_by_col, 
            metric_col, 
            metric_df,
            hover_data=[
                    'Date',
                    metric_col,
                    'Count Customers'
            ]
        )

        show_raw_data(
            total_metrics_by_last_n_days, 
            var_to_group_by_col, 
            metric_col, 
            metric_df
        )

    elif metric.startswith('new_'):
        metric_df = get_acquisition_metrics_by_group(
            start_date=start_date,
            end_date=end_date,
            total_metrics_by_last_n_days=total_metrics_by_last_n_days,
            var_to_group_by=var_to_group_by, 
            filters_dict=filters_dict
        ).rename (
            columns={
                'date': 'Date',
                metric: metric_col,
                var_to_group_by: var_to_group_by_col,
            }
        )
        plot_totals_metric(
            total_metrics_by_last_n_days, 
            var_to_group_by_col, 
            metric_col, 
            metric_df,
            order_legend_by=order_legend_by
        )

        with st.expander('Change 📉 Breakdown'):
            col1, col2 = st.columns(2)
            with col2:
                end_date_default = metric_df[metric_df[metric_col].isnull() == False]['Date'].max()
                end_date_change = st.date_input(
                    'End Date for Change',
                    end_date_default,
                    help='Date to end calculating change from'
                )
            with col1:
                start_date_change = st.date_input(
                    'Start Date for Change', 
                    end_date_default - timedelta(days=30),
                    help='Date to start calculating change from'
                )
            end_metric_df = metric_df[metric_df['Date'] == end_date_change]
            start_metric_df = metric_df[metric_df['Date'] == start_date_change]
            change_df = end_metric_df.merge(
                start_metric_df,
                on=var_to_group_by_col,
                suffixes=('_end', '_start')
            )
            change_df['relative_change'] = (
                100 * ((change_df[metric_col + '_end'] - change_df[metric_col + '_start']) / change_df[metric_col + '_start'])
            )
            change_df['absolute_change'] = (
                (change_df[metric_col + '_end'] - change_df[metric_col + '_start'])
            )
            change_df['pct_of_absolute_change'] = (
                100 * ((change_df['absolute_change'] / change_df['absolute_change'].sum()))
            )
            change_df['pct_of_metric_at_start'] = (
                100 * ((change_df[metric_col + '_start'] / change_df[metric_col + '_start'].sum()))
            )
            change_df['pct_of_metric_at_end'] = (
                100 * ((change_df[metric_col + '_end'] / change_df[metric_col + '_end'].sum()))
            )

            total_metric_start = change_df[metric_col + '_start'].sum()
            total_metric_end = change_df[metric_col + '_end'].sum()
            total_metric_change = total_metric_end - total_metric_start
            total_metric_relative_change = total_metric_change / total_metric_start

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric(
                    label=f'Start {metric_col}',
                    value='{:,}'.format(total_metric_start)
                )
            with col2:
                st.metric(
                    label=f'End {metric_col}',
                    value='{:,}'.format(total_metric_end)
                )
            with col3:
                st.metric(
                    label='Relative Change',
                    value='{:,}%'.format((total_metric_relative_change*100).round(1))
                )
            with col4:
                st.metric(
                    label='Absolute Change',
                    value='{:,}'.format(total_metric_change)
                )

            viz_df = change_df[[
                var_to_group_by_col,
                'pct_of_absolute_change',
                'relative_change',
                'absolute_change',
                metric_col + '_start',
                metric_col + '_end',
                'pct_of_metric_at_start',
                'pct_of_metric_at_end'
            ]]
            viz_df.index = viz_df[var_to_group_by_col]
            viz_df.drop(columns=[var_to_group_by_col], inplace=True)
            viz_df.sort_values(by='pct_of_absolute_change', ascending=False, inplace=True)
            for col in viz_df.columns:
                viz_df[col] = viz_df[col].astype(float)
            st.dataframe(
                viz_df, 
                column_config={
                    'pct_of_absolute_change': st.column_config.ProgressColumn(
                        "% of Absolute Change 📊",
                        format="%.1f%%",
                        width="medium",
                        min_value=0,
                        max_value=viz_df['pct_of_absolute_change'].max(),
                    ),
                    'absolute_change': st.column_config.NumberColumn(
                        "Absolute Change",
                        format="%.0f"
                    ),
                    'relative_change': st.column_config.NumberColumn(
                        "Relative Change",
                        format="%.1f%%"
                    ),
                    metric_col + '_start': st.column_config.ProgressColumn(
                        f'Start {metric_col}',
                        format="%.0f",
                        min_value=0,
                        max_value=viz_df[metric_col + '_start'].max(),
                    ),
                    metric_col + '_end': st.column_config.ProgressColumn(
                        f'End {metric_col}',
                        format="%.0f",
                        min_value=0,
                        max_value=viz_df[metric_col + '_end'].max(),
                    ),
                    'pct_of_metric_at_start': st.column_config.ProgressColumn(
                        f'% of {metric_col} at Start',
                        format="%.1f%%",
                        min_value=0,
                        max_value=viz_df['pct_of_metric_at_start'].max(),
                    ),
                    'pct_of_metric_at_end': st.column_config.ProgressColumn(
                        f'% of {metric_col} at End',
                        format="%.1f%%",
                        min_value=0,
                        max_value=viz_df['pct_of_metric_at_end'].max(),
                    ),
                },
                use_container_width=True
            )

        with st.expander('➗ Breakdowns over Time'):
            totals_per_date = metric_df.groupby('Date')[metric_col].sum().reset_index()
            metric_df = metric_df.merge(
                totals_per_date,
                on='Date',
                suffixes=('', '_total')
            )
            metric_df[metric_col + ' (%)'] = metric_df[metric_col] / metric_df[metric_col + '_total']
            plot_totals_metric(
                total_metrics_by_last_n_days,
                var_to_group_by_col,
                metric_col + ' (%)',
                metric_df,
                order_legend_by=order_legend_by,
                format_pct=True
            )

        show_raw_data(
            total_metrics_by_last_n_days, 
            var_to_group_by_col, 
            metric_col, 
            metric_df
        )

    elif metric in USER_METRICS:
        metric_df = get_user_metrics_by_group(
            start_date=start_date,
            end_date=end_date,
            total_metrics_by_last_n_days=total_metrics_by_last_n_days,
            var_to_group_by=var_to_group_by, 
            filters_dict=filters_dict
        ).rename (
            columns={
                'date': 'Date',
                metric: metric_col,
                var_to_group_by: var_to_group_by_col,
                'count_unique_users_last_n_days_totals': 'Count Users'
            }
        )
        if metric in AVG_USER_METRICS:
            plot_avg_metric(
                total_metrics_by_last_n_days, 
                var_to_group_by_col, 
                metric_col, 
                metric_df,
                hover_data=[
                        'Count Users'
                ]
            )
        elif metric in TOTALS_USER_METRICS:
            plot_totals_metric(
                total_metrics_by_last_n_days, 
                var_to_group_by_col, 
                metric_col, 
                metric_df,
                hover_data=[
                        'Count Users'
                ],
                order_legend_by=order_legend_by
            )
        elif metric in RATE_USER_METRICS:
            if metric == 'store_visits_to_referrals':
                decimals = 2
            else: 
                decimals = 1
            plot_rate_metric(
                total_metrics_by_last_n_days, 
                var_to_group_by_col, 
                metric_col, 
                metric_df,
                hover_data=[
                        'Count Users'
                ],
                decimals=decimals
            )
        show_raw_data(
            total_metrics_by_last_n_days, 
            var_to_group_by_col, 
            metric_col, 
            metric_df
        )
    
    elif metric in LTV_METRICS:
        metric_n_days = int(metric.split('_')[-1].strip('d'))
        ltv_metric_df, metric_col = get_ltv_metric_df(
            start_date=start_date,
            end_date=end_date,
            total_metrics_by_last_n_days=total_metrics_by_last_n_days,
            var_to_group_by=var_to_group_by,
            filters_dict=filters_dict,
            metric=metric,
            metric_n_days=metric_n_days
        )
        col1, col2 = st.columns(2)
        with col1:
            p = px.line(
                    ltv_metric_df,
                    x='Date',
                    y=metric_col,
                    color=var_to_group_by_col,
                    title=f'{metric_col} by {var_to_group_by_col} ({total_metrics_by_last_n_days}d rolling window)',
                    hover_data=[
                        'Date',
                        metric_col,
                        f'Count Customers Retained {metric_n_days}d',
                        f'Retention {metric_n_days}d'
                    ]
                )
            st.plotly_chart(p)
        
        with col2:
            bar_metric_df = ltv_metric_df[ltv_metric_df['Date'] == ltv_metric_df['Date'].max()]
            bar_metric_df = bar_metric_df.sort_values(by=metric_col, ascending=True)
            p = px.bar(
                    bar_metric_df,
                    y=var_to_group_by_col,
                    x=metric_col,
                    orientation='h',
                    title=f'{metric_col} by {var_to_group_by_col} (last {total_metrics_by_last_n_days}d)',
                    text=metric_col,
                    hover_data=[
                        'Date',
                        metric_col,
                        f'Count Customers Retained {metric_n_days}d',
                        f'Retention {metric_n_days}d'
                    ]
                )
            p.update_layout(xaxis_tickformat=f'$,.0f')
            st.plotly_chart(p)
        
        show_raw_data(
            total_metrics_by_last_n_days, 
            var_to_group_by_col, 
            metric_col, 
            ltv_metric_df
        )

    elif metric in ACTIVE_CUSTOMER_RATE_METRICS:
        metric_df = get_active_customer_rate_metrics(
            start_date=start_date,
            end_date=end_date,
            total_metrics_by_last_n_days=total_metrics_by_last_n_days,
            var_to_group_by=var_to_group_by, 
            filters_dict=filters_dict
        ).rename (
            columns={
                'date': 'Date',
                'customer_churn_rate_last_30d': metric_col,
                var_to_group_by: var_to_group_by_col,
            }
        )
        plot_rate_metric(
            total_metrics_by_last_n_days, 
            var_to_group_by_col, 
            metric_col, 
            metric_df
        )
        show_raw_data(
            total_metrics_by_last_n_days, 
            var_to_group_by_col, 
            metric_col, 
            metric_df
        )
        
    else:
        raise NotImplementedError(f"metric {metric} not implemented yet")