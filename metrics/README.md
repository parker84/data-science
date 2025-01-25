# Metrics
This folder contains valuable resources for building metrics.


### Retention
- `fct_accumulating_user_retention_metrics.sql`: calculates retention metrics per user.
- `fct_periodic_monthly_retention_metrics.sql`: aggregates the per user retention metrics to a monthly cohort level.

### Activation
- `fct_accumulating_user_activation_metrics.sql`: calculates time-capped activation metrics per user.
- `fct_periodic_activation_metrics.sql`: aggregates the per user activation metrics into a rolling agregation for the last 30 days. 
  - This ones also parameterized as it's directly from a query used to power a streamlit dashboard. 