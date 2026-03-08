# dbt Analytics Project

## Project Overview
Data pipeline for a pet health subscription service

## Sources
There are 3 sources stored in S3 bucket:
- **`customers`**
- **`subscriptions`**
- **`plans`**

## Models

### Staging Layer
- **`stg_customers`** - Customer data 
- **`stg_subscriptions`** - Subscription data
- **`stg_plans`** - Plans

### Intermediate Layer
- **`int_customers`** - Clean customer data
- **`int_subscriptions`** - Subscription data with plan details

### Marts Layer
- **`dim_customers`** - Customer dimension with aggregated metrics
- **`fct_mrr_monthly`** - Monthly Recurring Revenue fact table

## Packages
**`dbt_utils`**

## Notes
The Marts layer is built based on the assumption that subscriptions are paid every 30 days (not calendar month) on the first day.
