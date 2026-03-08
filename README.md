# dbt Analytics Project

## Owner
Yuli Dobson

## Project Overview
Data pipeline for a pet health subscription service

## Sources
There are 3 sources stored in S3 bucket:
- **`customers`**
- **`subscriptions`**
- **`plans`**

## Models

### Staging Layer - Views
- **`stg_customers`** - Customer data 
- **`stg_subscriptions`** - Subscription data
- **`stg_plans`** - Plans

### Intermediate Layer - Tables
- **`int_customers`** - Clean customer data
- **`int_subscriptions`** - Subscription data with plan details

### Marts Layer - Tables
- **`dim_customers`** - Customer dimension with aggregated metrics
- **`fct_mrr_monthly`** - Monthly Recurring Revenue fact table

## Packages
**`dbt_utils`**

## Notes
1. The Marts layer is built based on the assumption that subscriptions are paid every 30 days (not calendar month) on the first day.
2. This pipeline is created to work with Athena AWS.
3. Documentation is created on local host.
