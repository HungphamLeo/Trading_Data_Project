{{
  config(
    materialized='table',
    schema='marts'
  )
}}

/*
FACT TABLE: Transaction Facts for BI Analysis
Purpose: Single source of truth for completed transactions with USD values
Answers business questions:
1. Total transaction volume in USD by day/month/quarter
2. Transactions by KYC level at transaction time
3. Historical KYC level tracking
*/

with enriched_transactions as (
    select * from {{ ref('int_kyc_history') }}
),

final as (
    select
        -- Primary Key
        tx_id,
        
        -- Foreign Keys
        user_id,
        
        -- Transaction Details
        source_currency,
        source_amount,
        destination_currency,
        destination_amount,
        status,
        
        -- USD Conversion (Core Business Metric)
        destination_amount_usd,
        exchange_rate_usdt,
        rate_timestamp,
        
        -- KYC at Transaction Time (Critical Requirement #3)
        kyc_level_at_transaction,
        
        -- Time Dimensions for Analytics
        created_at_utc,
        transaction_date,
        date_trunc('week', transaction_date) as transaction_week,
        date_trunc('month', transaction_date) as transaction_month,
        date_trunc('quarter', transaction_date) as transaction_quarter,
        transaction_year,
        extract(month from transaction_date) as month_num,
        extract(quarter from transaction_date) as quarter_num,
        extract(dow from transaction_date) as day_of_week,
        to_char(transaction_date, 'Day') as day_name,
        to_char(transaction_date, 'Month') as month_name,
        
        -- Data Quality Flags
        is_missing_rate,
        is_missing_kyc_history,
        
        -- Metadata
        dbt_updated_at,
        current_timestamp as mart_created_at
        
    from enriched_transactions
    
    -- Only include completed transactions with valid USD amounts
    where status = 'COMPLETED'
      and destination_amount_usd is not null
)

select * from final