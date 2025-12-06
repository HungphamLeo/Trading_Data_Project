{{
  config(
    materialized='table',
    schema='marts'
  )
}}

/*
DIMENSION TABLE: User Dimension
Purpose: Current user attributes for BI analysis
Note: For historical KYC levels, use the snapshot table or fact_transactions
*/

with current_users as (
    select * from {{ ref('stg_users') }}
),

user_aggregates as (
    select
        user_id,
        count(*) as total_transactions,
        sum(destination_amount_usd) as lifetime_transaction_volume_usd,
        min(created_at_utc) as first_transaction_at,
        max(created_at_utc) as last_transaction_at,
        count(distinct transaction_date) as active_days
    from {{ ref('marts_core__fact_transactions') }}
    group by user_id
),

final as (
    select
        -- Primary Key
        u.user_id,
        
        -- Current User Attributes
        u.kyc_level as current_kyc_level,
        u.created_at as user_created_at,
        u.updated_at as user_updated_at,
        
        -- User Metrics
        coalesce(a.total_transactions, 0) as total_transactions,
        coalesce(a.lifetime_transaction_volume_usd, 0) as lifetime_transaction_volume_usd,
        a.first_transaction_at,
        a.last_transaction_at,
        coalesce(a.active_days, 0) as active_days,
        
        -- Derived Attributes
        case
            when a.total_transactions >= 100 then 'High'
            when a.total_transactions >= 10 then 'Medium'
            when a.total_transactions >= 1 then 'Low'
            else 'None'
        end as transaction_frequency_tier,
        
        case
            when a.lifetime_transaction_volume_usd >= 100000 then 'Whale'
            when a.lifetime_transaction_volume_usd >= 10000 then 'High Value'
            when a.lifetime_transaction_volume_usd >= 1000 then 'Medium Value'
            when a.lifetime_transaction_volume_usd >= 0 then 'Low Value'
            else 'None'
        end as value_tier,
        
        -- Metadata
        current_timestamp as mart_created_at
        
    from current_users u
    left join user_aggregates a on u.user_id = a.user_id
)

select * from final;