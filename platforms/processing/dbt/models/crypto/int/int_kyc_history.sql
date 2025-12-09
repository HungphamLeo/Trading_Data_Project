{{
  config(
    materialized='table',
    schema='int'
  )
}}

/*
Intermediate model: Join transactions with KYC level at transaction time
- Uses snapshot to get historical KYC level
- Critical for requirement: "know kyc_level at the time transaction occurred"
*/

with transactions as (
    select * from {{ ref('int_transactions_enriched') }}
),

kyc_snapshot as (
    select
        user_id,
        kyc_level,
        dbt_valid_from,
        dbt_valid_to,
        dbt_scd_id
    from {{ ref('user_kyc_snapshot') }}
),

-- Join transaction with snapshot record that was valid at transaction time
transactions_with_kyc as (
    select
        t.*,
        s.kyc_level as kyc_level_at_transaction,
        s.dbt_valid_from as kyc_valid_from,
        s.dbt_valid_to as kyc_valid_to,
        s.dbt_scd_id as kyc_snapshot_id
    from transactions t
    left join kyc_snapshot s
        on t.user_id = s.user_id
        and t.created_at_utc >= s.dbt_valid_from
        and (
            t.created_at_utc < s.dbt_valid_to
            or s.dbt_valid_to is null  -- Current record
        )
),

-- Handle users with no KYC history (default to L0 or null)
final as (
    select
        tx_id,
        user_id,
        source_currency,
        source_amount,
        destination_currency,
        destination_amount,
        destination_amount_usd,
        exchange_rate_usdt,
        rate_timestamp,
        created_at_utc,
        transaction_date,
        transaction_hour,
        transaction_year,
        transaction_month,
        status,
        is_missing_rate,
        
        -- KYC level at transaction time (critical business requirement)
        coalesce(kyc_level_at_transaction, 'L0') as kyc_level_at_transaction,
        
        kyc_valid_from,
        kyc_valid_to,
        kyc_snapshot_id,
        
        -- Flag for transactions with missing KYC info
        case
            when kyc_level_at_transaction is null then true
            else false
        end as is_missing_kyc_history,
        
        current_timestamp as dbt_updated_at
        
    from transactions_with_kyc
)

select * from final