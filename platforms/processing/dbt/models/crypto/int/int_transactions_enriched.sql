{{
  config(
    materialized='table',
    schema='int'
  )
}}

/*
Intermediate model: Enrich transactions with USD conversion rates
- Joins transactions with exchange rates at transaction time
- Handles USDT as stable (rate = 1.0)
- Calculates destination_amount_usd for all transactions
*/

with transactions as (
    select * from {{ ref('stg_transactions') }}
    where status = 'COMPLETED'  -- Only completed transactions
),

rates as (
    select
        base_currency,
        open_time_utc,
        close_time_utc,
        price_close,
        rate_hour
    from {{ ref('stg_rates') }}
),

-- Add symbol mapping for joining
transactions_with_symbol as (
    select
        *,
        case
            when destination_currency = 'USDT' then 'USDT'
            else destination_currency
        end as lookup_currency
    from transactions
),

-- Join with rates using LATERAL-style logic
-- For each transaction, find the most recent rate at or before transaction time
transactions_with_rates as (
    select
        t.*,
        r.price_close as exchange_rate_usdt,
        r.open_time_utc as rate_timestamp
    from transactions_with_symbol t
    left join lateral (
        select
            base_currency,
            price_close,
            open_time_utc
        from rates r
        where r.base_currency = t.lookup_currency
          and r.open_time_utc <= t.created_at_utc
        order by r.open_time_utc desc
        limit 1
    ) r on true
),

-- Calculate USD amounts
final as (
    select
        tx_id,
        user_id,
        source_currency,
        source_amount,
        destination_currency,
        destination_amount,
        created_at_utc,
        transaction_date,
        transaction_hour,
        transaction_year,
        transaction_month,
        status,
        
        -- Exchange rate used
        coalesce(exchange_rate_usdt, 
            case when destination_currency = 'USDT' then 1.0 else null end
        ) as exchange_rate_usdt,
        
        rate_timestamp,
        
        -- Calculate USD amount
        case
            -- USDT is already USD equivalent
            when destination_currency = 'USDT' then
                destination_amount
            -- Use fetched rate
            when exchange_rate_usdt is not null then
                destination_amount * exchange_rate_usdt
            -- No rate available
            else
                null
        end as destination_amount_usd,
        
        -- Flag for missing rates
        case
            when destination_currency != 'USDT' and exchange_rate_usdt is null then true
            else false
        end as is_missing_rate,
        
        current_timestamp as dbt_updated_at
        
    from transactions_with_rates
)

select * from final