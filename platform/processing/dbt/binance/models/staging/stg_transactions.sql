{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with raw_transactions as (
    select * from {{ source('raw', 'transactions') }}
),

cleaned as (
    select
        cast(tx_id as varchar) as tx_id,
        cast(user_id as varchar) as user_id,
        
        -- Currencies
        upper(trim(source_currency)) as source_currency,
        upper(trim(destination_currency)) as destination_currency,
        
        -- Amounts
        cast(source_amount as numeric(18, 8)) as source_amount,
        cast(destination_amount as numeric(18, 8)) as destination_amount,
        
        -- Timestamp - handle different formats
        case
            when created_at ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}' then
                cast(created_at as timestamp)
            else
                to_timestamp(created_at, 'YYYY-MM-DD HH24:MI:SS')
        end as created_at_utc,
        
        -- Status
        upper(trim(status)) as status,
        
        -- Metadata
        current_timestamp as dbt_loaded_at
        
    from raw_transactions
),

with_derived as (
    select
        *,
        date_trunc('day', created_at_utc) as transaction_date,
        date_trunc('hour', created_at_utc) as transaction_hour,
        extract(year from created_at_utc) as transaction_year,
        extract(month from created_at_utc) as transaction_month,
        extract(day from created_at_utc) as transaction_day
    from cleaned
)

select * from with_derived