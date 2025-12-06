{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with raw_rates as (
    select * from {{ source('raw', 'rates') }}
),

cleaned as (
    select
        symbol,
        
        -- Convert millisecond timestamps to timestamp
        to_timestamp(open_time / 1000.0) as open_time_utc,
        to_timestamp(close_time / 1000.0) as close_time_utc,
        
        -- Price data
        cast(open as numeric(18, 8)) as price_open,
        cast(high as numeric(18, 8)) as price_high,
        cast(low as numeric(18, 8)) as price_low,
        cast(close as numeric(18, 8)) as price_close,
        
        -- Volume data
        cast(volume as numeric(24, 8)) as volume,
        cast(quote_asset_volume as numeric(24, 8)) as quote_asset_volume,
        cast(number_of_trades as integer) as number_of_trades,
        cast(taker_buy_base_asset_volume as numeric(24, 8)) as taker_buy_base_volume,
        cast(taker_buy_quote_asset_volume as numeric(24, 8)) as taker_buy_quote_volume,
        
        -- Extract currency from symbol (e.g., BTCUSDT -> BTC)
        regexp_replace(symbol, 'USDT$', '') as base_currency,
        
        -- Metadata
        case
            when fetched_at ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}' then
                cast(fetched_at as timestamp)
            else
                current_timestamp
        end as fetched_at,
        
        current_timestamp as dbt_loaded_at
        
    from raw_rates
),

with_time_parts as (
    select
        *,
        date_trunc('hour', open_time_utc) as rate_hour,
        date_trunc('day', open_time_utc) as rate_date
    from cleaned
)

select * from with_time_parts