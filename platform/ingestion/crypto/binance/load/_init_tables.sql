-- Initialize PostgreSQL Database for Data Platform
-- Create schemas and raw tables

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS int;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS snapshots;

-- Create raw tables for data loading
CREATE TABLE IF NOT EXISTS public.transactions (
    tx_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    source_currency VARCHAR(50),
    destination_currency VARCHAR(50),
    source_amount NUMERIC(18, 8),
    destination_amount NUMERIC(18, 8),
    created_at TIMESTAMP,
    status VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.users (
    user_id VARCHAR(255) PRIMARY KEY,
    kyc_level VARCHAR(10) NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.rates (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    open_time BIGINT NOT NULL,
    close_time BIGINT NOT NULL,
    open NUMERIC(18, 8),
    high NUMERIC(18, 8),
    low NUMERIC(18, 8),
    close NUMERIC(18, 8),
    volume NUMERIC(24, 8),
    quote_asset_volume NUMERIC(24, 8),
    number_of_trades INTEGER,
    taker_buy_base_asset_volume NUMERIC(24, 8),
    taker_buy_quote_asset_volume NUMERIC(24, 8),
    fetched_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, open_time)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON public.transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON public.transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON public.transactions(status);
CREATE INDEX IF NOT EXISTS idx_transactions_dest_currency ON public.transactions(destination_currency);

CREATE INDEX IF NOT EXISTS idx_users_kyc_level ON public.users(kyc_level);
CREATE INDEX IF NOT EXISTS idx_users_updated_at ON public.users(updated_at);

CREATE INDEX IF NOT EXISTS idx_rates_symbol ON public.rates(symbol);
CREATE INDEX IF NOT EXISTS idx_rates_open_time ON public.rates(open_time);
CREATE INDEX IF NOT EXISTS idx_rates_symbol_open_time ON public.rates(symbol, open_time);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dataeng;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dataeng;
GRANT ALL PRIVILEGES ON SCHEMA staging TO dataeng;
GRANT ALL PRIVILEGES ON SCHEMA int TO dataeng;
GRANT ALL PRIVILEGES ON SCHEMA marts TO dataeng;
GRANT ALL PRIVILEGES ON SCHEMA snapshots TO dataeng;