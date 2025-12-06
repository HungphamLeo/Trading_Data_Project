-- Sample BI Queries for Business Analysis
-- Use these queries with marts.marts_core__fact_transactions

-- ==========================================
-- REQUIREMENT #1: Transaction Volume by Time Period
-- ==========================================

-- Daily transaction volume in USD
SELECT 
    transaction_date,
    COUNT(*) as transaction_count,
    SUM(destination_amount_usd) as daily_volume_usd,
    AVG(destination_amount_usd) as avg_transaction_usd,
    MIN(destination_amount_usd) as min_transaction_usd,
    MAX(destination_amount_usd) as max_transaction_usd
FROM marts.marts_core__fact_transactions
GROUP BY transaction_date
ORDER BY transaction_date DESC;

-- Monthly transaction volume
SELECT 
    transaction_month,
    COUNT(*) as transaction_count,
    SUM(destination_amount_usd) as monthly_volume_usd,
    AVG(destination_amount_usd) as avg_transaction_usd
FROM marts.marts_core__fact_transactions
GROUP BY transaction_month
ORDER BY transaction_month DESC;

-- Quarterly transaction volume
SELECT 
    transaction_quarter,
    quarter_num,
    transaction_year,
    COUNT(*) as transaction_count,
    SUM(destination_amount_usd) as quarterly_volume_usd,
    AVG(destination_amount_usd) as avg_transaction_usd
FROM marts.marts_core__fact_transactions
GROUP BY transaction_quarter, quarter_num, transaction_year
ORDER BY transaction_quarter DESC;

-- Weekly trend
SELECT 
    transaction_week,
    COUNT(*) as transaction_count,
    SUM(destination_amount_usd) as weekly_volume_usd
FROM marts.marts_core__fact_transactions
GROUP BY transaction_week
ORDER BY transaction_week DESC;

-- ==========================================
-- REQUIREMENT #2: Transactions by KYC Level
-- ==========================================

-- Total volume by KYC level
SELECT 
    kyc_level_at_transaction,
    COUNT(*) as transaction_count,
    SUM(destination_amount_usd) as total_volume_usd,
    AVG(destination_amount_usd) as avg_transaction_usd,
    MIN(destination_amount_usd) as min_transaction_usd,
    MAX(destination_amount_usd) as max_transaction_usd,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_total_transactions,
    ROUND(100.0 * SUM(destination_amount_usd) / SUM(SUM(destination_amount_usd)) OVER (), 2) as pct_of_total_volume
FROM marts.marts_core__fact_transactions
GROUP BY kyc_level_at_transaction
ORDER BY total_volume_usd DESC;

-- Monthly volume by KYC level
SELECT 
    transaction_month,
    kyc_level_at_transaction,
    COUNT(*) as transaction_count,
    SUM(destination_amount_usd) as monthly_volume_usd
FROM marts.marts_core__fact_transactions
GROUP BY transaction_month, kyc_level_at_transaction
ORDER BY transaction_month DESC, monthly_volume_usd DESC;

-- KYC level distribution over time
SELECT 
    transaction_date,
    kyc_level_at_transaction,
    COUNT(*) as transaction_count,
    SUM(destination_amount_usd) as daily_volume_usd
FROM marts.marts_core__fact_transactions
GROUP BY transaction_date, kyc_level_at_transaction
ORDER BY transaction_date DESC, kyc_level_at_transaction;

-- ==========================================
-- REQUIREMENT #3: Historical KYC Level Tracking
-- ==========================================

-- View a specific user's transactions with KYC at transaction time
SELECT 
    tx_id,
    user_id,
    created_at_utc,
    destination_currency,
    destination_amount,
    destination_amount_usd,
    kyc_level_at_transaction as kyc_at_tx_time,
    transaction_date
FROM marts.marts_core__fact_transactions
WHERE user_id = 'USER_ID_HERE'
ORDER BY created_at_utc DESC;

-- Compare current KYC vs KYC at transaction time
SELECT 
    f.tx_id,
    f.user_id,
    f.created_at_utc,
    f.kyc_level_at_transaction as kyc_at_tx_time,
    u.current_kyc_level,
    CASE 
        WHEN f.kyc_level_at_transaction != u.current_kyc_level 
        THEN 'Upgraded'
        ELSE 'No Change'
    END as kyc_status,
    f.destination_amount_usd
FROM marts.marts_core__fact_transactions f
JOIN marts.marts_core__dim_user u ON f.user_id = u.user_id
WHERE f.user_id = 'USER_ID_HERE'
ORDER BY f.created_at_utc DESC;

-- Users who upgraded KYC after their transactions
WITH user_kyc_changes AS (
    SELECT 
        user_id,
        MIN(CASE WHEN kyc_level_at_transaction = 'L0' THEN created_at_utc END) as first_l0_tx,
        MIN(CASE WHEN kyc_level_at_transaction = 'L1' THEN created_at_utc END) as first_l1_tx,
        MIN(CASE WHEN kyc_level_at_transaction = 'L2' THEN created_at_utc END) as first_l2_tx
    FROM marts.marts_core__fact_transactions
    GROUP BY user_id
)
SELECT 
    user_id,
    first_l0_tx,
    first_l1_tx,
    first_l2_tx,
    CASE 
        WHEN first_l2_tx IS NOT NULL THEN 'L0 → L1 → L2'
        WHEN first_l1_tx IS NOT NULL THEN 'L0 → L1'
        ELSE 'No Upgrade'
    END as kyc_journey
FROM user_kyc_changes
WHERE first_l1_tx IS NOT NULL OR first_l2_tx IS NOT NULL;

-- ==========================================
-- Advanced Analytics
-- ==========================================

-- Top currencies by volume
SELECT 
    destination_currency,
    COUNT(*) as transaction_count,
    SUM(destination_amount_usd) as total_volume_usd,
    AVG(destination_amount_usd) as avg_transaction_usd
FROM marts.marts_core__fact_transactions
GROUP BY destination_currency
ORDER BY total_volume_usd DESC
LIMIT 10;

-- User segmentation by transaction volume
SELECT 
    CASE 
        WHEN total_volume_usd >= 100000 THEN 'Whale (>100K)'
        WHEN total_volume_usd >= 10000 THEN 'High Value (10K-100K)'
        WHEN total_volume_usd >= 1000 THEN 'Medium Value (1K-10K)'
        ELSE 'Low Value (<1K)'
    END as user_segment,
    COUNT(DISTINCT user_id) as user_count,
    SUM(total_volume_usd) as segment_volume_usd,
    AVG(total_volume_usd) as avg_user_volume_usd
FROM (
    SELECT 
        user_id,
        SUM(destination_amount_usd) as total_volume_usd
    FROM marts.marts_core__fact_transactions
    GROUP BY user_id
) user_volumes
GROUP BY 1
ORDER BY segment_volume_usd DESC;

-- Day of week analysis
SELECT 
    day_of_week,
    day_name,
    COUNT(*) as transaction_count,
    SUM(destination_amount_usd) as total_volume_usd,
    AVG(destination_amount_usd) as avg_transaction_usd
FROM marts.marts_core__fact_transactions
GROUP BY day_of_week, day_name
ORDER BY day_of_week;

-- Growth rate analysis (Month-over-Month)
WITH monthly_stats AS (
    SELECT 
        transaction_month,
        SUM(destination_amount_usd) as monthly_volume
    FROM marts.marts_core__fact_transactions
    GROUP BY transaction_month
)
SELECT 
    transaction_month,
    monthly_volume,
    LAG(monthly_volume) OVER (ORDER BY transaction_month) as prev_month_volume,
    ROUND(
        100.0 * (monthly_volume - LAG(monthly_volume) OVER (ORDER BY transaction_month)) 
        / NULLIF(LAG(monthly_volume) OVER (ORDER BY transaction_month), 0),
        2
    ) as mom_growth_pct
FROM monthly_stats
ORDER BY transaction_month DESC;

-- Cohort analysis: User first transaction month
WITH user_cohorts AS (
    SELECT 
        user_id,
        DATE_TRUNC('month', MIN(created_at_utc)) as cohort_month
    FROM marts.marts_core__fact_transactions
    GROUP BY user_id
)
SELECT 
    c.cohort_month,
    DATE_TRUNC('month', f.transaction_month) as activity_month,
    COUNT(DISTINCT f.user_id) as active_users,
    SUM(f.destination_amount_usd) as cohort_volume_usd
FROM user_cohorts c
JOIN marts.marts_core__fact_transactions f ON c.user_id = f.user_id
GROUP BY c.cohort_month, DATE_TRUNC('month', f.transaction_month)
ORDER BY c.cohort_month, activity_month;