{% snapshot user_kyc_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

-- Snapshot to track KYC level changes over time
-- This enables point-in-time analysis of user KYC status
select
    user_id,
    kyc_level,
    updated_at,
    created_at
from {{ ref('stg_users') }}

{% endsnapshot %}

/*
How this works:
1. DBT creates columns: dbt_valid_from, dbt_valid_to, dbt_scd_id
2. When kyc_level changes, old record gets dbt_valid_to set
3. New record gets created with dbt_valid_from = updated_at
4. We can join transactions to get kyc_level at transaction time:
   
   WHERE snapshot.dbt_valid_from <= transaction.created_at
     AND (snapshot.dbt_valid_to > transaction.created_at 
          OR snapshot.dbt_valid_to IS NULL)
*/