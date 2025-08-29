# Gold Chargeback (Tag-driven)

## Purpose
Provide business chargeback analytics by leveraging normalized tags and identity fields from Silver.

## Inputs
- `plt_silver.silver_usage_txn` (normalized tags + identity)

## Views Created
- `plt_gold.vw_cost_by_business_unit`
- `plt_gold.vw_cost_by_environment`
- `plt_gold.vw_pipeline_cost_by_business`
- `plt_gold.vw_cluster_cost_by_business`
- `plt_gold.vw_cost_by_actor_type`
- `plt_gold.vw_tag_coverage`

## Notes
- Views aggregate cost from Silver; they do not introduce duplication.
- Prefer `silver_usage_txn` for all cost groupings; use `silver_usage_tags_exploded` only for filtering via semi-joins.

## Example Queries
```sql
-- Top costly pipelines yesterday
SELECT *
FROM plt_gold.vw_pipeline_cost_by_business
WHERE date_sk = date_format(current_date() - INTERVAL 1 DAY, 'yyyyMMdd')
ORDER BY pipeline_cost_usd DESC
LIMIT 50;

-- Cost split by environment
SELECT date_sk, environment, SUM(environment_cost_usd) AS cost
FROM plt_gold.vw_cost_by_environment
GROUP BY date_sk, environment
ORDER BY date_sk DESC, cost DESC;

-- Service Principal vs User costs (weekly)
SELECT date_sk, run_actor_type, SUM(total_cost_usd) AS cost
FROM plt_gold.vw_cost_by_actor_type
WHERE date_sk >= date_format(current_date() - INTERVAL 7 DAY, 'yyyyMMdd')
GROUP BY date_sk, run_actor_type
ORDER BY date_sk DESC;
```
