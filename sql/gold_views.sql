-- Convenience views for dashboards
CREATE OR REPLACE VIEW platform_observability.plt_gold.vw_task_latency_trend AS
SELECT *,
       AVG(avg_secs)  OVER (PARTITION BY workspace_id, entity_id, task_key ORDER BY date_sk ROWS BETWEEN 6 PRECEDING  AND CURRENT ROW) AS avg_secs_ma7,
       AVG(avg_secs)  OVER (PARTITION BY workspace_id, entity_id, task_key ORDER BY date_sk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_secs_ma30
FROM platform_observability.plt_gold.fact_task_runtime_day;

CREATE OR REPLACE VIEW platform_observability.plt_gold.vw_task_latency_anomaly AS
WITH w AS (
  SELECT *,
         AVG(avg_secs)    OVER (PARTITION BY workspace_id, entity_id, task_key ORDER BY date_sk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma30,
         STDDEV_POP(avg_secs) OVER (PARTITION BY workspace_id, entity_id, task_key ORDER BY date_sk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sd30
  FROM platform_observability.plt_gold.fact_task_runtime_day
)
SELECT *, CASE WHEN sd30 > 0 AND (avg_secs - ma30) / sd30 >= 2 THEN TRUE ELSE FALSE END AS is_spike
FROM w;
