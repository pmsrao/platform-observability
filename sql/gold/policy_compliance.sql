-- Gold Layer Policy Compliance
-- This file contains compliance and governance views

-- Policy Baseline Table
CREATE TABLE IF NOT EXISTS {catalog}.{gold_schema}.policy_baseline (
    policy_id STRING,
    policy_name STRING,
    policy_description STRING,
    policy_type STRING,
    threshold_value DOUBLE,
    threshold_unit STRING,
    severity STRING,
    is_active BOOLEAN,
    created_at TIMESTAMP
) USING DELTA;

-- Insert default policies
INSERT INTO {catalog}.{gold_schema}.policy_baseline 
VALUES 
    ('POL_001', 'High Cost Alert', 'Alert when daily cost exceeds threshold', 'COST', 1000.0, 'USD', 'HIGH', true, CURRENT_TIMESTAMP()),
    ('POL_002', 'Long Running Jobs', 'Alert when jobs run longer than threshold', 'PERFORMANCE', 24.0, 'HOURS', 'MEDIUM', true, CURRENT_TIMESTAMP()),
    ('POL_003', 'Failed Job Rate', 'Alert when failure rate exceeds threshold', 'QUALITY', 0.1, 'PERCENTAGE', 'HIGH', true, CURRENT_TIMESTAMP());

-- Policy Compliance View
CREATE OR REPLACE VIEW {catalog}.{gold_schema}.v_policy_compliance AS
SELECT 
    p.policy_id,
    p.policy_name,
    p.policy_description,
    p.policy_type,
    p.threshold_value,
    p.threshold_unit,
    p.severity,
    CASE 
        WHEN p.policy_type = 'COST' THEN 
            CASE WHEN f.daily_cost > p.threshold_value THEN 'VIOLATION' ELSE 'COMPLIANT' END
--        WHEN p.policy_type = 'PERFORMANCE' THEN 
--            CASE WHEN f.avg_duration > p.threshold_value THEN 'VIOLATION' ELSE 'COMPLIANT' END
        WHEN p.policy_type = 'QUALITY' THEN 
            CASE WHEN f.failure_rate > p.threshold_value THEN 'VIOLATION' ELSE 'COMPLIANT' END
        ELSE 'UNKNOWN'
    END as compliance_status,
    f.daily_cost,
--    f.avg_duration,
    f.failure_rate,
    f.date_key
FROM {catalog}.{gold_schema}.policy_baseline p
CROSS JOIN (
    SELECT 
        f.date_key,
        SUM(f.list_cost_usd) as daily_cost,
--        AVG(f.duration_hours) as avg_duration,
        SUM(r.failed_runs) * 1.0 / NULLIF(SUM(r.finished_runs), 0) as failure_rate
    FROM {catalog}.{gold_schema}.gld_fact_usage_priced_day f
    LEFT JOIN {catalog}.{gold_schema}.gld_fact_runs_finished_day r 
        ON f.date_key = r.date_key AND f.workspace_key = r.workspace_key
    GROUP BY f.date_key
) f
WHERE p.is_active = true;
