CREATE CATALOG IF NOT EXISTS platform_observability;

CREATE SCHEMA IF NOT EXISTS platform_observability.plt_bronze COMMENT 'Raw copies of Databricks system tables with CDF enabled';
CREATE SCHEMA IF NOT EXISTS platform_observability.plt_silver COMMENT 'Curated model (SCD, pricing, tags) built incrementally via CDF';
CREATE SCHEMA IF NOT EXISTS platform_observability.plt_gold   COMMENT 'Star schema for BI and dashboards';
