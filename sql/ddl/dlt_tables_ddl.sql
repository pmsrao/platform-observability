-- DDL for DLT tables created by simple_dlt_pipeline
-- These tables will be created in platform_observability.default schema

-- =============================================
-- Table: current_prices
-- =============================================
CREATE TABLE IF NOT EXISTS platform_observability.default.current_prices (
    sku_name STRING NOT NULL,
    cloud STRING NOT NULL,
    currency_code STRING NOT NULL,
    usage_unit STRING NOT NULL,
    price_usd DECIMAL(38,18) NOT NULL,
    price_start_time TIMESTAMP NOT NULL,
    price_end_time TIMESTAMP NOT NULL,
    _processed_at TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'silver',
    'pipeline.autoOptimize.managed' = 'true'
)
COMMENT 'Current pricing data extracted from billing list prices';

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_current_prices_sku_cloud 
ON platform_observability.default.current_prices (sku_name, cloud);

CREATE INDEX IF NOT EXISTS idx_current_prices_processed_at 
ON platform_observability.default.current_prices (_processed_at);

-- =============================================
-- View: pricing_summary (will be created as view by DLT)
-- =============================================
-- Note: This is a view, so no DDL needed, but here's the equivalent table structure
-- if you wanted to materialize it as a table instead

CREATE TABLE IF NOT EXISTS platform_observability.default.pricing_summary_table (
    sku_name STRING NOT NULL,
    cloud STRING NOT NULL,
    currency_code STRING NOT NULL,
    price_count BIGINT NOT NULL,
    min_price DECIMAL(38,18) NOT NULL,
    max_price DECIMAL(38,18) NOT NULL,
    avg_price DECIMAL(38,18) NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Summary view of pricing data by SKU and cloud (materialized version)';

-- Create indexes for the summary table
CREATE INDEX IF NOT EXISTS idx_pricing_summary_sku_cloud 
ON platform_observability.default.pricing_summary_table (sku_name, cloud);

-- =============================================
-- Sample queries to validate the DLT pipeline
-- =============================================

-- Query 1: Check current prices
-- SELECT * FROM platform_observability.default.current_prices 
-- ORDER BY sku_name, cloud, _processed_at DESC;

-- Query 2: Check pricing summary
-- SELECT * FROM platform_observability.default.pricing_summary 
-- ORDER BY sku_name, cloud;

-- Query 3: Count records in each table
-- SELECT 'current_prices' as table_name, count(*) as record_count 
-- FROM platform_observability.default.current_prices
-- UNION ALL
-- SELECT 'pricing_summary', count(*) 
-- FROM platform_observability.default.pricing_summary;

-- Query 4: Check data freshness
-- SELECT 
--   max(_processed_at) as latest_processing_time,
--   count(*) as total_current_prices
-- FROM platform_observability.default.current_prices;
