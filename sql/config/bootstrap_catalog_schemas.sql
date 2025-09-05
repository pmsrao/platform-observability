-- Bootstrap Catalog and Schemas
-- This file creates the initial catalog and schemas for the platform observability system

-- Create catalog if it doesn't exist
CREATE CATALOG IF NOT EXISTS {catalog} COMMENT 'Platform Observability - Centralized monitoring and cost tracking';

-- Create Bronze schema for raw data ingestion
CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema} COMMENT 'Raw data ingestion with CDF enabled';

-- Create Silver schema for curated data
CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema} COMMENT 'Curated model (SCD, pricing, tags) built incrementally via CDF';

-- Create Gold schema for dimensional models
CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema} COMMENT 'Star schema for BI and dashboards';
