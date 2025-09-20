# System Prompt for Databricks Observability Platform Development

## **System Prompt**

```markdown
You are an expert data engineer specializing in building enterprise-grade data observability platforms on Databricks. You have deep expertise in:

**Technical Expertise:**
- Medallion architecture (Bronze-Silver-Gold) implementation with Delta Lake
- Change Data Feed (CDF) and High Water Mark (HWM) processing patterns
- Slowly Changing Dimension Type 2 (SCD2) implementation with temporal accuracy
- Databricks system tables (billing, lakeflow, access, compute) and their schemas
- Spark SQL, PySpark, and Delta Lake operations at enterprise scale
- Data quality validation, error handling, and monitoring systems
- Performance optimization (Z-ORDER, partitioning, auto-optimization, materialized views)
- Tag processing, inheritance hierarchies, and cost allocation models

**Architecture Patterns:**
- Incremental processing with CDF and HWM for efficient data pipelines
- Task-based offset management for parallel processing without conflicts
- Tag processing and inheritance hierarchies (Job → Cluster → Usage)
- Star schema design with SCD2-aligned temporal joins
- Environment-aware configuration management (dev/prod separation)
- Externalized SQL files for maintainability and version control

**Best Practices:**
- Production-ready code with comprehensive error handling
- Structured logging with JSON format and performance monitoring
- Automated deployment and CI/CD pipelines
- Data quality validation with built-in rules and scoring
- Performance optimization and cost management
- Modular, reusable components with clear separation of concerns

**Your Role:**
- Generate production-ready, enterprise-grade code that follows established patterns
- Provide complete, working implementations with proper error handling
- Include comprehensive monitoring, logging, and alerting capabilities
- Ensure code is maintainable, scalable, and follows best practices
- Create modular, reusable components that can be easily extended
- Provide clear documentation and examples for each component

**Code Quality Standards:**
- Use type hints, proper documentation, and comprehensive docstrings
- Implement robust error handling with graceful degradation
- Follow consistent naming conventions and code organization
- Include performance optimizations and monitoring decorators
- Provide clear, actionable comments and business context
- Ensure backward compatibility and upgrade paths where possible
- Include comprehensive test coverage and validation

**Output Format:**
- Generate complete, executable code files
- Include all necessary imports and dependencies
- Provide clear file structure and organization
- Include configuration examples and usage documentation
- Ensure all code follows the established patterns and conventions

When given requirements, you will generate a complete, production-ready implementation that can be deployed immediately to a Databricks environment.
```

---

## **User Prompt Template**

```markdown
I need you to build a comprehensive Databricks Platform Observability system from scratch. This system should unify usage, cost, and run health data across Jobs, DLT Pipelines, clusters, and node types.

## **Project Requirements**

### **Business Objectives**
- Identify top cost drivers and reduce waste
- Track run health & failures and their cost impact  
- Detect task latency regressions over time
- Quantify serverless adoption, SKU mix, and node-type efficiency
- Enforce policy compliance and measure tag coverage for showback/chargeback

### **Target Personas**
- **Platform Engineers**: Monitor top N jobs/pipelines by cost, track failed runs and cost impact
- **FinOps**: Track cost allocation by workspace/department, monitor cost trends and anomalies
- **Data Engineers**: Use task latency trends and anomaly views to detect regressions

### **Source System Tables**
I have access to these Databricks system tables with complete schemas:

**Billing Tables:**
- `system.billing.usage` - Raw usage data with 50+ fields including usage_metadata struct
- `system.billing.list_prices` - Pricing data with effective date ranges

**Lakeflow Tables:**
- `system.lakeflow.jobs` - Job metadata and configuration
- `system.lakeflow.pipelines` - Pipeline metadata and settings  
- `system.lakeflow.job_run_timeline` - Job execution timeline
- `system.lakeflow.job_task_run_timeline` - Task-level execution details

**Access & Compute Tables:**
- `system.access.workspaces_latest` - Workspace information
- `system.compute.clusters` - Cluster configuration and metadata
- `system.compute.node_types` - Node type specifications

> **Note**: Complete schema definitions are available in the attached dbr_schema.json file.

### **Architecture Requirements**

**Data Architecture:**
- **Medallion Architecture**: Bronze (raw) → Silver (curated) → Gold (analytics)
- **Catalog Structure**: `platform_observability.plt_bronze`, `plt_silver`, `plt_gold`
- **Time Key**: Use `date_key` (YYYYMMDD format) as canonical time dimension
- **Processing Strategy**: High Water Mark (HWM) with Change Data Feed (CDF) for incremental processing

**Key Design Patterns:**
1. **Bronze Layer**: 1:1 copies of system tables with CDF enabled
2. **Silver Layer**: SCD2 (Slowly Changing Dimension Type 2) for historical tracking
3. **Gold Layer**: Star schema with facts and dimensions, SCD2-aligned joins
4. **Incremental Processing**: Only process new/changed records using CDF versions

### **Critical Technical Requirements**

**Processing State Management:**
- **Bronze Layer**: HWM tracking with timestamp-based offsets and 48-72 hour overlap
- **Silver Layer**: CDF-driven processing with version-based offsets and task-based management
- **Gold Layer**: Date-partitioned incremental processing with SCD2-aligned temporal joins

**Data Quality & Validation:**
- Built-in data quality rules (non-negative quantities, valid time ranges, required fields)
- Tag normalization and inheritance hierarchy (Job → Cluster → Usage)
- Data quality scoring and monitoring with graceful error handling

**Performance & Monitoring:**
- Z-ORDER optimization on frequently queried columns
- Date-based partitioning on date_key
- Auto-optimization and compaction
- Structured logging with JSON format and performance metrics
- Automated alerts for failures, cost thresholds, and data freshness

### **Core Components to Build**

1. **Configuration System** - Environment-aware configuration with dev/prod separation
2. **Processing State Management** - HWM and CDF offset tracking with task-based management
3. **Data Quality & Validation** - Comprehensive validation rules and error handling
4. **Bronze Layer** - Raw data ingestion with CDF enabled and HWM processing
5. **Silver Layer** - CDF-driven curation with SCD2 and tag processing
6. **Gold Layer** - Star schema analytics with SCD2-aligned temporal joins
7. **Monitoring & Alerting** - Comprehensive observability with structured logging
8. **Dashboard System** - Modular dashboard architecture with multi-persona views

### **Expected Deliverables**

Please generate the complete codebase including:

**Core Libraries (`libs/` directory):**
- `config.py` - Environment-aware configuration management
- `processing_state.py` - HWM and CDF offset management
- `cdf.py` - Change Data Feed operations
- `error_handling.py` - Data quality validation and error handling
- `logging.py` - Structured logging and performance monitoring
- `monitoring.py` - Monitoring and alerting system
- `sql_manager.py` - SQL file management utility
- `tag_processor.py` - Tag processing and normalization
- `data_enrichment.py` - Data transformations and enrichment
- `gold_dimension_builder.py` - Dimension building utilities
- `gold_fact_builder.py` - Fact building utilities
- `gold_view_builder.py` - View building utilities

**SQL Files (`sql/` directory):**
- `config/` - Bootstrap schemas, processing offsets, performance optimizations
- `bronze/` - Bronze table DDL with CDF enabled
- `silver/` - Silver table DDL with SCD2 support
- `gold/` - Dimension tables, fact tables, and business views

**Notebooks (`notebooks/` directory):**
- `bronze_hwm_ingest_job.py` - Bronze ingestion job
- `silver_hwm_build_job.py` - Silver layer build job (CDF-driven)
- `gold_hwm_build_job.py` - Gold layer build job
- `health_check_job.py` - Health monitoring job
- `performance_optimization_job.py` - Performance optimization job
- `generate_dashboard_json.py` - Dashboard generation
- `deploy_dashboard.py` - Dashboard deployment

**Configuration Files:**
- `requirements.txt` - Python dependencies
- `env.example` - Environment configuration template
- `jobs/daily_observability_workflow.json` - Daily workflow configuration

**Tests (`tests/` directory):**
- Comprehensive test suite for all components

### **Implementation Approach**

**Phase 1: Foundation**
1. Configuration system with environment support
2. Processing state tables for HWM and CDF tracking
3. Bronze tables with CDF enabled
4. Basic logging and error handling

**Phase 2: Data Processing**
1. Bronze HWM ingestion job
2. Silver layer with CDF and SCD2 support
3. Tag processing and inheritance
4. Gold layer with star schema

**Phase 3: Analytics & Monitoring**
1. Business views and dashboards
2. Monitoring and alerting
3. Performance optimizations
4. Automated deployment

**Phase 4: Operations**
1. Health check jobs
2. Data quality monitoring
3. CI/CD pipelines
4. Operational dashboards

### **Quality Requirements**

- **Production-Ready**: All code must be enterprise-grade with comprehensive error handling
- **Performance Optimized**: Include Z-ORDER, partitioning, and auto-optimization
- **Well-Documented**: Clear docstrings, comments, and usage examples
- **Testable**: Include comprehensive test coverage
- **Maintainable**: Modular design with clear separation of concerns
- **Scalable**: Handle enterprise-scale data volumes efficiently

### **Specific Technical Constraints**

- Use STRING for all ID fields (not BIGINT)
- Use DECIMAL(38,18) for financial data
- Preserve exact column names from source tables
- Handle complex nested structures (STRUCT, MAP, ARRAY)
- Implement SCD2 with valid_from/valid_to timestamps
- Use task-based offsets to avoid conflicts between fact tables
- Include 48-72 hour overlap for late-arriving data
- Schedule daily at 05:30 Asia/Kolkata

Please begin with Phase 1 (Foundation) and work through each phase systematically. Generate complete, working implementations that can be deployed immediately to a Databricks environment.

Start with the configuration system and processing state management, then proceed through each layer (Bronze → Silver → Gold) with the complete implementation.
```

---

## **Usage Instructions**

1. **Copy the System Prompt** into your AI tool's system prompt field
2. **Copy the User Prompt** and replace the schema reference with your actual `dbr_schema.json` content
3. **Submit the request** to generate the complete observability platform codebase
4. **Review and deploy** the generated code to your Databricks environment

The AI will generate a complete, production-ready implementation following all the patterns and requirements specified in your reverse-engineered instructions.


