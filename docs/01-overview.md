# Platform Observability - Solution Overview

## Table of Contents
- [1. Objective](#1-objective)
- [2. Scope & Assumptions](#2-scope--assumptions)
- [3. Solution & Design](#3-solution--design)
- [4. Data Flow](#4-data-flow)
- [5. How It Works](#5-how-it-works)
- [6. Usage & Personas](#6-usage--personas)
- [7. Related Documentation](#7-related-documentation)

## 1. Objective

Provide a reliable, low-cost observability layer on Databricks that unifies **usage, cost, and run health** across Jobs, DLT Pipelines, clusters and node types. The solution enables the platform team, FinOps, and data engineers to:

- Identify **top cost drivers** and reduce waste
- Track **run health & failures** and their cost impact
- Detect **task latency regressions** over time
- Quantify **serverless adoption**, **SKU mix**, and **node-type efficiency**
- Enforce **policy compliance** and measure **tag coverage** for showback/chargeback

**Business outcomes:** predictable spend, faster mean time to detect issues, capacity right‑sizing, and transparent chargeback.

## 2. Scope & Assumptions

**Sources (Databricks system catalog):**
- Billing: `system.billing.usage`, `system.billing.list_prices`
- Run timelines: `system.lakeflow.job_run_timeline`, `system.lakeflow.job_task_run_timeline`
- Metadata: `system.lakeflow.jobs`, `system.lakeflow.pipelines`, `system.access.workspaces_latest`
- Compute: `system.compute.clusters`, `system.compute.node_types`

**Design assumptions:**
- **Medallion** architecture with **Delta Change Data Feed (CDF)** driving Silver/Gold increments
- Catalog & schemas: `platform_observability.plt_bronze`, `plt_silver`, `plt_gold`
- **date_key** is the canonical time key (YYYYMMDD)
- Daily schedule **05:30 Asia/Kolkata**; late arrivals handled by CDF + small overlap where needed
- "Cost" = **list price**; can be extended to actual/discounted pricing later

## 3. Solution & Design

**Bronze (Raw + CDF)**
- 1:1 copies of system tables with **CDF enabled**
- No reshaping beyond standardization and light expectations

**Silver (Curated)**
- Conformed entities (Jobs, Pipelines & Clusters) with SCD2
- Pricing join (effective interval) to compute **list_cost_usd**
- **Tags exploded** to key/value rows
- Run & task timelines normalized with **date_key** and durations
- Optional cluster SCD and node‑type lookup for hardware analytics

**Gold (Star Schema)**
- Dimensions: **date**, **workspace**, **entity (job/pipeline)**, **cluster**, **SKU**, **run status**, **node type**
- Facts: **usage_priced_day**, **entity_cost**, **run_cost**, **run_status_cost**, **runs_finished_day**, **usage_by_node_type_day**
- Views: **policy compliance**, **required tag coverage**, **task latency trend/anomaly**

**Incremental engine (CDF‑driven):**
- Silver reads **only changed rows** from Bronze since the last bookmark
- Gold recalculates and **MERGEs only impacted `date_key`** partitions

**Quality & governance:**
- Expectations (non‑negative qty, valid durations, price coverage)
- Quarantine for malformed rows; metrics/alerts on coverage
- Lineage is explicit: Gold → Silver → Bronze → System tables

## 4. Data Flow

```
System Tables (billing, lakeflow, access, compute)
        │
        ▼  (HWM Job — Bronze, CDF enabled)
platform_observability.plt_bronze.*  ───────────────►  Bronze with CDF
        │                                  ▲
        │                                  │ CDF versions (bookmarks)
        ▼
(HWM Jobs — Silver/Gold, CDF read)
        │
        ├─► Silver: entity SCD2, pricing, tags, timelines, cluster/node types
        │
        └─► Gold: dims & facts (MERGE only impacted date_key)
                 ├─ dim_date, dim_workspace, dim_entity, dim_cluster, dim_sku, dim_run_status, dim_node_type
                 ├─ gld_fact_usage_priced_day, fact_entity_cost, fact_run_cost
                 ├─ fact_run_status_cost, fact_runs_finished_day, fact_usage_by_node_type_day
                 └─ views: policy compliance, tag coverage, latency trend/anomaly
```

## 5. How It Works

**Run sequence:**
1. **Bronze ingest (HWM Job)** pulls the latest snapshots from system tables and keeps **CDF** on for each table
2. **Silver build (HWM Job)** reads **only CDF changes** since the last successful version per source (bookmarks), derives entity/run fields, joins pricing, explodes tags, intervalizes SCDs, and standardizes **date_key**
3. **Gold materialization (HWM Job)** computes aggregates only for **impacted dates** and performs **idempotent MERGEs** into fact/dim tables
4. **Compliance & trend views** refresh off Gold/Silver without full rebuilds
5. **Bookmarks** are updated at the end of a successful run for next‑day increments

**Scheduling:** one orchestrated Job runs **Bronze → Silver/Gold** daily at **05:30 IST**. Retry on failure; optional ad‑hoc backfills by resetting bookmarks or toggling rebuild.

## 6. Usage & Personas

**For Platform Engineering**
- Monitor **Top N jobs/pipelines by cost** per day/week/month
- Track **failed or cancelled runs** and quantify their **cost impact**
- Review **policy compliance**: missing cluster policy, disallowed security modes, deprecated node types

**For Data Engineering Teams**
- Use **task latency trend** and **anomaly views** to detect regressions (p50/p90/p99)
- Correlate **throughput vs. latency**: rising run counts with slowing tasks

**For FinOps**
- Track **cost allocation** by workspace, department, project
- Monitor **cost trends** and identify anomalies
- Generate **chargeback reports** for business units

## 7. Related Documentation

- [02-getting-started.md](02-getting-started.md) - Step-by-step deployment guide
- [03-parameterization.md](03-parameterization.md) - Configuration and SQL management
- [04-data-dictionary.md](04-data-dictionary.md) - Complete data model documentation
- [11-deployment.md](11-deployment.md) - Production deployment and workflow setup
