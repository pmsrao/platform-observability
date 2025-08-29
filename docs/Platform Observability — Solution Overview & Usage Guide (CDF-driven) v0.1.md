# Platform Observability — Solution Overview & Usage Guide (CDF-driven) v0.1

## 1) Objective
Provide a reliable, low-cost observability layer on Databricks that unifies **usage, cost, and run health** across Jobs, DLT Pipelines, clusters and node types. The solution enables the platform team, FinOps, and data engineers to:
- Identify **top cost drivers** and reduce waste.
- Track **run health & failures** and their cost impact.
- Detect **task latency regressions** over time.
- Quantify **serverless adoption**, **SKU mix**, and **node-type efficiency**.
- Enforce **policy compliance** (cluster security mode, policy usage, required tags) and measure **tag coverage** for showback/chargeback.

**Business outcomes:** predictable spend, faster mean time to detect issues, capacity right‑sizing, and transparent chargeback.

---

## 2) Scope & Assumptions
**Sources (Databricks system catalog):**
- Billing: `system.billing.usage`, `system.billing.list_prices`
- Run timelines: `system.lakeflow.job_run_timeline`, `system.lakeflow.job_task_run_timeline`
- Metadata: `system.lakeflow.jobs`, `system.lakeflow.pipelines`, `system.access.workspaces_latest`
- Compute: `system.compute.clusters`, `system.compute.node_types`

**Design assumptions:**
- **Medallion** architecture with **Delta Change Data Feed (CDF)** driving Silver/Gold increments.
- Catalog & schemas: `platform_observability.plt_bronze`, `plt_silver`, `plt_gold`.
- **date_sk** is the canonical time key (YYYYMMDD).
- Daily schedule **05:30 Asia/Kolkata**; late arrivals handled by CDF + small overlap where needed.
- "Cost" = **list price**; can be extended to actual/discounted pricing later.

---

## 3) Solution & Design (high level)
**Bronze (Raw + CDF)**
- 1:1 copies of system tables with **CDF enabled**.
- No reshaping beyond standardization and light expectations.

**Silver (Curated)**
- Conformed entities (Jobs & Pipelines) with SCD2.
- Pricing join (effective interval) to compute **list_cost_usd**.
- **Tags exploded** to key/value rows.
- Run & task timelines normalized with **date_sk** and durations.
- Optional cluster SCD and node‑type lookup for hardware analytics.

**Gold (Star Schema)**
- Dimensions: **date**, **workspace**, **entity (job/pipeline)**, **SKU**, **run status**, **node type**.
- Facts: **usage_priced_day**, **entity_cost**, **run_cost**, **run_status_cost**, **runs_finished_day**, **usage_by_node_type_day**.
- Views: **policy compliance**, **required tag coverage**, **task latency trend/anomaly**.

**Incremental engine (CDF‑driven):**
- Silver reads **only changed rows** from Bronze since the last bookmark.
- Gold recalculates and **MERGEs only impacted `date_sk`** partitions.

**Quality & governance:**
- Expectations (non‑negative qty, valid durations, price coverage).
- Quarantine for malformed rows; metrics/alerts on coverage.
- Lineage is explicit: Gold → Silver → Bronze → System tables.

---

## 4) Data‑flow diagram (conceptual)
```
System Tables (billing, lakeflow, access, compute)
        │
        ▼  (DLT Pipeline A — Bronze, CDF enabled)
platform_observability.plt_bronze.*  ───────────────►  Bronze with CDF
        │                                  ▲
        │                                  │ CDF versions (bookmarks)
        ▼
(DLT Pipeline B — Silver/Gold, CDF read)
        │
        ├─► Silver: entity SCD2, pricing, tags, timelines, cluster/node types
        │
        └─► Gold: dims & facts (MERGE only impacted date_sk)
                 ├─ dim_date, dim_workspace, dim_entity, dim_sku, dim_run_status, dim_node_type
                 ├─ fact_usage_priced_day, fact_entity_cost, fact_run_cost
                 ├─ fact_run_status_cost, fact_runs_finished_day, fact_usage_by_node_type_day
                 └─ views: policy compliance, tag coverage, latency trend/anomaly
```

---

## 5) How it works (run sequence)
1. **Bronze ingest (DLT A)** pulls the latest snapshots from system tables and keeps **CDF** on for each table.
2. **Silver build (DLT B)** reads **only CDF changes** since the last successful version per source (bookmarks), derives entity/run fields, joins pricing, explodes tags, intervalizes SCDs, and standardizes **date_sk**.
3. **Gold materialization (DLT B)** computes aggregates only for **impacted dates** and performs **idempotent MERGEs** into fact/dim tables.
4. **Compliance & trend views** refresh off Gold/Silver without full rebuilds.
5. **Bookmarks** are updated at the end of a successful run for next‑day increments.

**Scheduling:** one orchestrated Job runs **Bronze → Silver/Gold** daily at **05:30 IST**. Retry on failure; optional ad‑hoc backfills by resetting bookmarks or toggling rebuild.

---

## 6) How to use the data (personas & recipes)
**For Platform Engineering**
- Monitor **Top N jobs/pipelines by cost** per day/week/month.
- Track **failed or cancelled runs** and quantify their **cost impact**.
- Review **policy compliance**: missing cluster policy, disallowed security modes, deprecated node types.

**For Data Engineering Teams**
- Use **task latency trend** and **anomaly views** to detect regressions (p50/p90/p99).
- Correlate **throughput vs. latency**: rising run counts with slowing tasks.
- Inspect **node‑type efficiency** to right‑size clusters or migrate to serverless.

**For FinOps / Budget Owners**
- Attribute spend by **workspace, entity, SKU, tag** (project, environment, cost_center).
- Compare **serverless vs non‑serverless** cost share and trend.
- Validate **required tag coverage** for showback/chargeback completeness.

**Access pattern (BI / Ad hoc):**
- Start from Gold **facts** and **dimensions**: filter by date range (`date_sk`), slice by workspace/entity/SKU, join to tags and node types as needed.
- Use the supplied **views** for quick answers (e.g., compliance, latency spikes) and drill down to Silver for root‑cause.

---

## 7) Insights the model provides (at a glance)
- **Top cost drivers**: entities/runs contributing most to spend; changes over time.
- **Cost of failures**: where unsuccessful runs consume significant DBUs.
- **Latency regressions**: tasks with rising p90/p99; week‑over‑week deltas.
- **Node‑type efficiency**: cost/usage by instance family/size; deprecated hardware still in use.
- **Serverless adoption**: share of cost/usage by serverless vs non‑serverless and the trajectory.
- **SKU & cloud mix**: which SKUs dominate cost and how that shifts.
- **Workspace benchmarking**: relative cost, success rates, and growth across workspaces.
- **Policy compliance & tag coverage**: which clusters/runs violate policy or lack mandatory tags.

---

## 8) Operational model & SLAs
- **Frequency**: daily at **05:30 IST** (tunable).
- **Freshness**: data available typically by **06:00 IST** depending on volumes.
- **Quality gates**: expectations on qty ≥ 0, duration ≥ 0, price coverage ≥ threshold; violations quarantined and alerted.
- **Performance**: partitioning by **date_sk**; clustering on hot slicers (workspace/entity/SKU); MERGE only impacted partitions.
- **Backfills**: reprocess by temporarily toggling rebuild or resetting bookmarks for selected sources.
- **Retention**: Bronze as per cost/need; Silver/Gold for analysis horizon (e.g., 13 months).

---

## 9) Table inventory (names & grains)
**Dimensions**
- `dim_date` — **day**
- `dim_workspace` — **workspace** (with URL)
- `dim_entity` — **job/pipeline** (current attributes)
- `dim_sku` — **SKU × cloud × unit**
- `dim_run_status` — **result_state × termination_code**
- `dim_node_type` — **compute family/size**

**Facts**
- `fact_usage_priced_day` — **date_sk × workspace × entity × SKU**
- `fact_entity_cost` — **date_sk × entity**
- `fact_run_cost` — **date_sk × run × SKU**
- `fact_run_status_cost` — **date_sk × run × status**
- `fact_runs_finished_day` — **date_sk × entity**
- `fact_usage_by_node_type_day` — **date_sk × entity × node_type**

**Views (examples)**
- `vw_cluster_policy_compliance_current`, `vw_required_tags_coverage_day`, `vw_required_tags_missing_day`
- `vw_task_latency_trend`, `vw_task_latency_anomaly`

---

## 10) Roadmap (optional)
- Add **actual cost** (after discounts/commitments) alongside list price.
- Build **alerting** for latency spikes, failure cost surges, and policy drift.
- Add **unit economics** (e.g., cost per successful run, cost per GB scanned by SKU).
- Extend to **near‑real‑time** with streaming inputs if required.
- Package a **semantic model** for BI tools (e.g., DBSQL/Power BI) with governed measures.
