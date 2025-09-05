# SCD2 Temporal Join Example

## Problem Statement

When a job is updated (e.g., name change from "Process Customer Data" to "Process Customer Data v2"), we need to ensure that:
- **Historical facts** (before the update) → Associate with **old dimension record**
- **New facts** (after the update) → Associate with **new dimension record**

## Example Scenario

### Job Evolution Timeline:
- **2025-06-01**: Job created with name "Process Customer Data" (Job ID: 1123453)
- **2025-08-25**: Job updated with name "Process Customer Data v2"

### Dimension Table (SCD2):
```sql
-- gld_dim_entity
entity_key | workspace_id | entity_id | name                    | valid_from  | valid_to    | is_current
1         | ws-123       | 1123453   | Process Customer Data   | 2025-06-01  | 2025-08-25  | false
2         | ws-123       | 1123453   | Process Customer Data v2| 2025-08-25  | null        | true
```

### Fact Data:
```sql
-- Usage facts from different time periods
usage_start_time | workspace_id | entity_id | usage_quantity | list_cost_usd
2025-07-15      | ws-123       | 1123453   | 100.5          | 15.75
2025-08-20      | ws-123       | 1123453   | 75.2           | 11.28
2025-09-01      | ws-123       | 1123453   | 120.8          | 18.12
2025-09-15      | ws-123       | 1123453   | 95.3           | 14.30
```

## SCD2 Temporal Join Logic

### Before Fix (Incorrect):
```python
# This would incorrectly join all facts to the current dimension record
.join(entity_dim, 
      (silver_df.workspace_id == entity_dim.workspace_id) & 
      (silver_df.entity_id == entity_dim.entity_id), 
      "left")
```

**Result**: All facts would get `entity_key = 2` (current version), losing historical context.

### After Fix (Correct SCD2):
```python
# This correctly joins facts to the appropriate dimension version based on time
.join(entity_dim, 
      (silver_df.workspace_id == entity_dim.workspace_id) & 
      (silver_df.entity_id == entity_dim.entity_id) &
      # SCD2 temporal condition: fact date must be within dimension validity period
      (F.to_date(silver_df.usage_start_time) >= entity_dim.valid_from) &
      ((entity_dim.valid_to.isNull()) | (F.to_date(silver_df.usage_start_time) < entity_dim.valid_to)), 
      "left")
```

## Expected Results

### Correct Fact-Dimension Association:
```sql
-- Final fact table with correct entity_key associations
date_key | workspace_key | entity_key | usage_quantity | list_cost_usd | job_name
20250715 | 1            | 1          | 100.5          | 15.75         | Process Customer Data
20250820 | 1            | 1          | 75.2           | 11.28         | Process Customer Data
20250901 | 1            | 2          | 120.8          | 18.12         | Process Customer Data v2
20250915 | 1            | 2          | 95.3           | 14.30         | Process Customer Data v2
```

## Query Examples

### Historical Analysis:
```sql
-- Get cost by job name over time (shows the name change impact)
SELECT 
    e.name as job_name,
    f.date_key,
    SUM(f.list_cost_usd) as total_cost
FROM gld_fact_usage_priced_day f
JOIN gld_dim_entity e ON f.entity_key = e.entity_key
WHERE f.workspace_key = 1
GROUP BY e.name, f.date_key
ORDER BY f.date_key;

-- Result:
job_name                  | date_key | total_cost
Process Customer Data     | 20250715 | 15.75
Process Customer Data     | 20250820 | 11.28
Process Customer Data v2  | 20250901 | 18.12
Process Customer Data v2  | 20250915 | 14.30
```

### Current State Analysis:
```sql
-- Get current job names and their total costs
SELECT 
    e.name as current_job_name,
    SUM(f.list_cost_usd) as total_cost
FROM gld_fact_usage_priced_day f
JOIN gld_dim_entity e ON f.entity_key = e.entity_key
WHERE e.is_current = true
GROUP BY e.name;

-- Result:
current_job_name      | total_cost
Process Customer Data v2 | 32.42
```

## Key Benefits

1. **Historical Accuracy**: Facts are correctly associated with the dimension version that was valid at the time
2. **Temporal Queries**: Can analyze data as it was at any point in time
3. **Change Tracking**: Can see the impact of dimension changes on metrics
4. **Data Integrity**: Ensures referential integrity across time periods
5. **Audit Trail**: Complete history of dimension changes and their effects

## Implementation Notes

- **Performance**: Temporal joins are more expensive than simple joins, but necessary for accuracy
- **Indexing**: Consider indexing on `valid_from` and `valid_to` columns for better performance
- **Data Quality**: Ensure dimension updates are properly timestamped
- **Testing**: Validate that facts are correctly associated with dimension versions
