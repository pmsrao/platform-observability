# 📚 Databricks Dashboard Development Learnings

## 🚨 **CRITICAL LEARNINGS - MUST FOLLOW**

### 1. **SQL Query Spacing (CRITICAL)**
**Problem**: SQL queries in `queryLines` arrays get concatenated without spaces, causing syntax errors.

**Solution**: 
- Each SQL statement MUST be on its own line in `queryLines`
- Add proper spacing after SQL keywords
- Use proper line breaks between statements

**Example**:
```json
// ❌ WRONG - causes syntax errors
"queryLines": [
  "SELECT COUNT(*)FROM tableWHERE date > '2024-01-01'"
]

// ✅ CORRECT - proper spacing and line breaks
"queryLines": [
  "SELECT COUNT(*)",
  "FROM table",
  "WHERE date > '2024-01-01'"
]
```

**SQL Keyword Spacing Rules**:
- `SELECT` → `SELECT `
- `FROM` → ` FROM `
- `WHERE` → ` WHERE `
- `AND` → ` AND `
- `OR` → ` OR `
- `JOIN` → ` JOIN `
- `ON` → ` ON `
- `UNION ALL` → ` UNION ALL `
- `ORDER BY` → ` ORDER BY `
- `GROUP BY` → ` GROUP BY `
- `LIMIT` → ` LIMIT `

### 2. **LakeFlow Dashboard Structure (CRITICAL)**
**Must Use Exact LakeFlow Pattern**:

#### **Datasets**:
```json
{
  "datasets": [
    {
      "name": "dataset_name",
      "displayName": "Display Name",
      "queryLines": ["SELECT ...", "FROM ...", "WHERE ..."],
      "parameters": [...]
    }
  ]
}
```

#### **Widget Queries**:
```json
{
  "queries": [
    {
      "name": "main_query",
      "query": {
        "datasetName": "dataset_name",
        "fields": [
          {
            "name": "field_name",
            "expression": "`field_name`"
          }
        ],
        "disaggregated": false
      }
    }
  ]
}
```

#### **Widget Types & Versions**:
- **Tables**: `"version": 1`, `"widgetType": "table"`, `"encodings": {"columns": [...]}`
- **Charts**: `"version": 3`, `"widgetType": "line/pie/bar"`, `"encodings": {"x": {...}, "y": {...}}`
- **Filters**: `"version": 2`, `"widgetType": "filter-date-picker/filter-single-select"`, `"encodings": {"fields": [...]}`

### 3. **Column Name Mapping (CRITICAL)**
**Problem**: Using wrong column names causes "UNRESOLVED_COLUMN" errors.

**Solution**:
- Fact tables use surrogate keys (`workspace_key`, `entity_key`, `cluster_key`)
- Must join with dimension tables to get business keys (`workspace_id`, `entity_id`, `cluster_id`)
- Use proper field expressions with backticks: `"expression": "`field_name`"`

**Example**:
```sql
-- ❌ WRONG - workspace_id doesn't exist in fact table
SELECT workspace_id FROM platform_observability.plt_gold.gld_fact_billing_usage

-- ✅ CORRECT - join with dimension table
SELECT w.workspace_id 
FROM platform_observability.plt_gold.gld_fact_billing_usage f
JOIN platform_observability.plt_gold.gld_dim_workspace w ON f.workspace_key = w.workspace_key
```

### 4. **Parameter Binding (CRITICAL)**
**Filters need both main queries and parameter queries**:

#### **Date Filters**:
```json
{
  "encodings": {
    "fields": [
      {
        "parameterName": "param_start_date",
        "queryName": "parameter_query_name"
      }
    ]
  }
}
```

#### **Workspace Filter**:
```json
{
  "queries": [
    {
      "name": "workspace_main_query",
      "query": {
        "datasetName": "workspace_dataset",
        "fields": [
          {
            "name": "workspace_id",
            "expression": "`workspace_id`"
          },
          {
            "name": "workspace_id_associativity",
            "expression": "COUNT_IF(`associative_filter_predicate_group`)"
          }
        ]
      }
    }
  ],
  "encodings": {
    "fields": [
      {
        "fieldName": "workspace_id",
        "displayName": "workspace_id",
        "queryName": "workspace_main_query"
      },
      {
        "parameterName": "param_workspace",
        "queryName": "parameter_query_name"
      }
    ]
  }
}
```

### 5. **Placeholder Replacement (CRITICAL)**
**Always replace placeholders with actual values**:
- `{catalog}` → `platform_observability`
- `{gold_schema}` → `plt_gold`

### 6. **Default Date Values (CRITICAL)**
**Use current date ranges, not 2024**:
```json
"defaultSelection": {
  "values": {
    "dataType": "DATE",
    "values": [{"value": "2025-08-22T00:00:00.000"}]
  }
}
```

## 🔧 **Validation Checklist**

Before importing any dashboard, verify:

1. ✅ **SQL Spacing**: All `queryLines` have proper spacing and line breaks
2. ✅ **Column Names**: All column references exist in the tables
3. ✅ **Widget Structure**: Correct versions and encodings for each widget type
4. ✅ **Parameter Binding**: All filters have proper parameter queries
5. ✅ **Placeholder Replacement**: All `{catalog}` and `{gold_schema}` replaced
6. ✅ **Date Ranges**: Default dates are current, not 2024

## 📋 **File Naming Convention**
- Use `dbv4_` prefix for new dashboards
- Keep one working version: `dbv4_final_working.lvdash.json`
- Remove old/duplicate files after successful import

## 🎯 **Success Criteria**
A dashboard is ready when:
- ✅ Imports without validation errors
- ✅ Filters show actual data (not "ALL WORKSPACES")
- ✅ Data widgets display data (not "No data")
- ✅ No "invalid widget definition" errors
- ✅ No SQL syntax errors

## 🚨 **ADDITIONAL CRITICAL LEARNINGS**

### 7. **UNION ALL Spacing (CRITICAL)**
**Problem**: `UNION ALL` needs space after it to prevent concatenation issues.

**Solution**: 
```json
// ❌ WRONG - causes UNIONALLSELECT
"queryLines": [
  "UNION ALL",
  "SELECT '<ALL_WORKSPACES>' as workspace_id"
]

// ✅ CORRECT - proper spacing
"queryLines": [
  "UNION ALL ",
  "SELECT '<ALL_WORKSPACES>' as workspace_id"
]
```

### 8. **Date Values (CRITICAL)**
**Problem**: Never use 2024 dates - always use current date ranges.

**Solution**:
- **Start Date**: 30 days ago from current date
- **End Date**: Current date
- **Format**: `YYYY-MM-DDTHH:MM:SS.000`

**Example**:
```python
from datetime import datetime, timedelta
current_date = datetime.now()
start_date = (current_date - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00.000")
end_date = current_date.strftime("%Y-%m-%dT00:00:00.000")
```

### 9. **Column Validation (CRITICAL)**
**Problem**: Don't include columns that don't exist in the source tables.

**Solution**: 
- Always validate column names against actual table schemas
- Remove non-existent columns from queries
- Example: `usage_start_time` doesn't exist in `v_cost_anomalies` - remove it

### 10. **Validation Process (CRITICAL)**
**Before declaring any dashboard "done":**
1. ✅ Run SQL concatenation validation
2. ✅ Check for 2024 date references
3. ✅ Validate all column names exist
4. ✅ Test UNION ALL spacing
5. ✅ Verify all placeholders are replaced

**Only declare success after ALL validations pass!**
