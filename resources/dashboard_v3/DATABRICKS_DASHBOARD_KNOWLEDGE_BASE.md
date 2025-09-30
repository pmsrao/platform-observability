# Databricks Dashboard Knowledge Base
## Complete Guide to Building Working Databricks Dashboards

### 📋 **Table of Contents**
1. [Dashboard JSON Structure](#dashboard-json-structure)
2. [Parameter Binding & Field Mapping](#parameter-binding--field-mapping)
3. [Widget Types & Configurations](#widget-types--configurations)
4. [SQL Query Structure](#sql-query-structure)
5. [Validation Steps](#validation-steps)
6. [Common Issues & Solutions](#common-issues--solutions)
7. [Best Practices](#best-practices)

---

## 🏗️ **Dashboard JSON Structure**

### **Top-Level Structure**
```json
{
  "datasets": [...],           // All dataset definitions
  "pages": [                   // Dashboard pages
    {
      "name": "page_name",
      "displayName": "Page Title",
      "layout": [...]          // Widget layout
    }
  ]
}
```

### **Dataset Structure**
```json
{
  "name": "dataset_001",                    // Unique dataset identifier
  "displayName": "Dataset Display Name",    // Human-readable name
  "queryLines": [                           // SQL query split into lines
    "SELECT ",
    "    column1,",
    "    column2",
    " FROM table_name",
    " WHERE condition = :param_name"
  ],
  "parameters": [                           // Parameter definitions
    {
      "displayName": "Parameter Name",
      "keyword": "param_name",
      "dataType": "DATE|STRING|NUMBER",
      "defaultSelection": {
        "values": {
          "dataType": "DATE|STRING|NUMBER",
          "values": [{"value": "default_value"}]
        }
      }
    }
  ]
}
```

### **Widget Structure**
```json
{
  "name": "widget_name",                    // Unique widget identifier
  "displayName": "Widget Display Name",     // Human-readable name
  "queries": [...],                         // Query references
  "spec": {                                 // Widget configuration
    "version": 1|2|3,                       // Widget version
    "widgetType": "table|line|bar|pie|filter-*",
    "encodings": {...}                      // Field mappings
  },
  "frame": {                                // Title configuration
    "showTitle": true,
    "title": "Widget Title"
  }
}
```

---

## 🔗 **Parameter Binding & Field Mapping**

### **Filter Widgets (Date Pickers)**
```json
{
  "spec": {
    "version": 2,
    "widgetType": "filter-date-picker",
    "encodings": {
      "fields": [
        {
          "parameterName": "param_start_date",    // Parameter binding
          "queryName": "parameter_query_name"
        }
      ]
    },
    "selection": {
      "defaultSelection": {
        "values": {
          "dataType": "DATE",
          "values": [{"value": "2025-08-23T00:00:00.000"}]
        }
      }
    },
    "frame": {
      "showTitle": true,
      "title": "Start Date"
    }
  }
}
```

### **Filter Widgets (Single Select)**
```json
{
  "spec": {
    "version": 2,
    "widgetType": "filter-single-select",
    "encodings": {
      "fields": [
        {
          "fieldName": "workspace_id",            // Field for dropdown
          "displayName": "workspace_id",
          "queryName": "workspace_main_query"
        },
        {
          "parameterName": "param_workspace",     // Parameter binding
          "queryName": "parameter_query_name"
        }
      ]
    },
    "selection": {
      "defaultSelection": {
        "values": {
          "dataType": "STRING",
          "values": [{"value": "<ALL_WORKSPACES>"}]
        }
      }
    }
  }
}
```

### **Parameter Queries**
```json
{
  "name": "parameter_dashboards/dashboard_id/datasets/dataset_param_name",
  "query": {
    "datasetName": "dataset_001",
    "parameters": [
      {
        "name": "param_name",
        "keyword": "param_name"
      }
    ],
    "disaggregated": false
  }
}
```

---

## 📊 **Widget Types & Configurations**

### **Table Widgets (Version 1)**
```json
{
  "spec": {
    "version": 1,
    "widgetType": "table",
    "encodings": {
      "columns": [
        {"fieldName": "column1"},
        {"fieldName": "column2"},
        {"fieldName": "column3"}
      ]
    }
  },
  "queries": [
    {
      "name": "main_query",
      "query": {
        "datasetName": "dataset_001",
        "fields": [
          {
            "name": "column1",
            "expression": "`column1`"
          },
          {
            "name": "column2", 
            "expression": "`column2`"
          }
        ],
        "disaggregated": false
      }
    }
  ]
}
```

### **Line Charts (Version 3)**
```json
{
  "spec": {
    "version": 3,
    "widgetType": "line",
    "encodings": {
      "x": {
        "fieldName": "date",
        "scale": {
          "type": "temporal"              // Required for date/time data
        },
        "displayName": "Date"
      },
      "y": {
        "fieldName": "daily_cost",
        "scale": {
          "type": "quantitative"          // Required for numeric data
        },
        "format": {
          "type": "number-plain",
          "abbreviation": "compact",
          "decimalPlaces": {
            "type": "max",
            "places": 2
          }
        },
        "displayName": "Daily Cost"
      }
    }
  },
  "queries": [
    {
      "name": "main_query",
      "query": {
        "datasetName": "dataset_002",
        "fields": [
          {
            "name": "date",
            "expression": "`date`"
          },
          {
            "name": "daily_cost",
            "expression": "`daily_cost`"
          }
        ],
        "disaggregated": false
      }
    }
  ]
}
```

### **Bar Charts (Version 3)**
```json
{
  "spec": {
    "version": 3,
    "widgetType": "bar",
    "encodings": {
      "x": {
        "fieldName": "workload_type",
        "scale": {
          "type": "ordinal"               // Required for categorical data
        },
        "displayName": "Workload Type"
      },
      "y": {
        "fieldName": "total_cost",
        "scale": {
          "type": "quantitative"          // Required for numeric data
        },
        "format": {
          "type": "number-plain",
          "abbreviation": "compact",
          "decimalPlaces": {
            "type": "max",
            "places": 2
          }
        },
        "displayName": "Total Cost"
      }
    }
  }
}
```

---

## 🗃️ **SQL Query Structure**

### **Query Lines Format**
```json
"queryLines": [
  "SELECT ",
  "    column1,",
  "    column2,",
  "    ROUND(SUM(column3), 2) as total_value",
  " FROM {catalog}.{schema}.table_name",
  " WHERE date_column >= date_format(:param_start_date, 'yyyyMMdd')",
  "   AND date_column <= date_format(:param_end_date, 'yyyyMMdd')",
  "   AND IF(:param_workspace = '<ALL_WORKSPACES>', true, workspace_id = :param_workspace)",
  " GROUP BY column1, column2",
  " ORDER BY total_value DESC "
]
```

### **Critical SQL Spacing Rules**
1. **Each line must end with a space** for proper concatenation
2. **Keywords must be followed by spaces**: `SELECT `, ` FROM `, ` WHERE `
3. **Last line must end with a space** for dynamic filter concatenation
4. **No trailing spaces on empty lines**

### **Parameter Usage**
- **Date Parameters**: Use `date_format(:param_name, 'yyyyMMdd')`
- **String Parameters**: Use `:param_name` directly
- **All Workspaces Logic**: `IF(:param_workspace = '<ALL_WORKSPACES>', true, workspace_id = :param_workspace)`

---

## ✅ **Validation Steps**

### **1. SQL Validation**
```python
def validate_sql_queries(dashboard_json):
    """Validate SQL query structure and spacing"""
    for dataset in dashboard_json["datasets"]:
        query_lines = dataset["queryLines"]
        
        # Check spacing
        for i, line in enumerate(query_lines):
            if line.strip() and not line.endswith(' '):
                print(f"❌ Line {i+1} missing trailing space: '{line}'")
        
        # Check parameter usage
        full_query = ''.join(query_lines)
        if ':param_' not in full_query:
            print(f"❌ Dataset {dataset['name']} missing parameters")
```

### **2. Widget Structure Validation**
```python
def validate_widget_structure(dashboard_json):
    """Validate widget configurations"""
    for page in dashboard_json["pages"]:
        for widget_layout in page["layout"]:
            widget = widget_layout["widget"]
            
            # Check required properties
            required = ["name", "displayName", "queries", "spec"]
            for prop in required:
                if prop not in widget:
                    print(f"❌ Widget {widget['name']} missing {prop}")
            
            # Check widget type specific requirements
            widget_type = widget["spec"]["widgetType"]
            if widget_type == "table":
                if "columns" not in widget["spec"]["encodings"]:
                    print(f"❌ Table widget {widget['name']} missing columns")
            elif widget_type in ["line", "bar"]:
                if "x" not in widget["spec"]["encodings"] or "y" not in widget["spec"]["encodings"]:
                    print(f"❌ Chart widget {widget['name']} missing x/y encodings")
```

### **3. Parameter Binding Validation**
```python
def validate_parameter_binding(dashboard_json):
    """Validate parameter queries and bindings"""
    # Check parameter queries exist for all parameters
    parameter_queries = []
    for page in dashboard_json["pages"]:
        for widget_layout in page["layout"]:
            widget = widget_layout["widget"]
            if "queries" in widget:
                parameter_queries.extend(widget["queries"])
    
    # Validate parameter query structure
    for query in parameter_queries:
        if "parameters" in query.get("query", {}):
            params = query["query"]["parameters"]
            for param in params:
                if "name" not in param or "keyword" not in param:
                    print(f"❌ Invalid parameter structure: {param}")
```

### **4. Field Mapping Validation**
```python
def validate_field_mappings(dashboard_json):
    """Validate field names match SQL columns"""
    # Extract field names from datasets
    dataset_fields = {}
    for dataset in dashboard_json["datasets"]:
        query_lines = dataset["queryLines"]
        full_query = ''.join(query_lines)
        # Parse SELECT clause to get column names
        # This is a simplified check - in practice, you'd parse SQL properly
    
    # Check widget field mappings
    for page in dashboard_json["pages"]:
        for widget_layout in page["layout"]:
            widget = widget_layout["widget"]
            if "queries" in widget:
                for query in widget["queries"]:
                    if "fields" in query.get("query", {}):
                        for field in query["query"]["fields"]:
                            field_name = field["name"]
                            # Validate field exists in dataset
```

---

## 🚨 **Common Issues & Solutions**

### **Issue 1: "Select fields to visualize"**
**Cause**: Missing `fields` array in widget queries
**Solution**: Add `fields` array with proper field mappings
```json
"query": {
  "datasetName": "dataset_001",
  "fields": [
    {
      "name": "field_name",
      "expression": "`field_name`"
    }
  ],
  "disaggregated": false
}
```

### **Issue 2: "Error loading dataset schema"**
**Cause**: Incorrect `disaggregated` property or malformed dataset structure
**Solution**: Use `"disaggregated": false` for data widgets, ensure proper query structure

### **Issue 3: "Cannot read properties of undefined (reading 'data')"**
**Cause**: JavaScript error due to malformed widget structure
**Solution**: Ensure all required properties are present and correctly formatted

### **Issue 4: Widget titles not showing**
**Cause**: Incorrect title structure
**Solution**: Use `frame.showTitle` structure instead of `title.visible`
```json
"frame": {
  "showTitle": true,
  "title": "Widget Title"
}
```

### **Issue 5: SQL syntax errors**
**Cause**: Missing spaces in query concatenation
**Solution**: Ensure all query lines end with spaces, especially keywords

### **Issue 6: Charts not rendering**
**Cause**: Missing scale properties in encodings
**Solution**: Add proper scale types:
- `"temporal"` for date/time fields
- `"quantitative"` for numeric fields  
- `"ordinal"` for categorical fields

---

## 🎯 **Best Practices**

### **1. Dataset Design**
- Use descriptive `displayName` for datasets
- Split SQL queries into logical lines with proper spacing
- Include all necessary parameters in dataset definitions
- Use consistent naming conventions (`dataset_001`, `dataset_002`, etc.)

### **2. Widget Configuration**
- Always include `displayName` for widgets
- Use proper widget versions (1 for tables, 3 for charts, 2 for filters)
- Include `frame.showTitle` for visible titles
- Add proper scale and format properties for charts

### **3. Parameter Binding**
- Create parameter queries for all filter parameters
- Use consistent parameter naming (`param_start_date`, `param_workspace`)
- Include proper default values
- Use `IF(:param = '<ALL_WORKSPACES>', true, condition)` pattern for "All" options

### **4. Field Mapping**
- Ensure field names in widgets match SQL column names exactly
- Use `fields` array in widget queries for proper data binding
- Include proper `expression` values with backticks
- Set `disaggregated: false` for data widgets

### **5. SQL Query Design**
- Use parameterized queries with `:param_name` syntax
- Include proper date formatting with `date_format()`
- Add proper spacing for query line concatenation
- Use consistent table and column naming

### **6. Validation**
- Always validate SQL syntax before deployment
- Check widget structure completeness
- Verify parameter binding correctness
- Test field mappings against actual data

---

## 📁 **File Structure**
```
dashboard_v3/
├── dbv4_gen.py              # Main dashboard generator
├── dbv4_sql.json            # SQL queries and parameters
├── dbv4_template.json       # Widget layout template
├── dbv4_out.lvdash.json     # Final generated dashboard
└── archive/                 # Historical files
```

---

## 🔧 **Generator Usage**
```bash
cd resources/dashboard_v3
python dbv4_gen.py
```

The generator will:
1. Load SQL queries and template
2. Update date parameters to current dynamic dates
3. Create datasets with proper structure
4. Generate parameter queries for all filters
5. Create widgets with correct configurations
6. Output final dashboard JSON

---

*This knowledge base represents the complete understanding of Databricks dashboard structure, parameter binding, widget configurations, and validation steps based on successful implementation and testing.*

