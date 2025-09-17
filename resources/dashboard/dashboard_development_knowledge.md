# Dashboard Development Knowledge Base

## 🎯 **Overview**
This document contains all learnings, patterns, and best practices discovered during the development of Databricks dashboards for the Platform Observability project.

## 📋 **Key Learnings**

### **1. Databricks Dashboard Structure**
- **File Extension**: Must use `.lvdash.json` (not `.json`)
- **Widget Version**: Use `version: 1` for most widgets, `version: 2` for filter widgets
- **JSON Validation**: Always validate JSON syntax before import

### **2. Parameter Binding Patterns**

#### **LakeFlow Pattern (Recommended)**
- **Separate Filter Widgets**: Create separate widgets for each parameter (start_date, end_date)
- **Parameter Queries**: Each filter widget needs parameter queries that reference the dataset
- **Query Names**: Use format `parameter_dashboards/{dashboard_id}/datasets/{dataset_id}_{param_name}`
- **Parameter Structure**: Use `parameterName` and `queryName` in filter encodings

#### **Parameter Query Structure**
```json
{
  "name": "parameter_dashboards/dashboard_v0/datasets/dataset_param_start_date",
  "query": {
    "datasetName": "dataset_name",
    "parameters": [
      {
        "name": "param_start_date",
        "keyword": "param_start_date"
      }
    ],
    "disaggregated": false
  }
}
```

### **3. Filter Widget Patterns**

#### **Date Filter Widget (LakeFlow Pattern)**
```json
{
  "widget": {
    "name": "start_date_filter",
    "queries": [/* parameter queries */],
    "spec": {
      "version": 2,
      "widgetType": "filter-date-picker",
      "encodings": {
        "fields": [
          {
            "parameterName": "param_start_date",
            "queryName": "parameter_dashboards/dashboard_v0/datasets/dataset_param_start_date"
          }
        ]
      },
      "selection": {
        "defaultSelection": {
          "values": {
            "dataType": "DATE",
            "values": [{"value": "2025-08-16T00:00:00.000"}]
          }
        }
      }
    }
  }
}
```

### **4. Dataset Parameter Structure**
```json
{
  "name": "dataset_name",
  "displayName": "Dataset Display Name",
  "queryLines": [
    "SELECT ...",
    "FROM table f",
    "WHERE f.date_key >= date_format(:param_start_date, 'yyyyMMdd')",
    "  AND f.date_key <= date_format(:param_end_date, 'yyyyMMdd')"
  ],
  "parameters": [
    {
      "displayName": "Start Date",
      "keyword": "param_start_date",
      "dataType": "DATE",
      "defaultSelection": {
        "values": {
          "dataType": "DATE",
          "values": [{"value": "2025-08-16T00:00:00.000"}]
        }
      }
    }
  ]
}
```

### **5. Widget Types and Patterns**

#### **Counter Widget**
```json
{
  "widget": {
    "name": "counter_widget",
    "queries": [
      {
        "name": "main_query",
        "query": {
          "datasetName": "dataset_name",
          "fields": [
            {
              "name": "field_name",
              "expression": "SUM(`column_name`)"
            }
          ],
          "disaggregated": true
        }
      }
    ],
    "spec": {
      "version": 1,
      "widgetType": "counter",
      "encodings": {
        "value": {
          "fieldName": "field_name",
          "queryName": "main_query",
          "numberFormat": "number-currency"
        }
      }
    }
  }
}
```

#### **Table Widget**
```json
{
  "widget": {
    "name": "table_widget",
    "queries": [
      {
        "name": "main_query",
        "query": {
          "datasetName": "dataset_name",
          "fields": [
            {"name": "col1", "expression": "`col1`"},
            {"name": "col2", "expression": "`col2`"}
          ],
          "disaggregated": true
        }
      }
    ],
    "spec": {
      "version": 1,
      "widgetType": "table",
      "encodings": {
        "columns": [
          {"fieldName": "col1", "queryName": "main_query"},
          {"fieldName": "col2", "queryName": "main_query"}
        ]
      }
    }
  }
}
```

## 🚨 **Common Issues and Solutions**

### **Issue 1: "Failed to import dashboard: failed to parse serialized dashboard"**
- **Cause**: Invalid JSON structure or wrong widget versions
- **Solution**: Use `version: 1` for most widgets, `version: 2` for filters

### **Issue 2: "Filter has no fields or parameters selected"**
- **Cause**: Filter widget not properly connected to dataset parameters
- **Solution**: Use LakeFlow pattern with separate filter widgets and parameter queries

### **Issue 3: "UNRESOLVED_COLUMN.WITH_SUGGESTION"**
- **Cause**: Trying to query parameters as columns
- **Solution**: Use `parameterName` in filter encodings, not `fieldName`

### **Issue 4: "UNBOUND_SQL_PARAMETER"**
- **Cause**: SQL has parameter placeholders but no parameter definitions
- **Solution**: Ensure dataset has parameters array with matching keywords

### **Issue 5: "Missing selections for parameters"**
- **Cause**: Parameters missing defaultSelection
- **Solution**: Add defaultSelection to all parameters in dataset

## ✅ **Validation Checklist**

### **Pre-Generation Checks**
1. JSON structure is valid
2. Dataset has correct parameters
3. SQL has parameter placeholders
4. Default dates are current (not hardcoded)
5. Widget versions are correct (1 for widgets, 2 for filters)

### **Post-Generation Checks**
1. JSON syntax validation
2. File extension is `.lvdash.json`
3. All widgets reference datasets correctly
4. Filter widgets follow LakeFlow pattern
5. Parameter queries exist and reference datasets
6. No invalid column references
7. Layout positioning is correct
8. All validation checks pass

## 🏗️ **Development Workflow**

### **1. Template Creation**
- Create `.databoard_template_v0.json` with dashboard structure
- Define page layouts and widget positions
- Include placeholder sections for datasets and SQL

### **2. SQL Query Management**
- Create `dashboard_sql_v0.json` with human-readable SQL
- Use parameter placeholders (`:param_start_date`, `:param_end_date`)
- Ensure proper WHERE clauses for filtering

### **3. Generator Script**
- Create `dashboard_gen_v0.py` that reads template and SQL
- Inject parameters dynamically
- Generate final `.lvdash.json` file
- Include comprehensive validation

### **4. Testing and Validation**
- Import dashboard into Databricks
- Test filter functionality
- Verify data display
- Check for errors and warnings

## 📊 **Best Practices**

### **1. Date Handling**
- Use current dates as defaults (last 30 days)
- Format: `YYYY-MM-DDTHH:MM:SS.000`
- Avoid hardcoded old dates

### **2. Parameter Naming**
- Use consistent naming: `param_start_date`, `param_end_date`
- Match parameter keywords with SQL placeholders
- Use descriptive display names

### **3. Widget Layout**
- Place filters at top (y=0)
- Use consistent positioning
- Avoid overlapping widgets
- Use appropriate widths and heights

### **4. SQL Optimization**
- Use proper table aliases
- Include necessary JOINs
- Add proper WHERE clauses for filtering
- Use appropriate aggregations

### **5. Error Handling**
- Validate JSON before writing
- Check for missing parameters
- Verify widget connections
- Test import functionality

## 🔧 **Tools and Scripts**

### **Generator Script Structure**
```python
def read_template(template_file):
    """Read dashboard template"""
    
def read_sql_queries(sql_file):
    """Read SQL queries"""
    
def inject_parameters(sql, params):
    """Inject parameters into SQL"""
    
def create_dataset_entry(dataset_id, dataset_info):
    """Create dataset with parameters"""
    
def create_filter_widget(param_name, position):
    """Create filter widget following LakeFlow pattern"""
    
def create_widget(widget_type, config, position):
    """Create widget of specified type"""
    
def validate_dashboard(dashboard):
    """Comprehensive validation"""
    
def generate_dashboard():
    """Main generation function"""
```

### **Validation Functions**
```python
def validate_json_structure(dashboard):
    """Validate JSON syntax and structure"""
    
def validate_parameters(dataset):
    """Validate dataset parameters"""
    
def validate_sql_binding(sql_lines):
    """Validate SQL parameter binding"""
    
def validate_filter_widgets(layout):
    """Validate filter widget structure"""
    
def validate_widget_connections(layout):
    """Validate widget dataset connections"""
```

## 📈 **Performance Considerations**

### **1. Query Optimization**
- Use appropriate indexes
- Limit result sets
- Use efficient aggregations
- Avoid N+1 queries

### **2. Dashboard Performance**
- Limit number of widgets per page
- Use appropriate refresh intervals
- Optimize filter queries
- Cache frequently accessed data

### **3. User Experience**
- Provide loading indicators
- Use appropriate default values
- Handle empty states gracefully
- Provide clear error messages

## 🎯 **Success Criteria**

### **Functional Requirements**
- ✅ Dashboard imports successfully
- ✅ Filters work and apply to widgets
- ✅ Data displays correctly
- ✅ No console errors
- ✅ Responsive layout

### **Technical Requirements**
- ✅ Valid JSON structure
- ✅ Proper parameter binding
- ✅ LakeFlow pattern compliance
- ✅ Comprehensive validation
- ✅ Clean, maintainable code

## 📝 **Future Enhancements**

### **1. Additional Filter Types**
- Workspace filter
- Cost center filter
- Environment filter
- Custom dimension filters

### **2. Advanced Widgets**
- Line charts
- Bar charts
- Pie charts
- Heatmaps
- Custom visualizations

### **3. Dashboard Features**
- Multiple pages
- Drill-down capabilities
- Export functionality
- Scheduled refreshes
- Alerting

### **4. Automation**
- CI/CD pipeline
- Automated testing
- Version control
- Deployment automation

---

**Last Updated**: September 2025
**Version**: 1.0
**Author**: Platform Observability Team
