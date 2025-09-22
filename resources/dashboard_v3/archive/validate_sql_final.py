#!/usr/bin/env python3
"""
Final validation of SQL concatenation
"""

import json
import os

def validate_sql_final():
    """Final validation of SQL concatenation"""
    
    # Load the dashboard
    with open('dbv4_final_working_spacing_fixed.lvdash.json', 'r') as f:
        dashboard = json.load(f)
    
    print("🔍 Final validation of SQL concatenation...")
    
    issues = []
    
    # Test each dataset's SQL concatenation
    for dataset in dashboard["datasets"]:
        dataset_name = dataset["name"]
        query_lines = dataset["queryLines"]
        
        # Concatenate the queryLines (this is what Databricks does)
        concatenated_sql = "".join(query_lines)
        
        print(f"\n📋 Testing {dataset_name}:")
        print(f"   Concatenated SQL: {concatenated_sql[:150]}...")
        
        # Check for ACTUAL concatenation issues (not false positives)
        if "SELECTCOUNT" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: SELECTCOUNT found - missing space between SELECT and COUNT")
        if "FROMplatform_observability" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: FROMplatform_observability found - missing space between FROM and table name")
        if "WHEREdate_key" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: WHEREdate_key found - missing space between WHERE and condition")
        if "ANDdate_key" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: ANDdate_key found - missing space between AND and condition")
        if "UNIONALL" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: UNIONALL found - missing space between UNION and ALL")
        if "ORDERBY" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: ORDERBY found - missing space between ORDER and BY")
        if "GROUPBY" in concatenated_sql:
            issues.append(f"❌ {dataset_name}: GROUPBY found - missing space between GROUP and BY")
        
        # Check for proper spacing patterns
        if "SELECT " in concatenated_sql:
            print(f"   ✅ SELECT spacing is correct")
        else:
            issues.append(f"❌ {dataset_name}: SELECT spacing is incorrect")
        
        if " FROM " in concatenated_sql:
            print(f"   ✅ FROM spacing is correct")
        else:
            issues.append(f"❌ {dataset_name}: FROM spacing is incorrect")
        
        if " WHERE " in concatenated_sql:
            print(f"   ✅ WHERE spacing is correct")
        else:
            issues.append(f"❌ {dataset_name}: WHERE spacing is incorrect")
    
    # Print results
    print(f"\n📊 Final Validation Results:")
    print(f"   Datasets tested: {len(dashboard['datasets'])}")
    
    if issues:
        print(f"\n❌ Issues Found ({len(issues)}):")
        for issue in issues:
            print(f"   {issue}")
        print(f"\n❌ SQL concatenation validation FAILED!")
        return False
    else:
        print(f"\n✅ No concatenation issues found!")
        print(f"✅ SQL concatenation validation PASSED!")
        print(f"✅ Dashboard is ready for import!")
        return True

if __name__ == "__main__":
    validate_sql_final()
