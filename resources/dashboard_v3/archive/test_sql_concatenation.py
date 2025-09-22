#!/usr/bin/env python3
import json

# Load and test SQL concatenation
with open('dbv4_out.lvdash.json', 'r') as f:
    dashboard = json.load(f)

print("🔍 Testing SQL concatenation for all datasets...")

for dataset in dashboard["datasets"]:
    dataset_name = dataset["name"]
    query_lines = dataset["queryLines"]
    concatenated_sql = "".join(query_lines)
    
    print(f"\n📋 {dataset_name}:")
    print(f"   Concatenated: {concatenated_sql[:100]}...")
    
    # Check for issues
    issues = []
    if "SELECTCOUNT" in concatenated_sql:
        issues.append("SELECTCOUNT")
    if "FROMplatform_observability" in concatenated_sql:
        issues.append("FROMplatform_observability")
    if "UNIONALL" in concatenated_sql:
        issues.append("UNIONALL")
    if "WHEREdate_key" in concatenated_sql:
        issues.append("WHEREdate_key")
    if "ANDdate_key" in concatenated_sql:
        issues.append("ANDdate_key")
    
    if issues:
        print(f"   ❌ Issues: {', '.join(issues)}")
    else:
        print(f"   ✅ No concatenation issues")

print("\n✅ SQL concatenation test complete")
