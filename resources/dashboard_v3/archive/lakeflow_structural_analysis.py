#!/usr/bin/env python3
"""
LakeFlow Dashboard Structural Analysis
Thoroughly analyze the working LakeFlow dashboard to understand the correct structure
"""

import json
import os
from collections import defaultdict
from typing import Dict, List, Any, Set

class LakeFlowStructuralAnalyzer:
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.lakeflow_path = os.path.join(self.base_dir, "../dashboard/LakeFlow System Tables Dashboard v0.1.lvdash.json")
        
    def load_lakeflow_dashboard(self) -> Dict[str, Any]:
        """Load the LakeFlow dashboard"""
        with open(self.lakeflow_path, 'r') as f:
            return json.load(f)
    
    def analyze_top_level_structure(self, dashboard: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze the top-level structure"""
        structure = {
            "top_level_keys": list(dashboard.keys()),
            "datasets_count": len(dashboard.get("datasets", [])),
            "pages_count": len(dashboard.get("pages", [])),
            "has_parameters": "parameters" in dashboard,
            "has_theme": "theme" in dashboard,
            "has_metadata": "metadata" in dashboard
        }
        return structure
    
    def analyze_datasets_structure(self, datasets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze dataset structure"""
        dataset_analysis = {
            "total_datasets": len(datasets),
            "dataset_names": [d.get("name", "unnamed") for d in datasets],
            "dataset_structure": {},
            "parameter_patterns": defaultdict(list),
            "query_patterns": defaultdict(list)
        }
        
        for i, dataset in enumerate(datasets):
            name = dataset.get("name", f"dataset_{i}")
            structure = {
                "has_name": "name" in dataset,
                "has_displayName": "displayName" in dataset,
                "has_queryLines": "queryLines" in dataset,
                "has_parameters": "parameters" in dataset,
                "query_lines_count": len(dataset.get("queryLines", [])),
                "parameters_count": len(dataset.get("parameters", [])),
                "parameter_types": [p.get("dataType", "unknown") for p in dataset.get("parameters", [])],
                "parameter_keywords": [p.get("keyword", "unknown") for p in dataset.get("parameters", [])]
            }
            dataset_analysis["dataset_structure"][name] = structure
            
            # Analyze parameters
            for param in dataset.get("parameters", []):
                keyword = param.get("keyword", "unknown")
                dataset_analysis["parameter_patterns"][keyword].append(name)
        
        return dataset_analysis
    
    def analyze_pages_structure(self, pages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze pages structure"""
        page_analysis = {
            "total_pages": len(pages),
            "page_structure": {},
            "widget_types": defaultdict(int),
            "filter_widgets": [],
            "data_widgets": []
        }
        
        for i, page in enumerate(pages):
            page_name = page.get("name", f"page_{i}")
            layout = page.get("layout", [])
            
            page_structure = {
                "has_name": "name" in page,
                "has_displayName": "displayName" in page,
                "has_layout": "layout" in page,
                "widgets_count": len(layout),
                "widgets": []
            }
            
            for j, widget_layout in enumerate(layout):
                widget = widget_layout.get("widget", {})
                widget_name = widget.get("name", f"widget_{j}")
                widget_type = widget.get("spec", {}).get("widgetType", "unknown")
                
                widget_info = {
                    "name": widget_name,
                    "type": widget_type,
                    "has_queries": "queries" in widget,
                    "queries_count": len(widget.get("queries", [])),
                    "has_spec": "spec" in widget,
                    "spec_version": widget.get("spec", {}).get("version", "unknown"),
                    "has_encodings": "encodings" in widget.get("spec", {}),
                    "encoding_structure": self.analyze_encodings(widget.get("spec", {}).get("encodings", {}))
                }
                
                page_structure["widgets"].append(widget_info)
                page_analysis["widget_types"][widget_type] += 1
                
                # Categorize widgets
                if "filter" in widget_name:
                    page_analysis["filter_widgets"].append(widget_info)
                else:
                    page_analysis["data_widgets"].append(widget_info)
            
            page_analysis["page_structure"][page_name] = page_structure
        
        return page_analysis
    
    def analyze_encodings(self, encodings: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze widget encodings structure"""
        encoding_analysis = {
            "encoding_keys": list(encodings.keys()),
            "has_fields": "fields" in encodings,
            "has_columns": "columns" in encodings,
            "has_x": "x" in encodings,
            "has_y": "y" in encodings,
            "has_theta": "theta" in encodings,
            "has_color": "color" in encodings,
            "fields_count": len(encodings.get("fields", [])),
            "columns_count": len(encodings.get("columns", [])),
            "field_structure": [],
            "column_structure": []
        }
        
        # Analyze fields
        for field in encodings.get("fields", []):
            field_info = {
                "has_fieldName": "fieldName" in field,
                "has_parameterName": "parameterName" in field,
                "has_queryName": "queryName" in field,
                "has_displayName": "displayName" in field,
                "field_name": field.get("fieldName", "unknown"),
                "parameter_name": field.get("parameterName", "unknown"),
                "query_name": field.get("queryName", "unknown")
            }
            encoding_analysis["field_structure"].append(field_info)
        
        # Analyze columns
        for column in encodings.get("columns", []):
            column_info = {
                "has_fieldName": "fieldName" in column,
                "has_displayName": "displayName" in column,
                "has_booleanValues": "booleanValues" in column,
                "has_imageUrlTemplate": "imageUrlTemplate" in column,
                "field_name": column.get("fieldName", "unknown"),
                "display_name": column.get("displayName", "unknown")
            }
            encoding_analysis["column_structure"].append(column_info)
        
        return encoding_analysis
    
    def analyze_parameter_queries(self, dashboard: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze parameter query patterns"""
        parameter_analysis = {
            "parameter_queries": [],
            "parameter_patterns": defaultdict(list),
            "query_naming_patterns": []
        }
        
        # Find all parameter queries in widgets
        for page in dashboard.get("pages", []):
            for widget_layout in page.get("layout", []):
                widget = widget_layout.get("widget", {})
                for query in widget.get("queries", []):
                    query_name = query.get("name", "")
                    if "parameter_dashboards" in query_name:
                        parameter_analysis["parameter_queries"].append({
                            "query_name": query_name,
                            "widget_name": widget.get("name", "unknown"),
                            "widget_type": widget.get("spec", {}).get("widgetType", "unknown"),
                            "query_structure": query.get("query", {})
                        })
                        parameter_analysis["query_naming_patterns"].append(query_name)
        
        return parameter_analysis
    
    def generate_structural_context(self) -> Dict[str, Any]:
        """Generate comprehensive structural context"""
        print("🔍 Loading LakeFlow dashboard...")
        dashboard = self.load_lakeflow_dashboard()
        
        print("📊 Analyzing top-level structure...")
        top_level = self.analyze_top_level_structure(dashboard)
        
        print("📊 Analyzing datasets structure...")
        datasets_analysis = self.analyze_datasets_structure(dashboard.get("datasets", []))
        
        print("📊 Analyzing pages structure...")
        pages_analysis = self.analyze_pages_structure(dashboard.get("pages", []))
        
        print("📊 Analyzing parameter queries...")
        parameter_analysis = self.analyze_parameter_queries(dashboard)
        
        structural_context = {
            "top_level_structure": top_level,
            "datasets_analysis": datasets_analysis,
            "pages_analysis": pages_analysis,
            "parameter_analysis": parameter_analysis,
            "key_insights": self.extract_key_insights(top_level, datasets_analysis, pages_analysis, parameter_analysis)
        }
        
        return structural_context
    
    def extract_key_insights(self, top_level: Dict, datasets: Dict, pages: Dict, parameters: Dict) -> List[str]:
        """Extract key insights from the analysis"""
        insights = []
        
        # Top-level insights
        insights.append(f"Dashboard has {top_level['datasets_count']} datasets and {top_level['pages_count']} pages")
        
        # Dataset insights
        insights.append(f"All datasets have name, displayName, and queryLines")
        insights.append(f"Parameter types used: {set().union(*[d['parameter_types'] for d in datasets['dataset_structure'].values()])}")
        
        # Widget insights
        widget_types = pages['widget_types']
        insights.append(f"Widget types found: {dict(widget_types)}")
        
        # Filter vs data widget insights
        insights.append(f"Filter widgets: {len(pages['filter_widgets'])}, Data widgets: {len(pages['data_widgets'])}")
        
        # Encoding insights
        for widget_info in pages['data_widgets']:
            encoding = widget_info['encoding_structure']
            if encoding['has_fields']:
                insights.append(f"Widget {widget_info['name']} uses 'fields' encoding with {encoding['fields_count']} fields")
            if encoding['has_columns']:
                insights.append(f"Widget {widget_info['name']} uses 'columns' encoding with {encoding['columns_count']} columns")
            if encoding['has_x'] and encoding['has_y']:
                insights.append(f"Widget {widget_info['name']} uses x/y encoding pattern")
        
        # Parameter query insights
        insights.append(f"Parameter queries found: {len(parameters['parameter_queries'])}")
        
        return insights
    
    def save_structural_context(self, context: Dict[str, Any]) -> str:
        """Save the structural context to a file"""
        output_path = os.path.join(self.base_dir, "lakeflow_structural_context.json")
        with open(output_path, 'w') as f:
            json.dump(context, f, indent=2)
        
        # Also create a human-readable summary
        summary_path = os.path.join(self.base_dir, "lakeflow_structural_summary.md")
        with open(summary_path, 'w') as f:
            f.write("# LakeFlow Dashboard Structural Analysis\n\n")
            f.write("## Key Insights\n\n")
            for insight in context["key_insights"]:
                f.write(f"- {insight}\n")
            
            f.write("\n## Top-Level Structure\n\n")
            f.write(f"- Keys: {context['top_level_structure']['top_level_keys']}\n")
            f.write(f"- Datasets: {context['top_level_structure']['datasets_count']}\n")
            f.write(f"- Pages: {context['top_level_structure']['pages_count']}\n")
            
            f.write("\n## Widget Types\n\n")
            for widget_type, count in context['pages_analysis']['widget_types'].items():
                f.write(f"- {widget_type}: {count}\n")
            
            f.write("\n## Parameter Queries\n\n")
            f.write(f"- Total parameter queries: {len(context['parameter_analysis']['parameter_queries'])}\n")
        
        return output_path

def main():
    analyzer = LakeFlowStructuralAnalyzer()
    context = analyzer.generate_structural_context()
    output_path = analyzer.save_structural_context(context)
    
    print("✅ LakeFlow structural analysis completed!")
    print(f"📊 Context saved to: {output_path}")
    print(f"📋 Summary saved to: lakeflow_structural_summary.md")
    
    print("\n🔍 Key Insights:")
    for insight in context["key_insights"]:
        print(f"   - {insight}")

if __name__ == "__main__":
    main()
