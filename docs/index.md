# Platform Observability - Documentation

Welcome to the comprehensive documentation for Platform Observability (CDF-driven).

## 📚 Documentation Structure

### **Core Documentation**
1. [01-overview.md](01-overview.md) - Solution Overview
   - **Purpose**: High-level architecture and design principles
   - **Audience**: Architects, Product Managers, Stakeholders
   - **Content**: Objectives, scope, solution design, data flow, usage patterns

2. [02-getting-started.md](02-getting-started.md) - Getting Started Guide
   - **Purpose**: Step-by-step deployment and setup
   - **Audience**: Developers, DevOps Engineers
   - **Content**: Prerequisites, quick start, detailed deployment, validation

3. [03-parameterization.md](03-parameterization.md) - Parameterization Guide
   - **Purpose**: Configuration management and SQL parameterization
   - **Audience**: Developers, Platform Engineers
   - **Content**: Configuration system, SQL organization, usage examples, migration

### **Technical Documentation**
4. [04-data-dictionary.md](04-data-dictionary.md) - Comprehensive Data Dictionary
   - **Purpose**: Complete data model documentation
   - **Audience**: Data Engineers, Analysts, BI Developers
   - **Content**: Bronze, Silver, Gold layer schemas, data flow, relationships

5. [05-tag-processing-and-chargeback.md](05-tag-processing-and-chargeback.md) - Tag Processing and Chargeback
   - **Purpose**: Comprehensive guide to tag processing, hierarchy, and chargeback model
   - **Audience**: Data Engineers, Platform Engineers, Business Analysts
   - **Content**: Tag hierarchy, processing flow, chargeback views, quality monitoring

6. [06-runtime-analysis-and-optimization.md](06-runtime-analysis-and-optimization.md) - Runtime Analysis and Optimization
   - **Purpose**: Databricks Runtime analysis, node types, and optimization insights
   - **Audience**: Platform Engineers, DevOps Engineers, Cost Optimization Teams
   - **Content**: Runtime version tracking, node type analysis, modernization opportunities, cost optimization

7. [07-insights-and-use-cases.md](07-insights-and-use-cases.md) - Insights and Use Cases
   - **Purpose**: Comprehensive guide to insights and use cases by persona and category
   - **Audience**: All stakeholders, Business Users, Platform Engineers, Data Teams
   - **Content**: Persona-based use cases, insight categories, implementation examples, best practices

### **Advanced Features**
8. [08-scd2-temporal-join-example.md](08-scd2-temporal-join-example.md) - SCD2 Implementation with Temporal Joins
   - **Purpose**: Comprehensive guide to SCD2 temporal joins and fact-dimension relationships
   - **Audience**: Data Engineers, Platform Engineers, BI Developers
   - **Content**: SCD2 temporal logic, fact-dimension associations, historical analysis examples

9. [09-task-based-processing.md](09-task-based-processing.md) - Task-Based Processing State Management
   - **Purpose**: Comprehensive guide to task-based processing state management across all layers
   - **Audience**: Data Engineers, Platform Engineers, DevOps Engineers
   - **Content**: Task-based offsets, independent processing, fault tolerance, monitoring, cross-layer integration

### **Operations & Migration**
10. [10-recent-changes-summary.md](10-recent-changes-summary.md) - Recent Changes & Migration Summary
    - **Purpose**: Comprehensive summary of all major changes, migrations, and updates
    - **Audience**: Developers, Platform Engineers, DevOps Engineers
    - **Content**: Architectural changes, naming convention updates, HWM migration, SCD2 implementation, file structure changes

11. [11-deployment.md](11-deployment.md) - Cloud-Agnostic Deployment Guide
    - **Purpose**: Cloud-agnostic production deployment and operational guidance
    - **Audience**: DevOps Engineers, Platform Engineers
    - **Content**: Cloud-agnostic deployment, pipeline deployment, workflow configuration, monitoring, troubleshooting

## 🚀 Quick Navigation

### **For New Users**
1. Start with [01-overview.md](01-overview.md) to understand the solution
2. Follow [02-getting-started.md](02-getting-started.md) for deployment
3. Reference [04-data-dictionary.md](04-data-dictionary.md) for data understanding
4. Explore [07-insights-and-use-cases.md](07-insights-and-use-cases.md) for business value

### **For Developers**
1. Review [03-parameterization.md](03-parameterization.md) for configuration
2. Study [04-data-dictionary.md](04-data-dictionary.md) for data models
3. Check [10-recent-changes-summary.md](10-recent-changes-summary.md) for latest changes
4. Reference [08-scd2-temporal-join-example.md](08-scd2-temporal-join-example.md) for SCD2 details
5. Review [09-task-based-processing.md](09-task-based-processing.md) for HWM approach

### **For Operations**
1. Follow [11-deployment.md](11-deployment.md) for cloud-agnostic deployment
2. Use [04-data-dictionary.md](04-data-dictionary.md) for troubleshooting
3. Reference [02-getting-started.md](02-getting-started.md) for validation
4. Check [10-recent-changes-summary.md](10-recent-changes-summary.md) for migration guidance

### **For Business Users**
1. Start with [01-overview.md](01-overview.md) for solution understanding
2. Explore [07-insights-and-use-cases.md](07-insights-and-use-cases.md) for business value
3. Review [05-tag-processing-and-chargeback.md](05-tag-processing-and-chargeback.md) for cost allocation
4. Check [06-runtime-analysis-and-optimization.md](06-runtime-analysis-and-optimization.md) for optimization insights

## 📋 Additional Resources

- **Repository**: Main code repository with source code
- **Examples**: Usage examples and sample configurations
- **Tests**: Test suite and validation scripts
- **Jobs**: Workflow configurations and scheduling

## 🔗 Cross-References

All documentation files include cross-references to related documents, making it easy to navigate between different topics and find relevant information quickly.

## 📝 Contributing

To contribute to the documentation:
1. Follow the established naming convention (##-description.md)
2. Include table of contents with anchor links
3. Add cross-references to related documents
4. Use consistent formatting and structure
5. Update this index file when adding new documents

---

*Last Updated: January 2025*  
*Version: 2.0*