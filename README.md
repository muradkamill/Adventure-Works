# Azure Databricks End-to-End ETL Pipeline

An end-to-end Data Engineering project built on **Microsoft Azure** and **Databricks**, implementing a scalable **Medallion Architecture (Bronze → Silver → Gold)** for processing and transforming Adventure Works data.

The project focuses on automated data ingestion, data quality management, centralized governance with Unity Catalog, and dimensional data modeling for analytical workloads.

---

## Architecture

The solution is built using the following Azure and Databricks components:

- **Microsoft Azure**
- **Azure Resource Group**
- **Azure Data Lake Storage Gen2 (ADLS Gen2)**
- **Azure Databricks**
- **Unity Catalog**
- **Delta Lake**
- **Auto Loader**
- **Structured Streaming**
- **PySpark**
- **Lakeflow Declarative Pipelines**
- **Medallion Architecture**
- **Dimensional Modeling / Star Schema**

### High-Level Data Flow

```text
Source Data
    │
    ▼
ADLS Gen2
    │
    ▼
┌──────────────────────┐
│       BRONZE         │
│ Raw Data Ingestion   │
│ Auto Loader +        │
│ Structured Streaming │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│       SILVER         │
│ Data Quality +       │
│ Transformations      │
│ + Quarantine         │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│        GOLD          │
│ Dimensions + Fact    │
│ Star Schema          │
└──────────────────────┘
```

---

# Azure Infrastructure

The project is deployed within an **Azure Resource Group** containing:

- Azure Data Lake Storage Gen2
- Azure Databricks Workspace

ADLS Gen2 is used as the data lake storage layer, while Databricks provides the processing, transformation, orchestration, and analytical environment.

---

# Unity Catalog & Data Governance

To provide centralized governance and secure management of data assets, a **Unity Catalog Metastore** was created and assigned to the Databricks Workspace.

Unity Catalog provides centralized management of:

- Catalogs
- Schemas
- Tables
- Metadata
- Data access control
- Data lineage
- Permissions
- Data governance

This allows the data platform to manage data assets through a unified governance layer instead of managing permissions and metadata independently across individual workspaces or tables.

The resulting structure follows:

```text
Metastore
    │
    └── Catalog
          │
          ├── Schema
          │     │
          │     ├── Tables
          │     ├── Views
          │     └── ...
```

---

# Medallion Architecture

The pipeline follows the **Medallion Architecture**, separating raw ingestion, data cleansing, and analytical modeling into three logical layers.

## Bronze Layer

The Bronze layer is responsible for ingesting raw source data into the data lake.

The ingestion process uses:

- **Auto Loader**
- **Structured Streaming**
- **Delta Lake**

The source contains multiple folders representing different datasets:

```text
calendar
customers
productCategories
productSubcategories
products
returns
sales
territories
```

### Automated Bronze Ingestion

Instead of manually entering the source folder name every time a Bronze notebook is executed, the pipeline was automated using a Parameters notebook.

The dataset configuration is maintained centrally:

```python
values = [
    {'name': 'calendar'},
    {'name': 'customers'},
    {'name': 'productCategories'},
    {'name': 'productSubcategories'},
    {'name': 'products'},
    {'name': 'returns'},
    {'name': 'sales'},
    {'name': 'territories'}
]

dbutils.jobs.taskValues.set('list', values)
```

The task values are then consumed by the Bronze ingestion workflow.

This allows the same ingestion logic to process multiple source folders dynamically without manually changing notebook parameters for every execution.

### Benefits

- Reduced manual configuration
- Reusable ingestion logic
- Easier pipeline maintenance
- Scalable ingestion of multiple datasets
- Automated orchestration

---

# Silver Layer

The Silver layer is responsible for **data cleansing, validation, quality checks, and transformations**.

Typical operations include:

- Data type standardization
- Duplicate handling
- Null validation
- Data quality checks
- Business-rule validation
- Data transformation
- Preparation of clean datasets for the Gold layer

### Data Quarantine

An important part of the Silver layer is the handling of invalid records.

If a record contains an undefined, invalid, or otherwise unresolvable value, it is not allowed to continue directly into the trusted data layer.

Instead, invalid records are redirected to a dedicated:

```text
quarantine/
```

location.

This provides a controlled mechanism for handling bad data without unnecessarily breaking the entire pipeline.

The quarantined data can later be:

- Investigated
- Reprocessed
- Corrected
- Re-filtered
- Permanently removed if required

This separates **data quality management** from the main processing flow.

---

# Gold Layer

The Gold layer contains business-ready analytical datasets.

The data model follows a **Star Schema**, with dimension tables surrounding the central sales fact table.

### Dimensions

The project contains dimensions such as:

- `dim_calendar`
- `dim_customers`
- `dim_products`
- `dim_territories`
- `dim_productCategories`
- `dim_productSubcategories`

A separate returns dataset is also modeled for analytical reporting.

### Surrogate Keys

Surrogate keys are introduced at the Gold layer to provide stable dimensional relationships.

For example:

```text
CustomerKey
ProductKey
TerritoryKey
CalendarDimKey
ProductCategoryKey
ProductSubcategoryKey
```

The fact table references these dimension keys instead of relying directly on descriptive source attributes.

---

# Sales Fact Table

The final `fact_sales` table is designed as the central transactional table of the analytical model.

The fact table contains:

- Date keys
- Dimension surrogate keys
- Order information
- Measures such as quantity
- Other required transactional attributes

For example:

```text
fact_sales
│
├── OrderDateKey
├── StockDateKey
├── ProductKey
├── CustomerKey
├── TerritoryKey
├── OrderNumber
├── OrderLineItem
└── OrderQuantity
```

Dates are connected to the Calendar dimension through surrogate keys, while customer, product, and territory relationships are established through their respective dimension keys.

This creates a clean analytical model suitable for reporting and BI workloads.

---

# Pipeline Dependency Graph

The Databricks workflow is organized into dependent stages:

```text
Parameters
     │
     ▼
Bronze Auto Loader
     │
     ├───────────────┐
     ▼               ▼
Silver Customers   Silver Products
     │               │
     │               ├──► Dim Products
     │               │
     │               └──► Silver Sales
     │
     ├──► Dim Customers
     │
     ▼
Silver Sales
     │
     ├──► Fact Sales
     │
     └──► ...
```

Additional dimensions and fact-related datasets are processed through their respective dependencies.

The Databricks job graph makes these dependencies explicit and allows the pipeline to execute tasks in the required order.

---

# Data Model

The final Gold layer follows a dimensional/star-schema approach:

```text
                     ┌─────────────────┐
                     │   Categories    │
                     └────────┬────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │  SubCategories  │
                     └────────┬────────┘
                              │
                              ▼
┌─────────────────┐     ┌───────────────┐     ┌─────────────────┐
│    Customers    │────►│    Products   │◄────│    Territories  │
└─────────────────┘     └───────┬───────┘     └─────────────────┘
                                │
                                ▼
                         ┌──────────────┐
                         │  fact_sales  │
                         └──────┬───────┘
                                │
                                ▼
                         ┌──────────────┐
                         │   Calendar   │
                         └──────────────┘
```

This structure separates descriptive attributes into dimensions and transactional measurements into fact tables.

---

# Key Engineering Concepts Demonstrated

This project demonstrates practical implementation of:

### Data Engineering

- End-to-end ETL pipeline development
- Medallion Architecture
- Batch and streaming-based ingestion
- Data transformation with PySpark
- Data quality validation
- Data quarantine
- Dimensional modeling
- Star schema design
- Surrogate key generation
- Fact and dimension relationships

### Azure & Databricks

- Azure Resource Groups
- ADLS Gen2
- Azure Databricks
- Unity Catalog
- Delta Lake
- Auto Loader
- Structured Streaming
- Lakeflow Declarative Pipelines
- Databricks Jobs & Pipelines

### Governance

- Centralized metadata management
- Catalog and schema organization
- Data lineage
- Access control
- Data permissions
- Centralized data governance

---

# Project Goals

The main objectives of this project were to build a data platform that is:

- **Automated** — minimizing manual ingestion configuration
- **Scalable** — supporting multiple datasets through reusable ingestion logic
- **Reliable** — isolating invalid records through quarantine
- **Governed** — centrally managed through Unity Catalog
- **Maintainable** — separated into Bronze, Silver, and Gold layers
- **Analytics-ready** — using a dimensional model for downstream reporting

---

# Technologies

| Technology | Purpose |
|---|---|
| Azure | Cloud infrastructure |
| ADLS Gen2 | Data lake storage |
| Azure Databricks | Data processing platform |
| Unity Catalog | Governance and metadata management |
| Delta Lake | Reliable data storage |
| Auto Loader | Incremental file ingestion |
| Structured Streaming | Streaming ingestion |
| PySpark | Data transformation |
| Lakeflow Declarative Pipelines | Pipeline processing |
| Databricks Jobs | Workflow orchestration |

---

# Project Outcome

The final result is an end-to-end Azure Databricks data platform that takes raw source files from ADLS Gen2, automatically ingests them into the Bronze layer, validates and transforms them in Silver, isolates problematic records through quarantine, and produces a governed Gold layer based on a dimensional star schema.

The resulting Gold layer provides clean, structured, analytics-ready datasets with centralized governance and traceable relationships between fact and dimension tables.
