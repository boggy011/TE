# OneData Databricks Metadata Framework Runbook (Gen 2)

## Project Information

**Project Team:** OneData - Databricks
**Prepared By:** Databricks Development Team
**Contacts:** Partiban, Tejesh

### Version History

| Version | Description / Reason for Change | Reviewed By | Authors | Date |
|---------|---------------------------------|-------------|---------|------|
| 1.0 | Creation | Partiban | Tejesh | Aug 28, 2025 |
| 1.1 | Naming Standards, Decision Tree L1, L2 | Partiban | Tejesh | Sep 3, 2025 |

---

## Table of Contents

1. [Introduction](#1-introduction)
   - [1.1 Prerequisites](#11-prerequisites)
   - [1.2 Roles and Responsibilities](#12-roles-and-responsibilities)
2. [Data Processing & Publication](#2-data-processing--publication)
   - [2.1 Process Flow](#21-process-flow)
   - [2.2 Development Steps](#22-development-steps)
   - [2.3 ER Diagram for Metadata](#23-er-diagram-for-metadata)
   - [2.4 Naming Standards](#24-naming-standards)
   - [2.5 Decision Tree Between L1 & L2](#25-decision-tree-between-l1--l2)
   - [2.6 Deployment Process](#26-deployment-process)
   - [2.7 CICD - Cloudbees Deployment Pipeline](#27-cicd---cloudbees-deployment-pipeline)
   - [2.8 Job Execution](#28-job-execution)
   - [2.9 Multiple SAP Systems Based Processing](#29-multiple-sap-systems-based-processing-and-deployment)
3. [Logging, Monitoring and Troubleshooting](#3-logging-monitoring-and-troubleshooting)
4. [Control-M Job Creation and Execution](#4-control-m-job-creation-and-execution-qa)
5. [ETL Project Life Cycle](#5-etl-project-life-cycle)
6. [Do's & Don'ts](#6-dos--donts)
7. [Guidelines for Multiple SAP Source Systems](#7-guidelines-for-multiple-sap-source-systems--scd--delta-merge)
8. [Frequently Asked Questions (FAQs)](#8-frequently-asked-questions-faqs)
9. [References](#9-references)

---

# 1. Introduction

This runbook is designed to provide teams with the necessary tools and information for the efficient deployment, monitoring, and maintenance of Databricks solutions. By adhering to the guidelines in this document, users can achieve optimal performance, rapid troubleshooting, and effective management of their data pipelines.

Databricks functions as our data processing and publication platform, retrieving data from Amazon S3, loading it into various layers, and making it accessible to business users. In Databricks, the data is processed in distinct stages across the following layers:

- **L0 - Stage Layer** (`L0_stage`)
- **L0 - Raw Layer** (`L0_raw`)
- **L1 - Curated Layer** (`L1_curated`)
- **L2 - Publication / Consume Layer** (`L2_Data_Product`)

## 1.1 Prerequisites

Access patterns vary depending on the stakeholder's specific usage needs, managed through IAM roles and credentials. Request access tailored to your particular use case.

### Accessing Databricks

- Raise an IAM request for the following roles: `DIA_IT_Development`, `DIA_IT_Support` (AWS Console).
- Go to the [IAM Portal](https://iam.connect.te.com/Home), click **All Apps**, and search for the above roles, or use this [direct link](https://iam.connect.te.com/Request/Access/EDH_IT_CLOUD).
- For **Dev**: Fill the environment field with `AWS Corp TEIS Enterprise Data Analytics Dev`.
- For **Prod**: Change the environment to `AWS Corp TEIS Enterprise Data Analytics Prod`.

### Access Control-M

- Raise an IAM request to access Control-M for the development environment via the [IAM Portal](https://iam.connect.te.com/Request/Access/CONTROL-M), selecting Development and providing justification.

### Databricks Important Links

| Resource | Link |
|---------|------|
| AWS Console | [AWS Console URL](http://go/aws) |
| Databricks Dev Console | [Development Console](https://te-eda-dev.cloud.databricks.com/) |
| Databricks QA Console | [Quality Console](https://te-eda-qa.cloud.databricks.com/) |
| Databricks Prod Console | [Production Console](https://te-eda-prod.cloud.databricks.com/) |
| GitHub Repository | [GitHub Portal URL](https://github.com/te-dia) |
| Databricks Training Docs | [Databricks Training](https://te360.sharepoint.com/:f:/r/sites/TE-DIA/Shared%20Documents/DIA%20Architecture/06%20-%20Training/Databricks) |
| DBeaver Connection Guide | [DBeaver Docs](https://docs.databricks.com/en/dev-tools/dbeaver.html) |

### Additional Access Requirements

- Databricks workspace and cluster access (DevOps and Framework developers only)
- Control-M access (for job orchestration)
- Redshift and Snowflake cluster access (Lift and shift use cases only)
- DBeaver as SQL client for Databricks Unity Catalog access
- CI/CD pipeline access for code deployment, support, and maintenance

> **Note:** Refer to the resource onboarding documentation for instructions on requesting access or initiating installation.

## 1.2 Roles and Responsibilities

| Framework | Environment | Module | Developer | DevOps Support | Platform | Architecture |
|-----------|-------------|--------|:---------:|:--------------:|:--------:|:------------:|
| Databricks | Dev | Framework Enhancements | No | No | No | Yes |
| Databricks | Dev | Metadata Entries | Yes | No | No | No |
| Databricks | Dev | PySpark / Generic Scripts Creation | Yes | No | No | No |
| Databricks | Dev | Data Validation | Yes | No | No | No |
| Databricks-CICD | Dev | ETL Deployment | Yes | No | No | No |
| Databricks-CICD | Dev | Framework Deployment Pipeline | No | Yes | No | Yes |
| Databricks-CICD | Dev | Manual Execution | Yes | Yes | No | No |
| Databricks | QA | Framework Enhancements and Support | No | No | No | Yes |
| Databricks | QA | Metadata Scripts Deployment (Dev → QA) | Yes | Yes | No | No |
| Databricks | QA | PySpark / Generic Scripts Deployment (Dev → QA) | Yes | Yes | No | No |
| Databricks | QA | Data Validation | Yes | No | No | No |
| Databricks-CICD | QA | ETL Deployment | Yes | No | No | No |
| Databricks-CICD | QA | Framework Deployment Pipeline | No | Yes | No | Yes |
| Databricks-CICD | QA | Manual Execution | Yes | Yes | No | No |
| Databricks | QA | Control-M Job Execution / Scheduling / Deployment | Yes | Yes | No | No |
| Databricks | PROD | Metadata Scripts Deployment (QA → PROD) | No | Yes | No | No |
| Databricks | PROD | PySpark / Generic Scripts Deployment (QA → PROD) | No | Yes | No | Yes |
| Databricks-CICD | PROD | ETL Deployment | No | Yes | No | No |
| Databricks-CICD | PROD | Framework Deployment Pipeline | No | Yes | No | No |
| Databricks-CICD | PROD | Manual Execution | No | Yes | No | No |
| Databricks | PROD | Control-M Job Execution / Scheduling / Deployment | No | Yes | No | No |

---

# 2. Data Processing & Publication

## 2.1 Process Flow

The OneData framework adopts a **metadata-driven approach**, with distinct layers of ETL processing organized according to structural requirements.

## 2.2 Development Steps

### Step 1: Create DLT / Workflow

Create an entry for the corresponding **Dataflow Group ID (DFG_ID)** in the `data_flow_control_header` metadata table, ensuring uniqueness. Include the necessary fields:

- `Data_Flow_Group_ID`
- `Compute_Class`
- `ETL_LAYER`
- `Ingestion_Mode`
- Other required fields

For DFG_ID naming conventions, refer to the [Development Guide](https://te360.sharepoint.com/sites/TEDSimplification/Shared%20Documents/General/15%20-%20Databricks%20Onboarding/Databricks%20Development%20Guide%20V1.0.docx).

### Step 2: Create Tasks / Detailed Pipeline

Add entries to the detailed metadata table based on the ETL layer for the specified `DFG_ID`.

#### Step 2a: L0-Stage & L0-Raw Layer

Insert the entry into the `DATA_FLOW_L0_DETAIL` metadata table.

**Example landing bucket configuration:**

- **S3 bucket (current):** `s3://aws-te-eda-landing-master-data-restricted-dev`
- **Also supported:** `s3://aws-te-eda-onedata-landing-master-data-c2-dev` — set `C2` in `storage_type` column
- **Folder path:** `ingest/gbl_current/gbl_acct_mgr_assignments_test`

**`DATA_FLOW_L0_DETAIL` Column Reference:**

| Column Name | Description | Example |
|-------------|-------------|---------|
| `DATA_FLOW_GROUP_ID` | Unique entry per logical group | `MASTER_DATA_ACCOUNT_MANAGER_L0` |
| `SOURCE` | Source system where the data originates. Should align with the S3 folder path. (Optional) | |
| `SOURCE_OBJ_SCHEMA` | Source object schema for the dataset. Aligns with the S3 folder path | `GBL_CURRENT` |
| `SOURCE_OBJ_NAME` | Source object name for the dataset. Aligns with the S3 folder path | `gbl_account_manager_types_test` |
| `LOB` | Line of Business; the source S3 bucket is derived from this path | `master-data` |
| `LOAD_TYPE` | Load type: `FULL` or `DELTA` | `FULL` |
| `INPUT_FILE_FORMAT` | File format in the landing bucket. Supports CSV, JSON, Parquet | `parquet` |
| `STORAGE_TYPE` | Storage type for the data (`R` → restricted, `C` → confidential) | `R` |
| `DQ_LOGIC` | Logic/rules for data quality checks (JSON) | See example below |
| `DELIMETER` | Delimiter used for CSV formats (e.g., `,` or `|`) | `null` |
| `CUSTOM_SCHEMA` | Custom schema with column names and data types for TSV only; otherwise `null` | `null` |
| `CDC_LOGIC` | CDC logic for incremental data processing; `null` for FULL Load | See example below |
| `TRANSFORM_QUERY` | Casting / transformation as key-value pairs | `{col_a: cast(col_a as date), col_b: cast(col_b as date)}` |
| `PRESTAG_FLAG` | `Y` if data must be available in L0 Stage; else `N` | `Y` |
| `PARTITION` | Comma-separated partition columns; defaults to `null` | `null` |
| `LS_FLAG` | Inclusion in lift and shift (`Y` / `N`) | `N` |
| `LS_DETAIL` | Configuration data for target systems | |
| `IS_ACTIVE` | Active indicator (`Y` / `N`) | `Y` |
| `INSERTED_BY` | Audit column | `current_user()` |
| `UPDATED_BY` | Audit column | `current_user()` |
| `INSERTED_TS` | Audit column | `current_timestamp()` |
| `UPDATED_TS` | Audit column | `current_timestamp()` |

**`DQ_LOGIC` example:**

```json
{
  "expect_or_drop": {
    "no_rescued_data": "_rescued_data IS NULL",
    "valid_id": "_rescued_data IS NULL AND ACCOUNT_MANAGER_TYPE_CDE IS NOT NULL"
  },
  "expect_or_quarantine": {
    "quarantine_rule": "_rescued_data IS NOT NULL OR ACCOUNT_MANAGER_TYPE_CDE IS NULL"
  }
}
```

**`CDC_LOGIC` example:**

```json
{
  "apply_as_deletes": "operation_column = 'DELETE'",
  "except_column_list": ["_rescued_data", "inputFilePath", "file_modification_time", "operation_column", "event_ts"],
  "keys": ["ACCOUNT_MANAGER_TYPE_CDE"],
  "scd_type": "1",
  "sequence_by": "event_ts"
}
```

> **PRESTAG_FLAG behavior:**
> - **`Y`** → Creates a DLT Streaming table in **L0 - Stage** for applying preprocessing logic, DQ rules, and CDC logic before loading into **L0 - Raw**.
> - **`N`** → Creates an external table pointing to S3 (view only) in **L0 - Stage**, and loads data per DQ rules and CDC logic directly into **L0 - Raw**.

#### Step 2b: L1 and L2 Layer

Insert the corresponding entry into the `DATA_FLOW_PB_DETAIL` metadata table.

- Define **priorities** to control sequential or parallel execution.
- Provide the correct **transformation query** to process data into the target object (Table or Materialized View).

### Step 3: Deployment

Once metadata entries are complete, refer to the [Deployment Process](#26-deployment-process) section.

## 2.3 ER Diagram for Metadata

### Metadata Tables (Admin Schema)

| Table Type | Table Name | Description |
|-----------|------------|-------------|
| **CONFIG** | `DATA_FLOW_CLUSTER_CONFIG_LOOKUP` | Pre-filled during deployment. Contains entries related to compute classes used across Databricks workloads (Workflows and DLT Pipelines). New cluster node types are added here. Managed only by the framework team. |
| **CONFIG** | `DATA_FLOW_ENV_CONFIG_LOOKUP` | Pre-filled during deployment. Contains entries for environment configs used as dynamic variables in the Databricks framework codebase. |
| **SECURITY** | `DATA_FLOW_RAW_OBJECT_SECURITY_LOOKUP` | Implements dataset-level security at the object level for CONF (L0) layer data objects. |
| **SECURITY** | `DATA_FLOW_OBJECT_SECURITY_LOOKUP` | Implements dataset / RLS / CLS security at the object level for PB layer data objects. |
| **METADATA** | `DATA_FLOW_CONTROL_HEADER` | Primary control table for the metadata-driven process. Includes a unique `Data_Flow_Group_ID`. Enables the wrapper job to handle ETL layer processing. |
| **METADATA** | `DATA_FLOW_L0_DETAIL` | Detailed information per `Data_Flow_Group_ID` for processing tables into the L0 Stage & Raw schemas. Loads data from landing S3 to raw tables. |
| **METADATA** | `DATA_FLOW_PB_DETAIL` | Detailed information per `Data_Flow_Group_ID` for processing tables within the PB layer (L1 / L2). Loads data from CONF Layer to PB Layer with both Technical and Business object schemas. |
| **TRACKER** | `DATA_FLOW_JOB_REPAIR_TRACKER_TABLE` | Handles repair scenarios on workflow failures. Drives the repair strategy for a specific flow group. |

### Sample Queries

#### `DATA_FLOW_CONTROL_HEADER`

```sql
INSERT INTO tedatacatalog_dev.admin.data_flow_l0_detail (
    DATA_FLOW_GROUP_ID, TRIGGER_TYPE, ETL_LAYER, COMPUTE_CLASS_DEV, COMPUTE_CLASS,
    IS_ACTIVE, INSERTED_BY, UPDATED_BY, INSERTED_TS, UPDATED_TS,
    BUSINESS_OBJECT_NAME, COST_CENTER, DATA_SME, BUSINESS_UNIT, PRODUCT_OWNER,
    INGESTION_MODE, INGESTION_BUCKET, SPARK_CONFIGS, WARNING_THRESHOLD_MINS,
    WARNING_DL_GROUP, min_version, max_version
) VALUES (
    'MASTER_DATA_INDUSTRY_BUSINESS_L0', 'DLT', 'L0', 'Serverless', 'Serverless',
    'Y', 'piyush.mehta@te.com', 'piyush.mehta@te.com',
    '2025-08-13 14:00:25.795804', '2025-08-13 14:00:25.797474',
    NULL, NULL, NULL, NULL, NULL,
    'DB_INGEST', 'eda', NULL, '180',
    'ted_simplification_data_team', '0.10', '0.10'
);
```

#### `DATA_FLOW_L0_DETAIL`

```sql
INSERT INTO tedatacatalog_dev.admin.data_flow_l0_detail (
    DATA_FLOW_GROUP_ID, SOURCE, SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME, LOB,
    LOAD_TYPE, INPUT_FILE_FORMAT, STORAGE_TYPE, DQ_LOGIC, DELIMETER,
    CUSTOM_SCHEMA, CDC_LOGIC, TRANSFORM_QUERY, PRESTAG_FLAG, PARTITION,
    LS_FLAG, LS_DETAIL, IS_ACTIVE, INSERTED_BY, UPDATED_BY,
    INSERTED_TS, UPDATED_TS
) VALUES (
    'MASTER_DATA_INDUSTRY_BUSINESS_L0', '', 'GBL_CURRENT',
    'gbl_industry_business_test', 'master-data', 'DELTA', 'parquet', 'R',
    '{ "expect_or_drop": { "no_rescued_data": "_rescued_data IS NULL",
       "valid_id": "_rescued_data IS NULL AND GIB_INDUSTRY_BUSINESS_CODE IS NOT NULL ..." },
       "expect_or_quarantine": { "quarantine_rule": "_rescued_data IS NOT NULL OR ..." } }',
    NULL, NULL,
    '{ "apply_as_deletes": "operation_column = ''DELETE''",
       "except_column_list": ["_rescued_data", "inputFilePath", "file_modification_time",
                              "operation_column", "event_ts"],
       "keys": ["GIB_INDUSTRY_BUSINESS_CODE"], "scd_type": "1", "sequence_by": "event_ts" }',
    '*', 'Y', NULL, 'N', NULL, 'Y',
    NULL, NULL, NULL, NULL
);
```

#### `DATA_FLOW_PB_DETAIL` — L1 example

```sql
INSERT INTO tedatacatalog_dev.admin.data_flow_pb_detail (
    DATA_FLOW_GROUP_ID, LOB, TARGET_OBJ_SCHEMA, TARGET_OBJ_NAME, PRIORITY,
    TARGET_OBJ_TYPE, TRANSFORM_QUERY, GENERIC_SCRIPTS, SOURCE_PK, TARGET_PK,
    LOAD_TYPE, IS_ACTIVE, LS_FLAG, LS_DETAIL, PARTITION_OR_INDEX,
    INSERTED_BY, UPDATED_BY, INSERTED_TS, UPDATED_TS,
    CUSTOM_SCRIPT_PARAMS, PARTITION_METHOD, RETENTION_DETAILS
) VALUES (
    'MASTER_DATA_INDUSTRY_BUSINESS_TECHNICAL_L1', 'master-data',
    'master_data_l1_curated', 'stg_industry_business_test', '1', 'TABLE',
    'select w.gib_industry_business_code as industry_business_code,
            w.gib_long_name as industry_business_code_name,
            ...
     from master_data_l0_raw.gbl_current__gbl_industry_business_test w
     left outer join master_data_l0_raw.gbl_current__gbl_mge_profit_centers_test_1 mpc
       on w.gib_hyperion_code = mpc.mge_profit_center_abbr_nm',
    NULL, NULL, NULL, 'FULL', 'Y', 'N', NULL, NULL,
    'piyush.mehta@te.com', 'piyush.mehta@te.com',
    '2025-08-21 10:53:25.445845', '2025-08-21 10:53:25.446965',
    NULL, NULL, NULL
);
```

#### `DATA_FLOW_PB_DETAIL` — L2 example

```sql
INSERT INTO tedatacatalog_dev.admin.data_flow_pb_detail (
    DATA_FLOW_GROUP_ID, LOB, TARGET_OBJ_SCHEMA, TARGET_OBJ_NAME, PRIORITY,
    TARGET_OBJ_TYPE, TRANSFORM_QUERY, ...
) VALUES (
    'MASTER_DATA_INDUSTRY_BUSINESS_BUSINESS_L2', 'master-data',
    'master_data_l2_data_product', 'dimension_industry_business_current_test',
    '1', 'MV',
    'select industry_business_code, industry_business_name, ...
     from master_data_l1_curated.dim_industry_business_test
     where record_active_indicator = ''Y''',
    NULL, NULL, NULL, 'DELTA', 'Y', 'N', NULL, NULL,
    'piyush.mehta@te.com', 'piyush.mehta@te.com',
    '2025-08-21 10:47:04.229744', '2025-08-21 10:47:04.231496',
    NULL, NULL, NULL
);
```

#### `DATA_FLOW_RAW_OBJECT_SECURITY_LOOKUP`

```sql
INSERT INTO tedatacatalog_dev.admin.data_flow_raw_object_security_lookup (
    DATA_FLOW_GROUP_ID, TARGET_OBJ_SCHEMA, TARGET_OBJ_NAME, SECURITY_GROUP,
    INSERTED_BY, UPDATED_BY, INSERTED_TS, UPDATED_TS
) VALUES (
    'DFG_TEST', 'direct_sales_l0_raw', 'sales_details', 'industrial_segment_grp',
    current_user(), current_user(), current_timestamp(), current_timestamp()
);
```

#### `DATA_FLOW_OBJECT_SECURITY_LOOKUP`

```sql
INSERT INTO tedatacatalog_dev.admin.data_flow_object_security_lookup (
    DATA_FLOW_GROUP_ID, TARGET_OBJ_SCHEMA, TARGET_OBJ_NAME, SECURITY_GROUP,
    ROW_SECURITY_FLAG, INSERTED_BY, UPDATED_BY, INSERTED_TS, UPDATED_TS,
    PII_COLUMN_SECURITY_LIST, COST_COLUMN_SECURITY_LIST
) VALUES (
    'DFG_TEST', 'direct_sales', 'direct_sales_details_with_cost',
    'industrial_segment_grp', 'Y',
    current_user(), current_user(), current_timestamp(), current_timestamp(),
    NULL, NULL
);
```

## 2.4 Naming Standards

### 2.4.1 S3 Naming Standards

**Format:**

```
aws-te-{aws_account}-{platform}-{optional:bu/coe_name}-{Layer}-{LOB/Data_Area}-{Data_classification}-{Env}
```

| Token | Allowed Values |
|-------|----------------|
| `{aws_account}` | `eda` \| `ss` \| `ds` |
| `{platform}` | `onedata` |
| `{optional: bu/coe name}` | `dnd` \| `digital` (for self-service) |
| `{Layer}` | `landing` \| `stage` \| `l0-raw` \| `l1-curated` \| `l2-data-product` |
| `{LOB/Data Area}` | `master-data` \| `direct-sales` etc. |
| `{Data classification}` | `c1` \| `c2` \| `c3` \| `c4` |
| `{Env}` | `dev` \| `qa` \| `prod` (Prod has no ENV suffix in the S3 bucket name) |

#### Approval Flow Design — DevOps for S3 Buckets and Databricks Schemas

A multi-stage approval workflow is required for DevOps to create S3 buckets and Databricks schemas:

1. **Data Owner Approval** — Project team validates and confirms data classification with the Data Owner.
2. **Secondary Approval — Rajagopal Radhakrishnan** — Confirms compliance with naming standards and business alignment.
3. **Final Approval — Partiban** — Authorizes Databricks schema creation based on the above approvals.

Once all approvals are secured, the DevOps team provisions resources through Infrastructure-as-Code (Terraform for S3, Databricks provider for schemas), ensuring encryption, access policies, tagging, and catalog registration.

### 2.4.2 Schema Naming Convention

**Format:** `<LOB>_<Abbr_ETL_Layer>`

- `<LOB>` — Line of Business
- `<Abbr_ETL_Layer>` — `Stage`, `L0`, `L1`, `L2`

**Example: Direct Sales as LOB**

| Layer | Schema Name |
|-------|-------------|
| Stage | `direct_sales_stage` |
| L0-Raw | `direct_sales_l0_raw` |
| L1-Curated | `direct_sales_l1_curated` |
| L2-Consume / Data Product | `direct_sales_l2_Data_Product` |

### 2.4.3 Data Flow Group ID

**Format:** `<LOB>_<Abbr_Dataset_Name>_<Subset_desc>_<Abbr_ETL_Layer>`

- `<LOB>` — Line of Business
- `<Abbr_Dataset_Name>` — Abbreviated dataset name without `fact` or `dim`; add abbreviation if needed.
- `<Subset_desc>` — `<source>` or `<step>_<desc>`; optional for multiple jobs populating the same target or different databases.
- `<Abbr_ETL_Layer>` — `UTIL`, `L0`, `L1`, `L2`.

**Example: Direct Sales as LOB and Daily News COSB as Dataset Name**

| Layer | Data Flow Group ID |
|-------|--------------------|
| Stage | Not Required |
| L0-Raw | `DIRECT_SALES_DAILY_NEWS_COSB_L0` |
| L1-Curated | `DIRECT_SALES_DAILY_NEWS_COSB_L1` |
| L2-Consume / Data Product | `DIRECT_SALES_DAILY_NEWS_COSB_L2` |

> **Note:** Data Flow Group ID name must not exceed **44 characters**.

### 2.4.4 Workflows and DLT Pipelines

Standards applied for deployments based on `DATA_FLOW_GROUP_ID`:

- **ETL Workflow Name:** `DBX_<DATA_FLOW_GROUP_ID>_JOB`
- **DLT Pipeline Name:** `DBX_<DATA_FLOW_GROUP_ID>_DLT`

> **Note:** A DLT Pipeline is created only when the data flow includes an `L0_RAW` layer, or when the L1 / L2 layers consist exclusively of materialized views.

**Example:** If `DATA_FLOW_GROUP_ID = MASTER_DATA_PRODUCT_L0_RAW`:

- **ETL Workflow Name:** `DBX_MASTER_DATA_PRODUCT_L0_RAW_JOB`
- **DLT Pipeline Name:** `DBX_MASTER_DATA_PRODUCT_L0_RAW_DLT`

## 2.5 Decision Tree Between L1 & L2

### L1 — Curated Layer

**Purpose:** Stores system-level, foundational, and technically curated datasets. Not intended for direct downstream consumption by Business Units.

**Characteristics and Guidelines:**

- Data is **cleansed, curated, and enriched**.
- Includes **Slowly Changing Dimensions (SCD) Type 1 & Type 2** handling.
- Maintains **raw yet structured business entities** (facts and master data in curated form).
- Persisted as **tables / MVs** (normalized or semi-flattened as needed).

**Examples:**

- **Customer Master Dimension:** L1 stores SCD1 (overwrite for corrections) and SCD2 (history tracking) as separate curated tables.
- **Sales Fact Data:** Stored with cleanup and KPIs, but not fully flattened with master data.

### L2 — Data Product Layer

**Purpose:** Stores business-facing, consumable data models aligned to domains, data marts, or analytics requirements.

**Characteristics:**

- **Flattened, aggregated, and business-ready** data.
- Includes **derived attributes, KPIs, and business rules**.
- Optimized for **reporting, dashboards, and analytics**.
- Implemented as **tables / MVs** (transactional data) or **views** (master data).

**Examples:**

- **Sales Data Mart:** L2 table with transactional sales fact data flattened with customer, product, and other master dimensions from L1.
- **Customer Master (Business Consumption):** L2 view on top of L1 curated SCD2 table, exposing current + historical view for reporting.

### Key Difference: Transactional vs Master Data

| Data Type | L2 Implementation |
|-----------|-------------------|
| Transactional Data | Flattened **Tables / MVs** in L2 |
| Master Data | Flattened **Views** in L2 |

> **Note:** Transactional data may also expose **views** in L2 if needed.

### Decision Tree

1. **Is the dataset technical, foundational, or not directly needed by business users?**
   - **Yes** → Store in **L1 — Curated** (e.g., Customer Master with SCD1 & SCD2).
   - **No** → Proceed to step 2.
2. **Is the dataset meant for direct business consumption, analytics, or dashboards?**
   - **Yes** → Store in **L2 — Product**.
     - **Transactional Data** → Flatten into **Table**.
     - **Master Data** → Expose as **View** on top of L1 curated.
   - **No** → Keep in L1 until further enrichment.

> **Note:** Choosing the right persistence layer reduces overall process execution time and saves costs.

## 2.6 Deployment Process

### 2.6.1 CICD Pipelines Overview

These pipelines are managed by Cloudbees and are available for Dev, QA, and Prod deployments.

| Pipeline Type | Pipeline Name | Description |
|---------------|---------------|-------------|
| Deployment | `TE-EDA-Databricks-ETL-Deployment-Pipeline` | Deploys Metadata and Workflows / Pipelines from the Databricks environment (Dev → QA → Prod). Inputs: **Deployment Mode** (`SPARK_SQL`, `PYSPARK_SCRIPTS`); **DATA_FLOW_GROUP_IDS** (list to deploy); **DEPLOY_TO_DATABRICKS** (select to update pipeline configuration). |
| Deployment | `TE-EDA-Databricks-Deployment-Pipeline` | Deletes any existing Data Flow Group that is no longer in use. |
| Support Action | `Databricks-Create-object-pipeline` | Creates tables, materialized views, or functions in existing schemas across environments. User uploads a `.txt` file with `CREATE` statements (one per line for multiples). |
| Support Action | `Databricks-drop-object-Pipeline` | Deletes tables, views, or materialized views from existing schemas across environments. |
| Support Action | `Databricks-support-pipeline` | Provides: **Cancel Databricks job run** (by run id), **Cancel Databricks pipeline run** (by pipeline id), **Insert/Update job repair tracker table**, **Drop from job repair tracker table**. |
| Support Action | `Databricks-Manual-Job-Run-Pipeline` | Runs a specific `Data_Flow_Group_id`; alternative to Control-M for Development and QA testing. |

**Demo Link:** [20241004 - Session 4 - CICD](https://te360.sharepoint.com/:f:/r/sites/TEDSimplification/Shared%20Documents/General/15%20-%20Databricks%20Onboarding/DBX%20-%20Framework%20Trainings/20241004%20-%20Session%204%20-%20CICD)

## 2.7 CICD — Cloudbees Deployment Pipeline

### 2.7.1 Deployment Pipeline for Development Environment

**`TE-EDA-Databricks-ETL-Deployment-Pipeline`** — used for deploying Metadata and Workflows / Pipelines from the Databricks environment.

**Input Parameters (Development):**

- **Deployment Mode:** `SPARK_SQL`, `PYSPARK_SCRIPTS`, `GENERIC_SCRIPTS`
  - **`SPARK_SQL`** — Generates SQL query in Dev and stores it in Git for use in subsequent QA / Prod deployments.
  - **`PYSPARK_SCRIPTS` / `GENERIC_SCRIPTS`** — Copies the PySpark script from the feature branch to the Dev environment Git location.

> **Note:** Default deployment mode is `SPARK_SQL`; use other modes only for specific purposes.

- **DATA_FLOW_GROUP_IDS:** List of Data Flow Group IDs to deploy.
- **DEPLOY_TO_DATABRICKS:** Select to update pipeline configuration (e.g., updating cluster type, adding new tasks). Unselect for metadata-only changes.

**Deployment Steps:**

Open the ETL Deployment Pipeline: [TE-EDA-Databricks-ETL-Deployment-Pipeline](https://dia-cloudbees-dev.tycoelectronics.net/job/ONEDATA/job/DATABRICKS-DEV/job/ETL/job/TE-EDA-Databricks-ETL-Deployment-Pipeline/)

> **Note:** Step-2 differs based on deployment mode selection, which controls feature branch selection and passing generic scripts.

**Monitoring:**

1. Go to the pipeline overview or build history section to find the latest build status.
2. Click on the specific build (e.g., `#256`) to monitor.
3. The **console output** section provides processing status and failure information.

### 2.7.2 Deployment Pipeline for QA / Prod Environment

**`TE-EDA-Databricks-ETL-Deployment-Pipeline`** — same pipeline as Dev but under a different Cloudbees folder. The deployment mode behavior differs:

- **`SPARK_SQL`:**
  - **QA** — Checks the Dev branch Metastore folder; deploys to QA Databricks workspace.
  - **Prod** — Checks the QA branch Metastore folder; deploys to Prod Databricks workspace.
- **`PYSPARK_SCRIPTS` / `GENERIC_SCRIPTS`** — Same as `SPARK_SQL`; no feature_branch available. Copies the PySpark script from the lower environment to the current environment.

> **Note:** Default deployment mode is `SPARK_SQL`.

**PROD Pipeline:** Managed by DevOps and Support team. After CCB approval, the developer submits details to the support team for ETL deployment in PROD.

## 2.8 Job Execution

### 2.8.1 Job Execution Through Cloudbees

**`Databricks-Manual-EC2-Wrapper-Job-Run-Pipeline`** — executed manually with the parameters below.

**Input Parameters:**

| Parameter | Required | Description |
|-----------|:--------:|-------------|
| **Data Flow Group ID** | Mandatory | Single DFG to run, e.g., `DFG_L0` or `DFG_L1`. |
| **Lift Shift Mode** | Optional | `LS_ONLY` or `LOAD_LS`. Default: blank. |
| **Target Load Table** | Optional | For PB, store data into a dynamic / new table. |
| **Query Filter** | Optional | For PB, apply a dynamic query condition (e.g., `where BU = 'Industrial'`). |

**Dev Pipeline Execution:**

Go to [Databricks-Manual-EC2-Wrapper-Job-Run-Pipeline](https://dia-cloudbees-dev.tycoelectronics.net/job/ONEDATA/job/DATABRICKS-DEV/job/SUPPORT/job/Databricks-Manual-EC2-Wrapper-Job-Run-Pipeline/).

After Step-3, a build is created (e.g., `#406`). Click on it to monitor. Internally, this pipeline triggers the Wrapper Orchestrator Job in Databricks, which executes the corresponding `Data_Flow_Group_Id`-based pipeline.

### 2.8.2 Job Execution Through Control-M

> **Currently, integration with Control-M is in progress.**

The framework wrapper script located on the job server is called from Control-M. The Control-M flow calls a wrapper shell script which initiates Databricks job execution internally.

- **Folder Name:** As per naming standards.
- **Job Name:** As per naming standards.
- **Job Type:** OS

**QA command line:**

```bash
sh /opt/eda-qa/databricks_framework/execute_dbx_framework.sh \
'{"data_flow_group_id":"MASTER_DATA_DUMMY_DATA_L0_RAW","environment":"QA","lift_shift_mode":"","target_object_name":"","query_filter":""}'
```

> **Note:** Parameters passed to the Databricks wrapper workflow must be specified as a JSON string in the command line.

**Sample JSON String:**

```json
{
  "data_flow_group_id": "MASTER_DATA_DUMMY_DATA_L0_RAW",
  "environment": "QA",
  "lift_shift_mode": "",
  "target_load_table": "",
  "query_filter": ""
}
```

**Parameters:**

| # | Name | Required | Allowed Values |
|---|------|:--------:|----------------|
| 1 | `data_flow_group_id` | Mandatory | Any valid DFG ID |
| 2 | `environment` | Mandatory | `DEV`, `QA`, `PROD` |
| 3 | `lift_shift_mode` | Optional | `LS_ONLY`, `LOAD_LS`, `LOAD`, `""` |
| 4 | `target_load_table` | Optional | For dynamic target table |
| 5 | `query_filter` | Optional | For dynamic query use case |

## 2.9 Multiple SAP Systems Based Processing and Deployment

Reference document: [Multiple SAP System Guide](https://te360.sharepoint.com/:w:/r/sites/TE-DIA/Shared%20Documents/DIA%20Architecture/01%20-%20Architecture%20Decisions/04%20-%20Reference%20Documents/OneData%20Architecture%20Docs/Architecture%20Guidelines/Data%20Processing/Multiple%20SAP%20System%20Guide.docx).

---

# 3. Logging, Monitoring and Troubleshooting

## 3.1 Execution & Troubleshooting

The data flow starts with either the Control-M or Cloudbees pipeline:

1. The Control-M or Cloudbees pipeline initiates the wrapper task with parameters such as `DATA_FLOW_GROUP_ID`.
2. The wrapper task references the control header table to determine whether to trigger a DLT or non-DLT task, and whether it should be a repair run or fresh run.
3. The next step executes the DLT or non-DLT tasks.

### Monitoring the DLT Processor Job

The DLT processor job is initiated by the wrapper job. Access it directly from the DBX workspace by navigating to the Jobs section.

**Parameters sent by the wrapper job:**

- `Data_flow_group_id`
- `Environment`
- `Lift_shift_mode` (optional: empty, `ls_only`, or `load_ls`)

**The DLT processor job consists of four main tasks:**

1. **If-else condition** — performs a lift and shift operation on specified tables if true.
2. **Primary task** — executes the DLT run logic; determines which tables to process, repair, etc.
3. **If-else condition** — decides whether a lift and shift operation should be conducted post-data load.
4. **Lift and shift notebook** — handles credentials setup and data movement.

Focus monitoring primarily on the **second task (`DBX_DLT_PROCESSOR`)**. The console output shows tables marked for `FULL_LOAD` and `DELTA_LOAD`, tables excluded due to ongoing loads, and the DLT pipeline name and run ID.

### Workflow Components Breakdown

| Component | Description |
|-----------|-------------|
| **Control-M Job** | Overarching job orchestrating the execution flow. Initiates the entire process and ensures dependent workflows run in sequence. |
| **EC2 Wrapper Job** | Encapsulates execution of `DBX_{DATA_FLOW_GROUP_ID}_JOB` and `DBX_{DATA_FLOW_GROUP_ID}_DLT`. Acts as the control / manager. |
| **`DBX_{DATA_FLOW_GROUP_ID}_JOB`** | Core job performing data extraction, transformation, or business logic. |
| **`DBX_{DATA_FLOW_GROUP_ID}_DLT`** | Final task performing the data loading operation (Delta Live Table Pipelines). |

> **Key Point:** Failure propagates upstream:
> - `DBX_..._DLT` failure → Wrapper Job fails.
> - EC2 Wrapper Job failure → Control-M Job fails.
> - Any failure halts the entire process.

### Monitoring Non-DLT Jobs

Non-DLT jobs primarily pertain to the publication layer (full loading from the conformance layer). They are initiated by the wrapper task with the following parameters:

| Parameter | Required |
|-----------|:--------:|
| `Data_flow_group_id` | Mandatory |
| `Environment` | Mandatory |
| `Lift_shift_mode` | Optional |
| `Query_filter` | Optional |
| `Target_load_table` | Optional |

From the wrapper console output, navigate directly to the job URL to review execution and parameter details.

## 3.2 Possible Errors and Troubleshooting for Non-DLT Jobs and Workflows

### Generic Issues and Troubleshooting Steps

| Issue | Troubleshooting |
|-------|-----------------|
| **Incorrect Parameter Configuration for Wrapper Job** | Ensure Control-M or Cloudbees pipeline is configured with correct parameters. |
| **Incorrect Cluster Class Selected for Workflow** | Check `tedatacatalog_dev.admin.data_flow_cluster_config_lookup` and update `tedatacatalog_dev.admin.data_flow_control_header` with appropriate cluster classes. Retry. |
| **Data Flow Group is Inactive** | Refer to `tedatacatalog_dev.admin.data_flow_control_header` and activate the required Data Flow Group. |
| **Multiple Entries for the Same Data Flow Group** | Ensure unique entries per Data Flow Group in `tedatacatalog_dev.admin.data_flow_control_header`. |
| **Multiple Workflows for the Same DFG ID** | Use unique names for workflows and DLTs to avoid conflicts. |
| **Workflow Already Running** | Avoid manually triggering workflows. If error persists, wait for the current workflow to complete. |
| **Repair Tracker Table Holding Wrong Entries** | Ensure `data_flow_job_repair_tracker_table` holds correct entries for `force_refresh_flag`, `repair_task_list`, `repair_dependent_flag`, and `data_flow_group_id`. |

> **Note:** The wrapper job does not support more than **1000 concurrent runs**.

### Possible Errors and Troubleshooting for DLT

| Issue | Troubleshooting |
|-------|-----------------|
| **Incorrect Data Flow Group ID** | Update DFG ID in `data_flow_control_header` and/or DLT metadata table. |
| **Incorrect File Path** | Verify landing zone location; ensure metadata table has accurate `source`, `source_schema`, and `lob`; check the `s3 static path context_key` in the environment config table. |
| **Incorrect DQ Logic** | Correct DQ logic to proper SQL format (or set to `null`); validate JSON formatting; confirm columns exist in the table. |
| **Incorrect CDC Logic** | For delta loads, ensure CDC logic includes keys, sequence keys, and operation columns. For full loads, these are not mandatory. |
| **Incorrect Permissions on Event Logs** | Ensure the candidate or service principal running the wrapper is the DLT owner. |

### File Handling in the Landing Zone

Supported ingestion modes (via `INGESTION_MODE` column in the control header table):

- `API_INGEST`
- `DB_INGEST`
- `DATASPHERE_INGEST`
- `EXTL_FULL`
- `EXTL_DELTA`

> For `DB_INGEST` and `API_INGEST` modes: if all files are full-load type and no trigger file is present, the framework will skip those tables. This is by design.

## 3.3 Restart Capabilities

Modes of restart for Dataflow groups that previously failed:

| Mode | Behavior |
|------|----------|
| **Repair Run** (Default) | Automatically addresses the point of failure; minimal disruption. |
| **Forceful Refresh Run** | Bypasses repair; starts the workflow fresh. Mark as `Y` if required, else `N`. |
| **Repair Specific Tasks** | Specifies which tasks to repair. Leave empty when using forceful refresh. |
| **Repair Dependent Tasks Flag** | Repairs tasks that depend on the provided list. Mark `Y` / `N`; leave empty for forceful refresh. |

Track repairs and modifications by inserting records into the `job_repair_tracker_table` using the [Databricks Support Pipeline](https://dia-cloudbees-dev.tycoelectronics.net/job/ONEDATA/job/DATABRICKS-QA/job/SUPPORT/job/Databricks-support-pipeline/build?delay=0sec).

## 3.4 Support Pipelines (Cloudbees)

### 3.4.1 Databricks Support Pipeline

Functionality provided:

- **Cancel Databricks job run** — kill / cancel a Databricks job by run ID.
- **Cancel Databricks pipeline run** — stop a Databricks pipeline run by pipeline ID.
- **Insert / Update job repair tracker table** — provide Data Flow Group ID, force refresh flag, repair dependent flag, and repair task list.
- **Drop from job repair tracker table** — delete an entry by Data Flow Group ID.

**Pipeline Inputs:**

| Input | Required | Description |
|-------|:--------:|-------------|
| `Ticket_number` | Mandatory | JIRA or ServiceNow ticket ID. |
| `Task` | Mandatory | Selected action (cancel job run, cancel pipeline run, insert/update tracker, drop from tracker). |
| Task-specific inputs | Varies | E.g., Job Run ID, Pipeline ID, DFG ID, flags. |
| `Comments` | Optional | Appropriate comments for the task. |

### 3.4.2 Databricks-DML-Object-Pipeline

Creates tables, views, functions, or materialized views in existing schemas across environments.

**Dev Pipeline:** [Databricks-DML-Object-Pipeline](https://dia-cloudbees-dev.tycoelectronics.net/job/ONEDATA/job/DATABRICKS-DEV/job/SUPPORT/job/Databricks-DML-Object-Pipeline/)

**Requirements:**

- Upload a `.txt` file containing `CREATE` statements.
- For multiple statements, separate each with a new line.
- Each statement must end with a semicolon.
- File must not be empty.

**Example file content:**

```sql
CREATE OR REPLACE TABLE tedatacatalog_dev.experiments.Persons (
    PersonID int, LastName varchar(255), FirstName varchar(255),
    Address varchar(255), City varchar(255)
);

CREATE OR REPLACE MATERIALIZED VIEW tedatacatalog_dev.experiments.Persons1_mv AS
SELECT * FROM tedatacatalog_dev.experiments.Persons;

CREATE OR REPLACE TABLE tedatacatalog_dev.experiments.Persons3 (
    PersonID int, LastName varchar(255), FirstName varchar(255),
    Address varchar(255), City varchar(255)
);
```

### 3.4.3 Databricks-Metadata-Operation-Pipeline

Deletes tables, views, or materialized views from existing schemas across environments.

**Dev Pipeline:** [Databricks-Metadata-Operation-Pipeline](https://dia-cloudbees-dev.tycoelectronics.net/job/ONEDATA/job/DATABRICKS-DEV/job/SUPPORT/job/Databricks-Metadata-Operation-Pipeline/build?delay=0sec)

**Requirements:**

- JIRA or ServiceNow ticket (mandatory).
- Select object type (`Table`, `MV`, `View`).
- Provide schema and table details (e.g., `department.employees`).
- For multiple tables, separate with commas.

---

# 4. Control-M Job Creation and Execution (QA)

## Naming Standards — Databricks

| Item | Pattern | Example |
|------|---------|---------|
| **Schedule / Folder Name** | `<SERVER_NAME>#DBXS_<LOB><Subject_Area>_<Additional_Description>` | `AW01TALAPPD002#DBXS_MASTER_DATA_CUSTOMER_DLY` |
| **Job Name** | `<SERVER_NAME>#DBXU_<DATA_FLOW_GROUP_ID>` | `AW01TALAPPD002#DBXU_MASTER_DATA_CUSTOMER_CUR_RAW` |

## 4.1 Control-M Job Creation Using UI

| Step | Action |
|------|--------|
| 1 | Create a new workspace. |
| 2 | Drag a folder from the workspace panel. |
| 3 | Select the Control-M server from the drop-down. |
| 4 | Name the folder per naming convention. |
| 5 | Drag an OS job type from the workspace panel into the folder. |
| 6 | Name the job per standards; add a description. |
| 7 | In the **What** section, select the **Command** option and pass the command shown below. |
| 8 | Add the Talend AWS server in Host Group and the Talend username in Run As User. |
| 9 | Set the schedule in the **Scheduling** tab. |
| 10 | Set prerequisites in the **Prerequisite** tab (if required). |
| 11 | Configure notifications in the **Action** tab. |

**Sample Dev command:**

```bash
sh /opt/eda-dev/databricks_framework/execute_dbx_framework.sh \
'{"data_flow_group_id":"<data_flow_group_id>","environment":"<env>","lift_shift_mode":"","target_load_table":"","query_filter":""}'
```

**Example with values:**

```bash
sh /opt/eda-dev/databricks_framework/execute_dbx_framework.sh \
'{"data_flow_group_id":"PURCHASING_SFDC_CASE_RAW","environment":"DEV","lift_shift_mode":"","target_load_table":"","query_filter":""}'
```

**ServiceNow Ticket Creation on Failure:**

1. Go to **Actions** → **Do Actions**.
2. **On** dropdown → select `Job ended not ok`.
3. **Do** dropdown → select `Notify`.
4. **Destination** → `create_ticket`.
5. Type a custom message in the message box.
6. Set the urgency from the dropdown.

**Job Dependency (Invoke another job on success):**

1. **On** dropdown → `Job ended ok`.
2. **Do** dropdown → `Order job`.
3. Provide the folder and job name to invoke.

## 4.2 Deployment Process to PROD

Once the Control-M job has been created and tested in Dev and QA, request the Control-M support team to migrate the job from QA to PROD.

Follow the instructions in [Requesting Control-M Job Creation Process](https://te360.sharepoint.com/:w:/r/sites/DIA-EDGE/_layouts/15/Doc.aspx?sourcedoc=%7BBC7D2E2A-7FDB-4B14-B704-38198A82EDF6%7D&file=Requesting%20Control-M%20Job%20Creation%20Process.docx&action=default&mobileredirect=true).

---

# 5. ETL Project Life Cycle

The following steps must be followed for a developer to publish code to Production:

1. Developer Life Cycle steps.
2. Approval Process Flow.

> *Refer to the original document for the visual flow diagrams.*

---

# 6. Do's & Don'ts

| Do | Don't |
|----|-------|
| Select appropriate cluster size based on workload. | Do not select a large cluster for small datasets, or vice versa. |
| Select `ingestion_mode` based on your input landing path for raw-layer workloads. | Do not give an `ingestion_mode` different from your folder path. |
| Ensure `Data_flow_group` is unique and follows the standard naming convention `<LOB>_<Abbr_Dataset_Name>_<Subset_desc>_<Abbr_ETL_Layer>`. | Do not use random names. Do not include `TEST` or `TESTING` when deploying to next environments. |
| Ensure the details table has appropriate entries for the respective `Data_flow_group_id`. | Do not create duplicate records. Uniqueness must hold on `source` and `source_object_name`. |
| Pass appropriate DQ logic in L0 metadata tables. | Do not pass random values; leave blank or `null` if unsure. |
| For CDC in L0 with `LOAD_TYPE = DELTA`, pass appropriate keys and CDC structure. | Do not leave `CDC_LOGIC` blank for `load_type = delta`. |
| Provide specific task / priority to ensure dependency creation for PB workflows in the metadata table. | Ensure priority reflects each stage / step. |
| In PB metadata, provide `transformation_query` entries **without** specifying `catalog_name` in queries. | Do not include `catalog_name` inside SQL transformation queries. |
| In PB metadata, when `Load_type = Pyspark`, provide the path to the transformation notebook (`.py`). | Do not provide a SQL query when selecting PySpark. |
| In PB metadata, specify `Partition_Method` as `liquid_cluster` or `partition`, and pass the column in `Partition_OR_Index`. | Do not pass anything other than `liquid_cluster` or `partition`. |
| For multiple partitions, separate with commas (`,`). | Do not give random values. |
| In PB metadata, ensure `target_object_type` is `Table`, `View`, or `MV`. | Do not pass anything except these 3 options. |
| For `target_object_type = MV` with column-level security, pass column-level details in `schema_description` separated by `||` followed by the transform query. | Ensure correct schema and column masking function. |
| Specify dataflow group ID, schema, and table name under `object_security_lookup` along with column / row-level security columns. | Do not pass column security list when `target_object_type = MV`. |

---

# 7. Guidelines for Multiple SAP Source Systems — SCD & Delta Merge

When integrating data from **multiple SAP source systems** into a shared target table, additional parameters must be configured.

## 7.1 Handling Multiple Source Systems

When loading data from more than one SAP source into the same target:

- Set `MULTIPLE_SOURCES = "Y"` in `CUSTOM_SCRIPT_PARAMS`.
- Ensures the merge logic treats each source independently.
- Prevents incorrect classification of missing records as deletes in SCD processing.

## 7.2 Delta Delete Processing

For `LOAD_TYPE = "DELTA"`, delete behavior is configured through three parameters:

### 7.2.1 Hard Delete

```text
HARD_DELETE_FLAG = "Y"
```

Records missing from the incoming source (matched by primary key and source system) are **physically deleted** from the target. Mirrors classical MERGE behavior (upserts + hard deletes).

### 7.2.2 Soft Delete

```text
SOFT_DELETE_FLAG = "Y"
SOFT_DELETE_FLAG_COLUMN = "<designated_column_name>"
```

Missing records are **not physically removed**. The designated column is set to `"Y"` to mark the record as logically deleted.

### 7.2.3 Upsert-Only Mode

If **neither** delete flag is provided, the system performs **upserts only** — no delete logic is applied.

### 7.2.4 Configuration Conflict

If **both** `SOFT_DELETE_FLAG` and `HARD_DELETE_FLAG` are set to `"Y"`, the system throws an error (only one delete strategy can be used at a time).

### 7.2.5 Multi-Source Delta Logic

For delta merges with multiple systems, comparison logic includes `source_system_id`, which must be populated from the source transformation query.

## 7.3 SCD Processing with Multiple Source Systems

- `source_system_id = 1` is the **highest-priority source**.
- On conflicting updates, changes from source system `1` take precedence.
- Ensures deterministic behavior across multi-source SCD updates.

> *Note: this logic is implemented but still undergoing validation; test cases are needed.*

## 7.4 Example Configurations

### Example: SCD Load with Multiple Sources

```text
LOAD_TYPE = "SCD"
```

```json
{
  "EXCLUDE_COLUMNS": "",
  "TIME_VARIANT_COLUMNS": "country_code",
  "IS_PME": "Y",
  "END_DATE_COLUMN": "effective_to_date",
  "START_DATE_COLUMN": "effective_from_date",
  "ACTIVE_FLAG_COLUMN": "record_active_indicator",
  "PRIMARY_KEYS": "plant_id",
  "TIME_AUDIT_COLUMNS": "record_load_timestamp,record_update_timestamp",
  "MULTIPLE_SOURCES": "Y"
}
```

### Example: Delta Load (Generic)

```text
LOAD_TYPE = "DELTA"
```

```json
{
  "MULTIPLE_SOURCES": "Y",
  "SOFT_DELETE_FLAG": "Y",
  "SOFT_DELETE_FLAG_COLUMN": "IS_DELETED",
  "HARD_DELETE_FLAG": "N"
}
```

### Delta Load — Soft Delete with Multiple Sources

```json
{
  "MULTIPLE_SOURCES": "Y",
  "SOFT_DELETE_FLAG": "Y",
  "SOFT_DELETE_FLAG_COLUMN": "IS_DELETED",
  "HARD_DELETE_FLAG": "N"
}
```

### Delta Load — Hard Delete with Multiple Sources

```json
{
  "MULTIPLE_SOURCES": "Y",
  "SOFT_DELETE_FLAG": "N",
  "HARD_DELETE_FLAG": "Y"
}
```

### Delta Load — Upsert Only

`CUSTOM_SCRIPT_PARAMS` can be set to `null` as in the current process.

---

# 8. Frequently Asked Questions (FAQs)

### Q1. What is the standard procedure when full-load counts do not match?

Compare counts from both Stage and L0 Raw (including `count(distinct id)` if duplicates are suspected). If re-running L0 results in a match with the expected Stage count, the load is accepted. If discrepancies continue, request the full file from the ingestion sources.

### Q2. How do we address resource errors when querying large stage tables?

If queries fail due to resource limitations, escalate to engineering and hold the deltas for the target object until counts stabilize and validation is completed.

### Q3. What cluster compute configuration is needed for the workflow?

Refer to `admin.data_flow_cluster_config_lookup`. Choose based on workload and required compute power. For more details, see [TE_ClusterGuidelines.pptx](https://te360.sharepoint.com/:p:/r/sites/TEDSimplification/Shared%20Documents/General/15%20-%20Databricks%20Onboarding/Databricks%20Framework%20Trainings/20250612-%20Weekly%20Sync%20-%20Cluster%20Metadata%20Changes/TE_ClusterGuideliness.pptx).

### Q4. Which metadata tables are essential for data loads in L0, L1, and L2?

| Layer | Required Metadata Table |
|-------|-------------------------|
| All layers | `admin.data_flow_control_header` (Control Header) |
| L0 | `admin.data_flow_l0_detail` |
| L1 / L2 | `admin.data_flow_pb_detail` |
| L0 Security (RLS / CLF) | `admin.data_flow_raw_object_security_lookup` |
| L1 / L2 Security (RLS / CLF) | `admin.data_flow_object_security_lookup` |

### Q5. How to automate loading a full dump (ad-hoc) when the load type is delta?

If source files are typically incremental but you need an ad-hoc full load, the source should send files with naming like `LOAD*` or with `_full_` in the name, accompanied by a `.trg` (trigger) file. This signals processing as a full load.

### Q6. Should we notify DevOps beforehand about QA deployments?

The project team manages QA deployment via Jenkins pipelines — no direct DevOps involvement is required for QA. **However, for production deployment, notify DevOps in advance and submit a Change Control (CCB) request.**

---

# 9. References

- **Databricks Demo Link:** [DBX - Framework Trainings](https://te360.sharepoint.com/:f:/r/sites/TEDSimplification/Shared%20Documents/General/15%20-%20Databricks%20Onboarding/DBX%20-%20Framework%20Trainings)
- **Databricks Development Guide:** [Databricks Development Guide V1.0](https://te360.sharepoint.com/:w:/r/sites/TEDSimplification/_layouts/15/Doc.aspx?sourcedoc=%7BD99E1E3D-0979-40AD-83B6-2BE9CC16F5DB%7D&file=Databricks%20Development%20Guide%20V1.0.docx)

---

*Document: Databricks Run-Book Gen 2 — V1.0*
