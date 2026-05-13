-- Databricks notebook source
-- DBTITLE 1,Pipeline overview
-- ============================================================================
-- AR SKU Snapshot Framework — Pipeline registration
-- ============================================================================
-- Single DFG_ID with 4 framework tasks running in strict priority order. The 5th
-- (DQ) is a `test` materialization that does NOT fit the framework's FULL/DELTA/SCD
-- LOAD_TYPE matrix — see the note below and dev notebook 05_dq_dev.sql.
--
-- DFG_ID:    FINANCE_FIN360_AR_SKU_SNAPSHOT_L1   (32 chars, max 44)
-- ETL_LAYER: PB (L1)
-- Run order (strict — enforced via PRIORITY):
--
--   Priority 1 — Task 1 (DELTA merge):  Archive stale rows to Iceberg
--                Target: iceberg_cold.fact_accounts_receivable_invoice_sku_snapshot_archive
--                MUST run first; otherwise Task 2's rebuild drops stale rows without archiving.
--
--   Priority 2 — Task 2 (FULL CTAS):    Rebuild L1 within 60-day window (excl. today)
--                Target: finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
--                Reads itself; ~9.8B rows. The "framework tax".
--
--   Priority 3 — Task 3 (DELTA merge):  Append today's slice (OPEN + cleared-today)
--                Target: finance_tbl.fact_accounts_receivable_invoice_sku_snapshot  (same as Task 2)
--                MUST run after Task 2, otherwise the rebuild wipes today's rows.
--
--   Priority 4 — Task 4 (FULL CTAS):    Rebuild open-only L2
--                Target: finance_tbl.fact_accounts_receivable_invoice_sku_snapshot_open
--                Reads the just-completed L1; ~1.5B rows.
--
-- ----------------------------------------------------------------------------
-- ITEMS TO VERIFY BEFORE RUNNING THE INSERTS BELOW:
--   [1] :as_of_date binding — confirm the framework substitutes this placeholder
--       at runtime. If not, the TRANSFORM_QUERY strings must be re-written to use
--       a framework-native parameter syntax.
--   [2] iceberg_cold catalog/schema/external location — must exist before Task 1 runs.
--   [3] Two DETAIL rows share the same TARGET_OBJ_NAME (Tasks 2 and 3 both write
--       fact_accounts_receivable_invoice_sku_snapshot). Confirm the framework
--       allows this within a single DFG_ID; if not, split into two DFG_IDs.
--   [4] uniform_iceberg table_format on Task 1's archive target — confirm framework support.
--   [5] DATA_SME / PRODUCT_OWNER — replace TODO_ placeholders below.
--   [6] COMPUTE_CLASS — copied from AR L1; the 9.8B-row Task 2 rewrite may need bigger.
--   [7] WARNING_THRESHOLD_MINS — set to 120 to match the ~50-90 min expected wall time.
--   [8] DQ (Task 5) — does NOT fit framework LOAD_TYPE. Recommended: register as a
--       separate workflow task (notebook task type), not via data_flow_pb_detail.
-- ============================================================================

-- COMMAND ----------

-- DBTITLE 1,HEADER INSERT
insert into admin.data_flow_control_header (
    DATA_FLOW_GROUP_ID
    ,TRIGGER_TYPE
    ,ETL_LAYER
    ,COMPUTE_CLASS_DEV
    ,COMPUTE_CLASS
    ,IS_ACTIVE
    ,INSERTED_BY
    ,UPDATED_BY
    ,INSERTED_TS
    ,UPDATED_TS
    ,BUSINESS_OBJECT_NAME
    ,COST_CENTER
    ,DATA_SME
    ,BUSINESS_UNIT
    ,PRODUCT_OWNER
    ,INGESTION_MODE
    ,INGESTION_BUCKET
    ,SPARK_CONFIGS
    ,WARNING_THRESHOLD_MINS
    ,WARNING_DL_GROUP
    ,MIN_VERSION
    ,MAX_VERSION
)
    values
        (
            'FINANCE_FIN360_AR_SKU_SNAPSHOT_L1'         -- DATA_FLOW_GROUP_ID
            ,'JOB'                                      -- TRIGGER_TYPE
            ,'PB'                                       -- ETL_LAYER
            ,'S_R5'                                     -- COMPUTE_CLASS_DEV (resize if needed)
            ,'M_R5_WP'                                  -- COMPUTE_CLASS     (resize for ~9.8B rewrite)
            ,'Y'                                        -- IS_ACTIVE
            ,current_user()                             -- INSERTED_BY
            ,current_user()                             -- UPDATED_BY
            ,current_timestamp()                        -- INSERTED_TS
            ,current_timestamp()                        -- UPDATED_TS
            ,'finance'                                  -- BUSINESS_OBJECT_NAME
            ,null                                       -- COST_CENTER
            ,'TODO_DATA_SME@te.com'                     -- DATA_SME (replace)
            ,'IN HOUSE'                                 -- BUSINESS_UNIT
            ,'TODO_PRODUCT_OWNER@te.com'                -- PRODUCT_OWNER (replace)
            ,'DB_INGEST'                                -- INGESTION_MODE
            ,'eda'                                      -- INGESTION_BUCKET
            ,'spark.sql.adaptive.coalescePartitions.enabled=false' -- SPARK_CONFIGS (per 00_README §Optimization)
            ,120                                        -- WARNING_THRESHOLD_MINS (~50-90 min total)
            ,'support_developer_dl'                     -- WARNING_DL_GROUP
            ,null                                       -- MIN_VERSION
            ,null                                       -- MAX_VERSION
        );

-- COMMAND ----------

-- DBTITLE 1,DETAIL INSERT — Priority 1: Task 1 archive (DELTA merge to Iceberg)
insert into admin.data_flow_pb_detail (
    DATA_FLOW_GROUP_ID
    ,LOB
    ,SOURCE
    ,TARGET_OBJ_SCHEMA
    ,TARGET_OBJ_NAME
    ,PRIORITY
    ,TARGET_OBJ_TYPE
    ,TRANSFORM_QUERY
    ,GENERIC_SCRIPTS
    ,SOURCE_PK
    ,TARGET_PK
    ,LOAD_TYPE
    ,IS_ACTIVE
    ,LS_FLAG
    ,LS_DETAIL
    ,PARTITION_OR_INDEX
    ,INSERTED_BY
    ,UPDATED_BY
    ,INSERTED_TS
    ,UPDATED_TS
    ,CUSTOM_SCRIPT_PARAMS
    ,PARTITION_METHOD
    ,RETENTION_DETAILS
    ,DEPLOYMENT_SOURCE_DFG
)
    values
        (
            'FINANCE_FIN360_AR_SKU_SNAPSHOT_L1'                                       -- DFG
            ,'finance'                                                                 -- LOB
            ,null                                                                      -- SOURCE
            ,'iceberg_cold'                                                            -- TARGET_OBJ_SCHEMA  (TODO[iceberg-location])
            ,'fact_accounts_receivable_invoice_sku_snapshot_archive'                   -- TARGET_OBJ_NAME
            ,1                                                                         -- PRIORITY
            ,'TABLE'                                                                   -- TARGET_OBJ_TYPE
            ,"
select /*+ REPARTITION(50) */
    s.*,
    cast(:as_of_date as date) as archived_on_date
from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot s
where s.snapshot_date < date_sub(cast(:as_of_date as date), 60)
"                                                                                      -- TRANSFORM_QUERY
            ,null                                                                      -- GENERIC_SCRIPTS
            ,'snapshot_date,snapshot_state,company_code,accounting_document_id,accounting_document_posting_fiscal_year_id,accounting_document_item_id,accounting_document_breakdown_item_id,billing_document_id,billing_document_item_id,material_number' -- SOURCE_PK
            ,'snapshot_date,snapshot_state,company_code,accounting_document_id,accounting_document_posting_fiscal_year_id,accounting_document_item_id,accounting_document_breakdown_item_id,billing_document_id,billing_document_item_id,material_number' -- TARGET_PK
            ,'DELTA'                                                                   -- LOAD_TYPE
            ,'Y'                                                                       -- IS_ACTIVE
            ,'N'                                                                       -- LS_FLAG
            ,null                                                                      -- LS_DETAIL
            ,'snapshot_date'                                                           -- PARTITION_OR_INDEX
            ,current_user()
            ,current_user()
            ,current_timestamp()
            ,current_timestamp()
            ,map(
                'PRIMARY_KEYS','snapshot_date,snapshot_state,company_code,accounting_document_id,accounting_document_posting_fiscal_year_id,accounting_document_item_id,accounting_document_breakdown_item_id,billing_document_id,billing_document_item_id,material_number',
                'MULTIPLE_SOURCES','N',
                'SOFT_DELETE_FLAG','N',
                'HARD_DELETE_FLAG','N'
            )                                                                          -- CUSTOM_SCRIPT_PARAMS
            ,'partition'                                                               -- PARTITION_METHOD
            ,null                                                                      -- RETENTION_DETAILS
            ,null                                                                      -- DEPLOYMENT_SOURCE_DFG
        );

-- COMMAND ----------

-- DBTITLE 1,DETAIL INSERT — Priority 2: Task 2 rebuild (FULL CTAS, framework tax)
insert into admin.data_flow_pb_detail (
    DATA_FLOW_GROUP_ID
    ,LOB
    ,SOURCE
    ,TARGET_OBJ_SCHEMA
    ,TARGET_OBJ_NAME
    ,PRIORITY
    ,TARGET_OBJ_TYPE
    ,TRANSFORM_QUERY
    ,GENERIC_SCRIPTS
    ,SOURCE_PK
    ,TARGET_PK
    ,LOAD_TYPE
    ,IS_ACTIVE
    ,LS_FLAG
    ,LS_DETAIL
    ,PARTITION_OR_INDEX
    ,INSERTED_BY
    ,UPDATED_BY
    ,INSERTED_TS
    ,UPDATED_TS
    ,CUSTOM_SCRIPT_PARAMS
    ,PARTITION_METHOD
    ,RETENTION_DETAILS
    ,DEPLOYMENT_SOURCE_DFG
)
    values
        (
            'FINANCE_FIN360_AR_SKU_SNAPSHOT_L1'
            ,'finance'
            ,null
            ,'finance_tbl'
            ,'fact_accounts_receivable_invoice_sku_snapshot'
            ,2                                                                         -- PRIORITY
            ,'TABLE'
            ,"
select /*+ REPARTITION(400, snapshot_date) */
    *
from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
where snapshot_date >= date_sub(cast(:as_of_date as date), 60)
  and snapshot_date <  cast(:as_of_date as date)
"                                                                                      -- TRANSFORM_QUERY
            ,null
            ,null                                                                      -- SOURCE_PK (FULL load)
            ,null                                                                      -- TARGET_PK (FULL load)
            ,'FULL'                                                                    -- LOAD_TYPE
            ,'Y'
            ,'N'
            ,null
            ,'snapshot_date,company_code'                                              -- PARTITION_OR_INDEX (liquid cluster)
            ,current_user()
            ,current_user()
            ,current_timestamp()
            ,current_timestamp()
            ,null                                                                      -- CUSTOM_SCRIPT_PARAMS
            ,'liquid_cluster'                                                          -- PARTITION_METHOD
            ,null
            ,null
        );

-- COMMAND ----------

-- DBTITLE 1,DETAIL INSERT — Priority 3: Task 3 append today (DELTA merge)
insert into admin.data_flow_pb_detail (
    DATA_FLOW_GROUP_ID
    ,LOB
    ,SOURCE
    ,TARGET_OBJ_SCHEMA
    ,TARGET_OBJ_NAME
    ,PRIORITY
    ,TARGET_OBJ_TYPE
    ,TRANSFORM_QUERY
    ,GENERIC_SCRIPTS
    ,SOURCE_PK
    ,TARGET_PK
    ,LOAD_TYPE
    ,IS_ACTIVE
    ,LS_FLAG
    ,LS_DETAIL
    ,PARTITION_OR_INDEX
    ,INSERTED_BY
    ,UPDATED_BY
    ,INSERTED_TS
    ,UPDATED_TS
    ,CUSTOM_SCRIPT_PARAMS
    ,PARTITION_METHOD
    ,RETENTION_DETAILS
    ,DEPLOYMENT_SOURCE_DFG
)
    values
        (
            'FINANCE_FIN360_AR_SKU_SNAPSHOT_L1'
            ,'finance'
            ,null
            ,'finance_tbl'
            ,'fact_accounts_receivable_invoice_sku_snapshot'
            ,3                                                                         -- PRIORITY (after Task 2)
            ,'TABLE'
            ,"
with snapshot_today_rows as (
    select
        cast(:as_of_date as date)                                  as snapshot_date,
        sku.accounting_document_item_clearing_status_name          as snapshot_state,
        sku.*,
        case
            when sku.accounting_document_item_clearing_date is null
             and length(sku.accounting_document_item_payment_reason_code) = 2
                then true
            else false
        end                                                        as is_disputed_flag,
        case
            when sku.accounting_document_item_clearing_status_name = 'OPEN'
             and sku.accounting_document_item_due_date >= cast(:as_of_date as date)
                then true
            else false
        end                                                        as is_current_due_flag
    from finance_tbl.fact_accounts_receivable_invoice_sku sku
    where
        sku.accounting_document_item_clearing_status_name = 'OPEN'
        or
        (sku.accounting_document_item_clearing_status_name = 'CLEARED'
         and sku.accounting_document_item_clearing_date = cast(:as_of_date as date))
)
select /*+ REPARTITION(50) */
    snapshot_date,
    snapshot_state,
    company_code,
    customer_account_number,
    accounting_document_id,
    accounting_document_posting_fiscal_year_id,
    accounting_document_item_id,
    accounting_document_type_code,
    accounting_document_create_date,
    accounting_document_posting_date,
    accounting_document_item_clearing_document_id,
    accounting_document_item_clearing_date,
    accounting_document_item_credit_or_debit_code,
    accounting_document_item_posting_key_used_in_payment_indicator,
    accounting_document_item_invoice_document_id,
    accounting_document_item_text,
    accounting_document_item_baseline_date,
    accounting_document_item_due_date,
    accounting_document_item_payment_terms_code,
    accounting_document_item_cash_discount_days_quantity_1,
    accounting_document_item_cash_discount_days_quantity_2,
    accounting_document_create_network_user_id,
    accounting_document_item_document_currency_code,
    accounting_document_item_functional_currency_code,
    accounting_document_item_payment_reason_code,
    accounting_document_item_clearing_status_name,
    ted_overriding_profit_center_id,
    profit_center_id,
    ted_profit_center_id,
    ted_profit_center_effective_date,
    accounting_document_breakdown_item_id,
    accounting_document_item_document_currency_amount,
    accouting_document_item_general_ledger_reconciliation_account_number,
    accounting_document_item_functional_currency_amount,
    accounting_document_item_actual_rate_functional_amount,
    accounting_document_item_tariff_percentage,
    data_security_tag_id,
    business_unit_group_id,
    accounting_document_item_budget_rate_amount,
    sold_to_customer_number,
    sales_organization_id,
    distribution_channel_code,
    worldwide_customer_level_1_number,
    worldwide_customer_level_1_name,
    worldwide_customer_level_2_number,
    worldwide_customer_level_2_name,
    country_region_level_1_name,
    country_region_level_2_name,
    gam_level_3_assoc_full_name,
    account_manager_level_6_assoc_full_name,
    account_manager_level_7_assoc_full_name,
    sales_territory_level_6_assoc_full_name,
    sales_territory_level_7_assoc_full_name,
    sales_office_code,
    sales_office_name,
    account_manager_assoc_full_name,
    sales_territory_assoc_full_name,
    sensors_alternate_sales_territory_code,
    sensors_alternate_sales_territory_name,
    cast(case
        when snapshot_state = 'OPEN'
            then accounting_document_item_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as receivable_open_actual_rate_functional_amount,
    cast(case
        when snapshot_state = 'OPEN' and is_disputed_flag
            then accounting_document_item_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as receivable_disputed_actual_rate_functional_amount,
    cast(case
        when snapshot_state = 'OPEN' and is_current_due_flag and not is_disputed_flag
            then accounting_document_item_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as receivable_current_due_actual_rate_functional_amount,
    cast((
        case when snapshot_state = 'OPEN'
             then accounting_document_item_actual_rate_functional_amount else 0 end
        - case when snapshot_state = 'OPEN' and is_disputed_flag
               then accounting_document_item_actual_rate_functional_amount else 0 end
        - case when snapshot_state = 'OPEN' and is_current_due_flag and not is_disputed_flag
               then accounting_document_item_actual_rate_functional_amount else 0 end
    ) as decimal(36, 3)) as receivable_past_due_actual_rate_functional_amount,
    cast(case
        when snapshot_state = 'CLEARED'
            then accounting_document_item_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as invoice_cleared_actual_rate_functional_amount,
    billing_document_id,
    billing_document_item_id,
    material_number,
    raw_material_number,
    material_description,
    material_quantity,
    material_unit_of_measure_code,
    material_billing_line_net_value,
    material_billing_line_gross_value,
    product_hierarchy_l3_code,
    product_hierarchy_l3_name,
    product_hierarchy_l4_code,
    product_hierarchy_l4_name,
    material_cbc_code,
    material_cbc_name,
    material_prorate_factor,
    billing_doc_total_net_value,
    billing_doc_line_item_count,
    material_document_currency_amount,
    material_actual_rate_functional_amount,
    cast(case
        when snapshot_state = 'OPEN'
            then material_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as material_open_functional_amount,
    cast(case
        when snapshot_state = 'OPEN' and is_disputed_flag
            then material_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as material_disputed_functional_amount,
    cast(case
        when snapshot_state = 'OPEN' and is_current_due_flag and not is_disputed_flag
            then material_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as material_current_due_functional_amount,
    cast((
        case when snapshot_state = 'OPEN'
             then material_actual_rate_functional_amount else 0 end
        - case when snapshot_state = 'OPEN' and is_disputed_flag
               then material_actual_rate_functional_amount else 0 end
        - case when snapshot_state = 'OPEN' and is_current_due_flag and not is_disputed_flag
               then material_actual_rate_functional_amount else 0 end
    ) as decimal(36, 3)) as material_past_due_functional_amount,
    cast(case
        when snapshot_state = 'CLEARED'
            then material_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as material_cleared_functional_amount
from snapshot_today_rows
"                                                                                      -- TRANSFORM_QUERY
            ,null
            ,'snapshot_date,snapshot_state,company_code,accounting_document_id,accounting_document_posting_fiscal_year_id,accounting_document_item_id,accounting_document_breakdown_item_id,billing_document_id,billing_document_item_id,material_number' -- SOURCE_PK
            ,'snapshot_date,snapshot_state,company_code,accounting_document_id,accounting_document_posting_fiscal_year_id,accounting_document_item_id,accounting_document_breakdown_item_id,billing_document_id,billing_document_item_id,material_number' -- TARGET_PK
            ,'DELTA'                                                                   -- LOAD_TYPE (merge)
            ,'Y'
            ,'N'
            ,null
            ,'snapshot_date,company_code'                                              -- PARTITION_OR_INDEX
            ,current_user()
            ,current_user()
            ,current_timestamp()
            ,current_timestamp()
            ,map(
                'PRIMARY_KEYS','snapshot_date,snapshot_state,company_code,accounting_document_id,accounting_document_posting_fiscal_year_id,accounting_document_item_id,accounting_document_breakdown_item_id,billing_document_id,billing_document_item_id,material_number',
                'MULTIPLE_SOURCES','N',
                'SOFT_DELETE_FLAG','N',
                'HARD_DELETE_FLAG','N'
            )                                                                          -- CUSTOM_SCRIPT_PARAMS
            ,'liquid_cluster'                                                          -- PARTITION_METHOD
            ,null
            ,null
        );

-- COMMAND ----------

-- DBTITLE 1,DETAIL INSERT — Priority 4: Task 4 open L2 (FULL CTAS)
insert into admin.data_flow_pb_detail (
    DATA_FLOW_GROUP_ID
    ,LOB
    ,SOURCE
    ,TARGET_OBJ_SCHEMA
    ,TARGET_OBJ_NAME
    ,PRIORITY
    ,TARGET_OBJ_TYPE
    ,TRANSFORM_QUERY
    ,GENERIC_SCRIPTS
    ,SOURCE_PK
    ,TARGET_PK
    ,LOAD_TYPE
    ,IS_ACTIVE
    ,LS_FLAG
    ,LS_DETAIL
    ,PARTITION_OR_INDEX
    ,INSERTED_BY
    ,UPDATED_BY
    ,INSERTED_TS
    ,UPDATED_TS
    ,CUSTOM_SCRIPT_PARAMS
    ,PARTITION_METHOD
    ,RETENTION_DETAILS
    ,DEPLOYMENT_SOURCE_DFG
)
    values
        (
            'FINANCE_FIN360_AR_SKU_SNAPSHOT_L1'
            ,'finance'
            ,null
            ,'finance_tbl'
            ,'fact_accounts_receivable_invoice_sku_snapshot_open'
            ,4                                                                         -- PRIORITY (after L1 chain)
            ,'TABLE'
            ,"
select /*+ REPARTITION(200, snapshot_date) */
    *
from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
where snapshot_state = 'OPEN'
"                                                                                      -- TRANSFORM_QUERY
            ,null
            ,null                                                                      -- SOURCE_PK (FULL load)
            ,null                                                                      -- TARGET_PK (FULL load)
            ,'FULL'                                                                    -- LOAD_TYPE
            ,'Y'
            ,'N'
            ,null
            ,'snapshot_date,company_code'                                              -- PARTITION_OR_INDEX
            ,current_user()
            ,current_user()
            ,current_timestamp()
            ,current_timestamp()
            ,null
            ,'liquid_cluster'                                                          -- PARTITION_METHOD
            ,null
            ,null
        );

-- COMMAND ----------

-- DBTITLE 1,Note on Task 5 (DQ) — NOT registered as framework DETAIL
-- The DQ script 05_dq_ar_sku_snapshot.sql has materialization = `test`. It is a SELECT
-- that must return 0 rows for a healthy run; on non-empty result the orchestrator fails
-- the pipeline. This does NOT map to the framework's FULL / DELTA / SCD LOAD_TYPE matrix
-- (those all WRITE to a target table; a `test` writes nothing).
--
-- Recommended deployment options:
--   A) Add a separate Databricks workflow task that runs notebook 05_dq_dev.sql after
--      the framework job completes. Fail the task if the result is non-empty.
--   B) Wrap the DQ SELECT in a CTAS to a dq_runs audit table — that DOES fit LOAD_TYPE = FULL
--      and would give an auditable history. The 00_README explicitly notes there is no
--      dq_runs table in this iteration; adding one is a small extension.
-- Either way, do NOT register the DQ SELECT in data_flow_pb_detail as written.

-- COMMAND ----------

-- DBTITLE 1,SECURITY LOOKUP INSERTs — one per distinct target object
-- Inherit ROW_SECURITY_FLAG / BU_SECURITY_FLAG / SECURITY_GROUP / PII / cost-column
-- lists from the existing AR L1 row. Verify with the SELECT below before running.
--
--   select * from admin.data_flow_object_security_lookup
--   where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_L1'
--     and TARGET_OBJ_NAME    = 'fact_accounts_receivable_invoice';

-- Security row for the L1 snapshot table (targets of Tasks 2 and 3 — same table, single row)
insert into admin.data_flow_object_security_lookup (
    DATA_FLOW_GROUP_ID
    ,TARGET_OBJ_SCHEMA
    ,TARGET_OBJ_NAME
    ,SECURITY_GROUP
    ,ROW_SECURITY_FLAG
    ,BU_SECURITY_FLAG
    ,INSERTED_BY
    ,UPDATED_BY
    ,INSERTED_TS
    ,UPDATED_TS
    ,PII_COLUMN_SECURITY_LIST
    ,COST_COLUMN_SECURITY_LIST
)
select
    'FINANCE_FIN360_AR_SKU_SNAPSHOT_L1'                  as DATA_FLOW_GROUP_ID
    ,'finance_tbl'                                        as TARGET_OBJ_SCHEMA
    ,'fact_accounts_receivable_invoice_sku_snapshot'      as TARGET_OBJ_NAME
    ,SECURITY_GROUP
    ,ROW_SECURITY_FLAG
    ,BU_SECURITY_FLAG
    ,current_user()                                       as INSERTED_BY
    ,current_user()                                       as UPDATED_BY
    ,current_timestamp()                                  as INSERTED_TS
    ,current_timestamp()                                  as UPDATED_TS
    ,PII_COLUMN_SECURITY_LIST
    ,COST_COLUMN_SECURITY_LIST
from admin.data_flow_object_security_lookup
where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_L1'
  and TARGET_OBJ_NAME    = 'fact_accounts_receivable_invoice';

-- Security row for the L2 open-only table (Task 4 target)
insert into admin.data_flow_object_security_lookup (
    DATA_FLOW_GROUP_ID
    ,TARGET_OBJ_SCHEMA
    ,TARGET_OBJ_NAME
    ,SECURITY_GROUP
    ,ROW_SECURITY_FLAG
    ,BU_SECURITY_FLAG
    ,INSERTED_BY
    ,UPDATED_BY
    ,INSERTED_TS
    ,UPDATED_TS
    ,PII_COLUMN_SECURITY_LIST
    ,COST_COLUMN_SECURITY_LIST
)
select
    'FINANCE_FIN360_AR_SKU_SNAPSHOT_L1'                       as DATA_FLOW_GROUP_ID
    ,'finance_tbl'                                             as TARGET_OBJ_SCHEMA
    ,'fact_accounts_receivable_invoice_sku_snapshot_open'      as TARGET_OBJ_NAME
    ,SECURITY_GROUP
    ,ROW_SECURITY_FLAG
    ,BU_SECURITY_FLAG
    ,current_user()                                            as INSERTED_BY
    ,current_user()                                            as UPDATED_BY
    ,current_timestamp()                                       as INSERTED_TS
    ,current_timestamp()                                       as UPDATED_TS
    ,PII_COLUMN_SECURITY_LIST
    ,COST_COLUMN_SECURITY_LIST
from admin.data_flow_object_security_lookup
where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_L1'
  and TARGET_OBJ_NAME    = 'fact_accounts_receivable_invoice';

-- Security row for the Iceberg archive (Task 1 target) — TODO[iceberg-location] confirm if needed

-- COMMAND ----------

-- DBTITLE 1,Verify
select * from admin.data_flow_control_header
where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_AR_SKU_SNAPSHOT_L1';

select * from admin.data_flow_pb_detail
where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_AR_SKU_SNAPSHOT_L1'
order by PRIORITY;

select * from admin.data_flow_object_security_lookup
where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_AR_SKU_SNAPSHOT_L1';
