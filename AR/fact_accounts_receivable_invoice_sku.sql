-- Databricks notebook source
-- DBTITLE 1,Model description
-- ============================================================================
-- Model:           fact_accounts_receivable_invoice_sku
-- Target:          finance_tbl.fact_accounts_receivable_invoice_sku
-- Materialization: create_or_replace_table  (framework LOAD_TYPE = FULL)
-- Runs on:         every_day
-- Depends on:      finance_tbl.fact_accounts_receivable_invoice
--                  direct_sales_conf.sapPR2028__vbrp
--                  master_data_l1_curated.dim_material_current
-- Description:     AR invoice fact exploded to AR x material grain via VBRP
--                  billing-line pro-rata. Carries the live aging-bucket
--                  classification (open / disputed / current_due / past_due /
--                  cleared) using current_date() as the as-of anchor.
-- ============================================================================
-- REVIEW BEFORE RUNNING THE INSERTS BELOW:
--   1. DFG_ID                  — proposed: FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SKU_L1 (41 chars, max 44)
--   2. COMPUTE_CLASS / _DEV    — copied from AR_L1; resize if VBRP fanout makes it heavy
--   3. DATA_SME / PRODUCT_OWNER — replace placeholders with real emails
--   4. SECURITY_GROUP / flags  — confirm against the existing AR_L1 security row
--   5. TRANSFORM_QUERY         — embedded in cell 3; same SELECT as the temp view in cell 1
-- ============================================================================

-- COMMAND ----------

-- DBTITLE 1,ar_sku temp view
create or replace temp view ar_sku as
with ar_base as (
    -- Source: enriched AR invoice fact (built by another job from BSID/BSAD + master data).
    -- Explicit column list so every downstream dependency is visible and reviewable.
    select
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
        sensors_alternate_sales_territory_name
    from finance_tbl.fact_accounts_receivable_invoice
),

ar_with_buckets as (
    -- Adds AR aging buckets at INVOICE-ITEM grain.
    -- Disjoint partition (Option A): open = current_due + past_due + disputed.
    -- Dispute rule: still uncleared AND payment_reason_code is exactly 2 chars (SAP BSEG-RSTGR).
    select
        ar.*,

        case
            when ar.accounting_document_item_clearing_status_name = 'OPEN'
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as receivable_open_actual_rate_functional_amount,

        case
            when ar.accounting_document_item_clearing_date is null
             and length(ar.accounting_document_item_payment_reason_code) = 2
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as receivable_disputed_actual_rate_functional_amount,

        case
            when ar.accounting_document_item_clearing_status_name = 'OPEN'
             and ar.accounting_document_item_due_date >= current_date()
             and not (length(ar.accounting_document_item_payment_reason_code) = 2)
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as receivable_current_due_actual_rate_functional_amount,

        case
            when ar.accounting_document_item_clearing_status_name = 'OPEN'
             and ar.accounting_document_item_due_date < current_date()
             and not (length(ar.accounting_document_item_payment_reason_code) = 2)
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as receivable_past_due_actual_rate_functional_amount,

        case
            when ar.accounting_document_item_clearing_status_name = 'CLEARED'
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as invoice_cleared_actual_rate_functional_amount
    from ar_base ar
),

vbrp_lines as (
    -- Billing-doc lines at material grain. Used to fan out one AR row into N material rows.
    select
        vbeln                                              as billing_document_id,
        posnr                                              as billing_document_item_id,
        ltrim('0', matnr)                                  as material_number,
        matnr                                              as raw_material_number,
        arktx                                              as material_billing_description,
        cast(lmeng as decimal(18, 3))                      as billing_quantity,
        meins                                              as unit_of_measure_code,
        cast(netwr as decimal(36, 3))                      as billing_line_net_value,
        cast(kzwi1 as decimal(36, 3))                      as billing_line_gross_value
    from direct_sales_conf.sapPR2028__vbrp
    where matnr is not null
      and trim(matnr) <> ''
),

vbrp_totals as (
    -- Pro-rata denominator: total net value per billing doc, plus line count for the equal-split
    -- fallback when total_net is zero.
    select
        billing_document_id,
        sum(billing_line_net_value)         as billing_doc_total_net_value,
        count(billing_document_item_id)     as billing_doc_line_item_count
    from vbrp_lines
    group by billing_document_id
),

dim_material as (
    -- Material dimension at curated grain. Source system 1 is the SAP master.
    select
        ltrim('0', material_number)                                as material_number,
        material_description,
        product_structure_level_3_code                             as product_hierarchy_l3_code,
        product_structure_level_3_label                            as product_hierarchy_l3_name,
        product_structure_level_4_code                             as product_hierarchy_l4_code,
        product_structure_level_4_label                            as product_hierarchy_l4_name,
        product_structure_level_5_code                             as cbc_code,
        product_structure_level_5_label                            as cbc_name
    from master_data_l1_curated.dim_material_current
    where source_system_id = 1
)

select
    -- AR grain (one row per AR invoice item, inherited from ar_with_buckets)
    ar.company_code,
    ar.customer_account_number,
    ar.accounting_document_id,
    ar.accounting_document_posting_fiscal_year_id,
    ar.accounting_document_item_id,
    ar.accounting_document_type_code,
    ar.accounting_document_create_date,
    ar.accounting_document_posting_date,
    ar.accounting_document_item_clearing_document_id,
    ar.accounting_document_item_clearing_date,
    ar.accounting_document_item_credit_or_debit_code,
    ar.accounting_document_item_posting_key_used_in_payment_indicator,
    ar.accounting_document_item_invoice_document_id,
    ar.accounting_document_item_text,
    ar.accounting_document_item_baseline_date,
    ar.accounting_document_item_due_date,
    ar.accounting_document_item_payment_terms_code,
    ar.accounting_document_item_cash_discount_days_quantity_1,
    ar.accounting_document_item_cash_discount_days_quantity_2,
    ar.accounting_document_create_network_user_id,
    ar.accounting_document_item_document_currency_code,
    ar.accounting_document_item_functional_currency_code,
    ar.accounting_document_item_payment_reason_code,
    ar.accounting_document_item_clearing_status_name,
    ar.ted_overriding_profit_center_id,
    ar.profit_center_id,
    ar.ted_profit_center_id,
    ar.ted_profit_center_effective_date,
    ar.accounting_document_breakdown_item_id,
    ar.accounting_document_item_document_currency_amount,
    ar.accouting_document_item_general_ledger_reconciliation_account_number,
    ar.accounting_document_item_functional_currency_amount,
    ar.accounting_document_item_actual_rate_functional_amount,
    ar.accounting_document_item_tariff_percentage,
    ar.data_security_tag_id,
    ar.business_unit_group_id,
    ar.accounting_document_item_budget_rate_amount,
    ar.sold_to_customer_number,
    ar.sales_organization_id,
    ar.distribution_channel_code,
    ar.worldwide_customer_level_1_number,
    ar.worldwide_customer_level_1_name,
    ar.worldwide_customer_level_2_number,
    ar.worldwide_customer_level_2_name,
    ar.country_region_level_1_name,
    ar.country_region_level_2_name,
    ar.gam_level_3_assoc_full_name,
    ar.account_manager_level_6_assoc_full_name,
    ar.account_manager_level_7_assoc_full_name,
    ar.sales_territory_level_6_assoc_full_name,
    ar.sales_territory_level_7_assoc_full_name,
    ar.sales_office_code,
    ar.sales_office_name,
    ar.account_manager_assoc_full_name,
    ar.sales_territory_assoc_full_name,
    ar.sensors_alternate_sales_territory_code,
    ar.sensors_alternate_sales_territory_name,

    -- Invoice-level AR aging buckets (live view, classified against current_date())
    -- Disjoint partition: receivable_open = current_due + past_due + disputed
    ar.receivable_open_actual_rate_functional_amount,
    ar.receivable_disputed_actual_rate_functional_amount,
    ar.receivable_current_due_actual_rate_functional_amount,
    ar.receivable_past_due_actual_rate_functional_amount,
    ar.invoice_cleared_actual_rate_functional_amount,

    -- Material grain identity (from VBRP)
    vl.billing_document_id,
    vl.billing_document_item_id,
    vl.material_number,
    vl.raw_material_number,
    coalesce(dm.material_description, vl.material_billing_description) as material_description,
    vl.billing_quantity                                                as material_quantity,
    vl.unit_of_measure_code                                            as material_unit_of_measure_code,
    vl.billing_line_net_value                                          as material_billing_line_net_value,
    vl.billing_line_gross_value                                        as material_billing_line_gross_value,

    -- Product hierarchy from material master
    dm.product_hierarchy_l3_code,
    dm.product_hierarchy_l3_name,
    dm.product_hierarchy_l4_code,
    dm.product_hierarchy_l4_name,
    dm.cbc_code                                                        as material_cbc_code,
    dm.cbc_name                                                        as material_cbc_name,

    -- Pro-rata factor (tri-state)
    --   Branch 1: total_net > 0  → line_net / total_net  (NET_VALUE_SHARE)
    --   Branch 2: total_net = 0  → 1 / line_count (>1)   (EQUAL_SPLIT)
    --   Branch 3: single-line    → 1.0 via nullif         (SINGLE_LINE)
    cast(
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ) as decimal(18, 10)
    ) as material_prorate_factor,
    vt.billing_doc_total_net_value,
    vt.billing_doc_line_item_count,

    -- Pro-rated amounts at material grain
    cast(round(
        ar.accounting_document_item_document_currency_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_document_currency_amount,

    cast(round(
        ar.accounting_document_item_actual_rate_functional_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_actual_rate_functional_amount,

    cast(round(
        ar.receivable_open_actual_rate_functional_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_open_functional_amount,

    cast(round(
        ar.receivable_current_due_actual_rate_functional_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_current_due_functional_amount,

    cast(round(
        ar.receivable_past_due_actual_rate_functional_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_past_due_functional_amount,

    cast(round(
        ar.receivable_disputed_actual_rate_functional_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_disputed_functional_amount,

    cast(round(
        ar.invoice_cleared_actual_rate_functional_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_cleared_functional_amount

from ar_with_buckets ar
    inner join vbrp_lines vl
        on vl.billing_document_id = ar.accounting_document_item_invoice_document_id
    left join vbrp_totals vt
        on vt.billing_document_id = ar.accounting_document_item_invoice_document_id
    left join dim_material dm
        on dm.material_number = vl.material_number;

-- COMMAND ----------

-- DBTITLE 1,HEADER INSERT
-- One row per pipeline group. Review the placeholders flagged at the top of cell 1
-- (DATA_SME, PRODUCT_OWNER, COMPUTE_CLASS) before running.
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
            'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SKU_L1' -- DATA_FLOW_GROUP_ID
            ,'JOB'                                      -- TRIGGER_TYPE
            ,'PB'                                       -- ETL_LAYER (L1 sits in the PB layer)
            ,'S_R5'                                     -- COMPUTE_CLASS_DEV (TODO: validate for SKU fanout)
            ,'M_R5_WP'                                  -- COMPUTE_CLASS
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
            ,null                                       -- SPARK_CONFIGS
            ,180                                        -- WARNING_THRESHOLD_MINS
            ,'support_developer_dl'                     -- WARNING_DL_GROUP
            ,null                                       -- MIN_VERSION
            ,null                                       -- MAX_VERSION
        );

-- COMMAND ----------

-- DBTITLE 1,DETAIL INSERT
-- One row per target object (here: the single fact table). TRANSFORM_QUERY is the
-- same WITH...SELECT from the temp view in cell 1 (no `create or replace temp view`
-- wrapper — the framework wraps the SELECT itself).
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
            'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SKU_L1' -- DATA_FLOW_GROUP_ID
            ,'finance'                                  -- LOB
            ,null                                       -- SOURCE
            ,'finance_tbl'                              -- TARGET_OBJ_SCHEMA
            ,'fact_accounts_receivable_invoice_sku'     -- TARGET_OBJ_NAME
            ,1                                          -- PRIORITY (single target)
            ,'TABLE'                                    -- TARGET_OBJ_TYPE
            ,"
with ar_base as (
    select
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
        sensors_alternate_sales_territory_name
    from finance_tbl.fact_accounts_receivable_invoice
),
ar_with_buckets as (
    select
        ar.*,
        case
            when ar.accounting_document_item_clearing_status_name = 'OPEN'
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as receivable_open_actual_rate_functional_amount,
        case
            when ar.accounting_document_item_clearing_date is null
             and length(ar.accounting_document_item_payment_reason_code) = 2
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as receivable_disputed_actual_rate_functional_amount,
        case
            when ar.accounting_document_item_clearing_status_name = 'OPEN'
             and ar.accounting_document_item_due_date >= current_date()
             and not (length(ar.accounting_document_item_payment_reason_code) = 2)
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as receivable_current_due_actual_rate_functional_amount,
        case
            when ar.accounting_document_item_clearing_status_name = 'OPEN'
             and ar.accounting_document_item_due_date < current_date()
             and not (length(ar.accounting_document_item_payment_reason_code) = 2)
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as receivable_past_due_actual_rate_functional_amount,
        case
            when ar.accounting_document_item_clearing_status_name = 'CLEARED'
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as invoice_cleared_actual_rate_functional_amount
    from ar_base ar
),
vbrp_lines as (
    select
        vbeln                                              as billing_document_id,
        posnr                                              as billing_document_item_id,
        ltrim('0', matnr)                                  as material_number,
        matnr                                              as raw_material_number,
        arktx                                              as material_billing_description,
        cast(lmeng as decimal(18, 3))                      as billing_quantity,
        meins                                              as unit_of_measure_code,
        cast(netwr as decimal(36, 3))                      as billing_line_net_value,
        cast(kzwi1 as decimal(36, 3))                      as billing_line_gross_value
    from direct_sales_conf.sapPR2028__vbrp
    where matnr is not null
      and trim(matnr) <> ''
),
vbrp_totals as (
    select
        billing_document_id,
        sum(billing_line_net_value)         as billing_doc_total_net_value,
        count(billing_document_item_id)     as billing_doc_line_item_count
    from vbrp_lines
    group by billing_document_id
),
dim_material as (
    select
        ltrim('0', material_number)                                as material_number,
        material_description,
        product_structure_level_3_code                             as product_hierarchy_l3_code,
        product_structure_level_3_label                            as product_hierarchy_l3_name,
        product_structure_level_4_code                             as product_hierarchy_l4_code,
        product_structure_level_4_label                            as product_hierarchy_l4_name,
        product_structure_level_5_code                             as cbc_code,
        product_structure_level_5_label                            as cbc_name
    from master_data_l1_curated.dim_material_current
    where source_system_id = 1
)
select
    ar.company_code,
    ar.customer_account_number,
    ar.accounting_document_id,
    ar.accounting_document_posting_fiscal_year_id,
    ar.accounting_document_item_id,
    ar.accounting_document_type_code,
    ar.accounting_document_create_date,
    ar.accounting_document_posting_date,
    ar.accounting_document_item_clearing_document_id,
    ar.accounting_document_item_clearing_date,
    ar.accounting_document_item_credit_or_debit_code,
    ar.accounting_document_item_posting_key_used_in_payment_indicator,
    ar.accounting_document_item_invoice_document_id,
    ar.accounting_document_item_text,
    ar.accounting_document_item_baseline_date,
    ar.accounting_document_item_due_date,
    ar.accounting_document_item_payment_terms_code,
    ar.accounting_document_item_cash_discount_days_quantity_1,
    ar.accounting_document_item_cash_discount_days_quantity_2,
    ar.accounting_document_create_network_user_id,
    ar.accounting_document_item_document_currency_code,
    ar.accounting_document_item_functional_currency_code,
    ar.accounting_document_item_payment_reason_code,
    ar.accounting_document_item_clearing_status_name,
    ar.ted_overriding_profit_center_id,
    ar.profit_center_id,
    ar.ted_profit_center_id,
    ar.ted_profit_center_effective_date,
    ar.accounting_document_breakdown_item_id,
    ar.accounting_document_item_document_currency_amount,
    ar.accouting_document_item_general_ledger_reconciliation_account_number,
    ar.accounting_document_item_functional_currency_amount,
    ar.accounting_document_item_actual_rate_functional_amount,
    ar.accounting_document_item_tariff_percentage,
    ar.data_security_tag_id,
    ar.business_unit_group_id,
    ar.accounting_document_item_budget_rate_amount,
    ar.sold_to_customer_number,
    ar.sales_organization_id,
    ar.distribution_channel_code,
    ar.worldwide_customer_level_1_number,
    ar.worldwide_customer_level_1_name,
    ar.worldwide_customer_level_2_number,
    ar.worldwide_customer_level_2_name,
    ar.country_region_level_1_name,
    ar.country_region_level_2_name,
    ar.gam_level_3_assoc_full_name,
    ar.account_manager_level_6_assoc_full_name,
    ar.account_manager_level_7_assoc_full_name,
    ar.sales_territory_level_6_assoc_full_name,
    ar.sales_territory_level_7_assoc_full_name,
    ar.sales_office_code,
    ar.sales_office_name,
    ar.account_manager_assoc_full_name,
    ar.sales_territory_assoc_full_name,
    ar.sensors_alternate_sales_territory_code,
    ar.sensors_alternate_sales_territory_name,
    ar.receivable_open_actual_rate_functional_amount,
    ar.receivable_disputed_actual_rate_functional_amount,
    ar.receivable_current_due_actual_rate_functional_amount,
    ar.receivable_past_due_actual_rate_functional_amount,
    ar.invoice_cleared_actual_rate_functional_amount,
    vl.billing_document_id,
    vl.billing_document_item_id,
    vl.material_number,
    vl.raw_material_number,
    coalesce(dm.material_description, vl.material_billing_description) as material_description,
    vl.billing_quantity                                                as material_quantity,
    vl.unit_of_measure_code                                            as material_unit_of_measure_code,
    vl.billing_line_net_value                                          as material_billing_line_net_value,
    vl.billing_line_gross_value                                        as material_billing_line_gross_value,
    dm.product_hierarchy_l3_code,
    dm.product_hierarchy_l3_name,
    dm.product_hierarchy_l4_code,
    dm.product_hierarchy_l4_name,
    dm.cbc_code                                                        as material_cbc_code,
    dm.cbc_name                                                        as material_cbc_name,
    cast(
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ) as decimal(18, 10)
    ) as material_prorate_factor,
    vt.billing_doc_total_net_value,
    vt.billing_doc_line_item_count,
    cast(round(
        ar.accounting_document_item_document_currency_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_document_currency_amount,
    cast(round(
        ar.accounting_document_item_actual_rate_functional_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_actual_rate_functional_amount,
    cast(round(
        ar.receivable_open_actual_rate_functional_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_open_functional_amount,
    cast(round(
        ar.receivable_current_due_actual_rate_functional_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_current_due_functional_amount,
    cast(round(
        ar.receivable_past_due_actual_rate_functional_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_past_due_functional_amount,
    cast(round(
        ar.receivable_disputed_actual_rate_functional_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_disputed_functional_amount,
    cast(round(
        ar.invoice_cleared_actual_rate_functional_amount *
        coalesce(
            vl.billing_line_net_value / nullif(vt.billing_doc_total_net_value, 0),
            1.0 / nullif(vt.billing_doc_line_item_count, 1)
        ), 3
    ) as decimal(36, 3)) as material_cleared_functional_amount
from ar_with_buckets ar
    inner join vbrp_lines vl
        on vl.billing_document_id = ar.accounting_document_item_invoice_document_id
    left join vbrp_totals vt
        on vt.billing_document_id = ar.accounting_document_item_invoice_document_id
    left join dim_material dm
        on dm.material_number = vl.material_number
"                                               -- TRANSFORM_QUERY
            ,null                               -- GENERIC_SCRIPTS
            ,null                               -- SOURCE_PK (FULL load, no MERGE keys needed)
            ,null                               -- TARGET_PK
            ,'FULL'                             -- LOAD_TYPE
            ,'Y'                                -- IS_ACTIVE
            ,'N'                                -- LS_FLAG
            ,null                               -- LS_DETAIL
            ,null                               -- PARTITION_OR_INDEX
            ,current_user()                     -- INSERTED_BY
            ,current_user()                     -- UPDATED_BY
            ,current_timestamp()                -- INSERTED_TS
            ,current_timestamp()                -- UPDATED_TS
            ,null                               -- CUSTOM_SCRIPT_PARAMS
            ,null                               -- PARTITION_METHOD
            ,null                               -- RETENTION_DETAILS
            ,null                               -- DEPLOYMENT_SOURCE_DFG
        );

-- COMMAND ----------

-- DBTITLE 1,SECURITY LOOKUP INSERT
-- Mirrors the existing AR L1 security row. Verify SECURITY_GROUP / flags match the
-- live AR_L1 entry before running (run the SELECT below to confirm).
--
--   select * from admin.data_flow_object_security_lookup
--   where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_L1'
--     and TARGET_OBJ_NAME    = 'fact_accounts_receivable_invoice';

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
    'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SKU_L1' as DATA_FLOW_GROUP_ID
    ,'finance_tbl'                              as TARGET_OBJ_SCHEMA
    ,'fact_accounts_receivable_invoice_sku'     as TARGET_OBJ_NAME
    ,SECURITY_GROUP
    ,ROW_SECURITY_FLAG
    ,BU_SECURITY_FLAG
    ,current_user()                             as INSERTED_BY
    ,current_user()                             as UPDATED_BY
    ,current_timestamp()                        as INSERTED_TS
    ,current_timestamp()                        as UPDATED_TS
    ,PII_COLUMN_SECURITY_LIST
    ,COST_COLUMN_SECURITY_LIST
from admin.data_flow_object_security_lookup
where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_L1'
  and TARGET_OBJ_NAME    = 'fact_accounts_receivable_invoice';

-- COMMAND ----------

-- DBTITLE 1,Verify
select * from admin.data_flow_control_header
where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SKU_L1';

select * from admin.data_flow_pb_detail
where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SKU_L1';

select * from admin.data_flow_object_security_lookup
where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SKU_L1';
