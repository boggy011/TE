-- Databricks notebook source
-- DBTITLE 1,header
select * from admin.data_flow_control_header
where    1=1
and DATA_FLOW_GROUP_ID LIKE 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1'

-- COMMAND ----------

select * from admin.data_flow_control_header
where    1=1
and DATA_FLOW_GROUP_ID LIKE 'FINANCE_FIN360_DATA_PRODUCTS_L2'

-- COMMAND ----------

-- DBTITLE 1,detail
select * from admin.data_flow_pb_detail
where    1=1
--and LOAD_TYPE = 'DELTA'
and DATA_FLOW_GROUP_ID LIKE 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1'
--and TARGET_OBJ_NAME like  'fact_accounts_receivable_invoice%'

-- COMMAND ----------

-- DBTITLE 1,create table script 1
-- =============================================================================
-- File 01: finance.fact_accounts_receivable_invoice_snapshot — Target table DDL (idempotent)
-- =============================================================================
-- Purpose:  Persistent AR fact table. Populated by MERGE (file 07).
--           Consumers should use finance.v_fact_accounts_receivable_invoice_snapshot (file 02) which filters
--           soft-deleted rows.
--
-- DDL:      CREATE TABLE IF NOT EXISTS (no-op after day 1)
-- Task:     T01 (parallel with T02-T05)
-- Params:   None
--
-- Grain (6 columns, §3.4):
--   company_code, customer_account_number, accounting_document_id,
--   accounting_document_posting_fiscal_year_id,
--   accounting_document_item_id, accounting_document_breakdown_item_id
-- =============================================================================
drop table if exists finance.fact_accounts_receivable_invoice_snapshot;

create table if not exists finance.fact_accounts_receivable_invoice_snapshot (
        company_code string
        ,customer_account_number string
        ,accounting_document_id string
        ,accounting_document_posting_fiscal_year_id string
        ,accounting_document_item_id string
        ,accounting_document_breakdown_item_id int
        ,accounting_document_type_code string
        ,accounting_document_create_date date
        ,accounting_document_posting_date date
        ,accounting_document_item_clearing_document_id string
        ,accounting_document_item_clearing_date date
        ,accounting_document_item_credit_or_debit_code string
        ,accounting_document_item_posting_key_used_in_payment_indicator string
        ,accounting_document_item_invoice_document_id string
        ,accounting_document_item_text string
        ,accounting_document_item_baseline_date date
        ,accounting_document_item_due_date date
        ,accounting_document_item_payment_terms_code string
        ,accounting_document_item_cash_discount_days_quantity_1 string
        ,accounting_document_item_cash_discount_days_quantity_2 string
        ,accounting_document_create_network_user_id string
        ,accounting_document_item_document_currency_code string
        ,accounting_document_item_functional_currency_code string
        ,accounting_document_item_payment_reason_code string
        ,accounting_document_item_clearing_status_name string
        ,ted_overriding_profit_center_id string
        ,profit_center_id string
        ,ted_profit_center_id string
        ,ted_profit_center_effective_date date
        ,accounting_document_item_document_currency_amount decimal(18, 3)
        ,accouting_document_item_general_ledger_reconciliation_account_number string
        ,accounting_document_item_functional_currency_amount decimal(18, 3)
        ,accounting_document_item_actual_rate_functional_amount decimal(18, 3)
        ,accounting_document_item_tariff_percentage decimal(18, 3)
        ,data_security_tag_id bigint
        ,business_unit_group_id string
        ,accounting_document_item_budget_rate_amount decimal(18, 3)
        ,sales_organization_id string
        ,distribution_channel_code string
        ,worldwide_customer_level_1_number string
        ,worldwide_customer_level_1_name string
        ,worldwide_customer_level_2_number string
        ,worldwide_customer_level_2_name string
        ,country_region_level_1_name string
        ,country_region_level_2_name string
        ,gam_level_3_assoc_full_name string
        ,account_manager_level_6_assoc_full_name string
        ,account_manager_level_7_assoc_full_name string
        ,sales_territory_level_6_assoc_full_name string
        ,sales_territory_level_7_assoc_full_name string
        ,sales_office_code string
        ,sales_office_name string
        ,account_manager_assoc_full_name string
        ,sales_territory_assoc_full_name string
        ,sensors_alternate_sales_territory_code string
        ,sensors_alternate_sales_territory_name string
    );

-- COMMAND ----------

create table if not exists finance_tbl.fx_rates_baseline (
        reference_date date
        ,from_currency string
        ,target_currency string
        ,date_exchange_rate date
        ,exchange_rate decimal(25, 10)
        ,from_currency_ratio decimal(18, 5)
        ,target_currency_ratio decimal(18, 5)
    );

-- COMMAND ----------

update  admin.data_flow_control_header
set     COMPUTE_CLASS_DEV = "Serverless"
    ,COMPUTE_CLASS =  "Serverless"
where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1';

-- COMMAND ----------

-- DBTITLE 1,HEADER INSERT
-- =============================================================================
-- Framework Registration: AR Delta Pipeline v2
-- =============================================================================
-- Run these INSERTs to register the v2 pipeline in the framework.
--
-- NOTE: Review and adjust before running:
--   1. DFG name — using _L2 suffix to distinguish from existing _L1
--   2. INSERTED_BY / UPDATED_BY — replace with your email
--   3. COMPUTE_CLASS — copied from existing L1; resize if needed
--   4. PRIORITY — controls execution order within the DFG
--   5. TRANSFORM_QUERY — file 06_v2 is large; pasted in full below
--      For files 04/05/09/02 the queries are short.
-- =============================================================================
-- =========================================================================
-- 1. HEADER: one row for the pipeline group
-- =========================================================================
--insert into admin.data_flow_control_header (
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
            'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNNAPSHOT_L2' -- DATA_FLOW_GROUP_ID
            ,'JOB' -- TRIGGER_TYPE
            ,'PB' -- ETL_LAYER
            ,'S_R5' -- COMPUTE_CLASS_DEV
            ,'M_R5_WP' -- COMPUTE_CLASS
            ,'Y' -- IS_ACTIVE
            ,current_user() -- INSERTED_BY
            ,current_user() -- UPDATED_BY
            ,current_timestamp() -- INSERTED_TS
            ,current_timestamp() -- UPDATED_TS
            ,'finance' -- BUSINESS_OBJECT_NAME
            ,null -- COST_CENTER
            ,'matt.edwards@te.com' -- DATA_SME
            ,'IN HOUSE' -- BUSINESS_UNIT
            ,'matt.edwards@te.com' -- PRODUCT_OWNER
            ,'DB_INGEST' -- INGESTION_MODE
            ,'eda' -- INGESTION_BUCKET
            ,null -- SPARK_CONFIGS
            ,180 -- WARNING_THRESHOLD_MINS
            ,'support_developer_dl' -- WARNING_DL_GROUP
            ,null -- MIN_VERSION
            ,null -- MAX_VERSION
        );

-- COMMAND ----------

-- DBTITLE 1,DETAIL AR KEYS
-- -- =========================================================================
-- 2. DETAIL: one row per target object (5 rows)
-- =========================================================================
-- -------------------------------------------------------------------------
-- 2a. staging.v_ar_active_keys (VIEW) — Priority 1
-- -------------------------------------------------------------------------
--insert into admin.data_flow_pb_detail (
--     DATA_FLOW_GROUP_ID
--     ,LOB
--     ,SOURCE
--     ,TARGET_OBJ_SCHEMA
--     ,TARGET_OBJ_NAME
--     ,PRIORITY
--     ,TARGET_OBJ_TYPE
--     ,TRANSFORM_QUERY
--     ,GENERIC_SCRIPTS
--     ,SOURCE_PK
--     ,TARGET_PK
--     ,LOAD_TYPE
--     ,IS_ACTIVE
--     ,LS_FLAG
--     ,LS_DETAIL
--     ,PARTITION_OR_INDEX
--     ,INSERTED_BY
--     ,UPDATED_BY
--     ,INSERTED_TS
--     ,UPDATED_TS
--     ,CUSTOM_SCRIPT_PARAMS
--     ,PARTITION_METHOD
--     ,RETENTION_DETAILS
--     ,DEPLOYMENT_SOURCE_DFG
-- )
--     values
--         (
--             'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1' -- DATA_FLOW_GROUP_ID
--             ,'finance' -- LOB
--             ,null -- SOURCE
--             ,'finance_tbl' -- TARGET_OBJ_SCHEMA
--             ,'v_ar_active_keys' -- TARGET_OBJ_NAME
--             ,1 -- PRIORITY (parallel with 05)
--             ,'VIEW' -- TARGET_OBJ_TYPE
--             ,"
-- SELECT
--   bukrs   AS company_code,
--   kunnr   AS customer_account_number,
--   belnr   AS accounting_document_id,
--   gjahr   AS accounting_document_posting_fiscal_year_id,
--   buzei   AS accounting_document_item_id
-- FROM
--   finance_conf.sapdl2008__bsid

-- UNION ALL

-- SELECT
--   bukrs   AS company_code,
--   kunnr   AS customer_account_number,
--   belnr   AS accounting_document_id,
--   gjahr   AS accounting_document_posting_fiscal_year_id,
--   buzei   AS accounting_document_item_id
-- FROM
--   finance_conf.sapdl2008__bsad
-- " -- TRANSFORM_QUERY
--             ,null -- GENERIC_SCRIPTS
--             ,null -- SOURCE_PK
--             ,null -- TARGET_PK (view — no PK)
--             ,'FULL' -- LOAD_TYPE
--             ,'Y' -- IS_ACTIVE
--             ,'N' -- LS_FLAG
--             ,null -- LS_DETAIL
--             ,null -- PARTITION_OR_INDEX
--             ,current_user() -- INSERTED_BY
--             ,current_user() -- UPDATED_BY
--             ,current_timestamp() -- INSERTED_TS
--             ,current_timestamp() -- UPDATED_TS
--             ,null -- CUSTOM_SCRIPT_PARAMS
--             ,null -- PARTITION_METHOD
--             ,null -- RETENTION_DETAILS
--             ,null -- DEPLOYMENT_SOURCE_DFG
--         );

-- COMMAND ----------

-- DBTITLE 1,DETAIL FX RATE CHANGED
-- -------------------------------------------------------------------------
-- 2b. staging.v_fx_rates_changed (VIEW) — Priority 1
-- -------------------------------------------------------------------------
--insert into admin.data_flow_pb_detail (
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
            'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1'
            ,'finance'
            ,null
            ,'finance_tbl'
            ,'v_fx_rates_changed'
            ,1 -- PRIORITY (parallel with 04)
            ,'VIEW'
            ,"
SELECT
  curr.reference_date,
  curr.from_currency
FROM
  finance_tbl.dim_currencies_dates_rates curr
    INNER JOIN finance_tbl.fx_rates_baseline base
      ON  curr.reference_date   = base.reference_date
      AND curr.from_currency    = base.from_currency
      AND curr.target_currency  = base.target_currency
WHERE
  curr.exchange_rate          IS DISTINCT FROM base.exchange_rate
  OR curr.from_currency_ratio IS DISTINCT FROM base.from_currency_ratio
  OR curr.target_currency_ratio IS DISTINCT FROM base.target_currency_ratio

UNION

SELECT
  curr.reference_date,
  curr.from_currency
FROM
  finance_tbl.dim_currencies_dates_rates curr
    LEFT JOIN finance_tbl.fx_rates_baseline base
      ON  curr.reference_date   = base.reference_date
      AND curr.from_currency    = base.from_currency
      AND curr.target_currency  = base.target_currency
WHERE
  base.reference_date IS NULL
"
            ,null
            ,null
            ,null
            ,'FULL'
            ,'Y'
            ,'N'
            ,null
            ,null
            ,current_user()
            ,current_user()
            ,current_timestamp()
            ,current_timestamp()
            ,null
            ,null
            ,null
            ,null
        );

-- COMMAND ----------

-- DBTITLE 1,DETAIL AR FACT DELTA
-- -------------------------------------------------------------------------
-- 2c. l2.ar_fact (TABLE, MERGE) — Priority 2
--     This is the main transform — file 06_v2
--     SOFT_DELETE_FLAG = Y so framework handles soft-delete
-- -------------------------------------------------------------------------
--insert into admin.data_flow_pb_detail (
--     DATA_FLOW_GROUP_ID
--     ,LOB
--     ,SOURCE
--     ,TARGET_OBJ_SCHEMA
--     ,TARGET_OBJ_NAME
--     ,PRIORITY
--     ,TARGET_OBJ_TYPE
--     ,TRANSFORM_QUERY
--     ,GENERIC_SCRIPTS
--     ,SOURCE_PK
--     ,TARGET_PK
--     ,LOAD_TYPE
--     ,IS_ACTIVE
--     ,LS_FLAG
--     ,LS_DETAIL
--     ,PARTITION_OR_INDEX
--     ,INSERTED_BY
--     ,UPDATED_BY
--     ,INSERTED_TS
--     ,UPDATED_TS
--     ,CUSTOM_SCRIPT_PARAMS
--     ,PARTITION_METHOD
--     ,RETENTION_DETAILS
--     ,DEPLOYMENT_SOURCE_DFG
-- )
--     values
--         (
--             'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1'
--             ,'finance'
--             ,null
--             ,'finance' -- TARGET_OBJ_SCHEMA
--             ,'fact_accounts_receivable_invoice_snapshot' -- TARGET_OBJ_NAME
--             ,2 -- PRIORITY (depends on views at priority 1)
--             ,'TABLE'
update admin.data_flow_pb_detail set TRANSFORM_QUERY =
"
WITH delta_config AS (
  SELECT
    CASE
      -- First run: table is empty → full load
      WHEN (SELECT COUNT(*) FROM finance.fact_accounts_receivable_invoice_snapshot) = 0
      THEN DATE '1900-01-01'
      -- Last Sunday of the month → full load
      WHEN DAYOFWEEK(CURRENT_DATE()) = 1
        AND MONTH(DATE_ADD(CURRENT_DATE(), 7)) != MONTH(CURRENT_DATE())
      THEN DATE '1900-01-01'
      -- Any other Sunday → 7-day window
      WHEN DAYOFWEEK(CURRENT_DATE()) = 1
      THEN DATE_SUB(CURRENT_DATE(), 7)
      -- Monday–Saturday → 1-day increment
      ELSE DATE_SUB(CURRENT_DATE(), 1)
    END AS delta_from_date
),


f_accounts_receivable_invoice_cte AS (

  SELECT
    bukrs, kunnr, belnr, gjahr, buzei, blart, bldat, budat,
    augbl, augdt, shkzg, xzahl, vbeln, sgtxt, zfbdt, zterm,
    zbd1t, zbd2t, zbd3t, waers, wrbtr, dmbtr, rstgr, saknr,
    'OPEN' AS item_status
  FROM
    finance_conf.sapdl2008__bsid
  WHERE
    budat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
    OR cpudt >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
    -- OR aedat >= ELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)    -- aedat not available in BSID/BSAD

  UNION ALL

  SELECT
    bukrs, kunnr, belnr, gjahr, buzei, blart, bldat, budat,
    augbl, augdt, shkzg, xzahl, vbeln, sgtxt, zfbdt, zterm,
    zbd1t, zbd2t, zbd3t, waers, wrbtr, dmbtr, rstgr, saknr,
    'CLEARED' AS item_status
  FROM
    finance_conf.sapdl2008__bsad
  WHERE
    budat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
    OR cpudt >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
    OR augdt >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
    -- OR aedat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)    -- aedat not available in BSID/BSAD

  UNION ALL

  SELECT
    bsid.bukrs, bsid.kunnr, bsid.belnr, bsid.gjahr, bsid.buzei, bsid.blart, bsid.bldat, bsid.budat,
    bsid.augbl, bsid.augdt, bsid.shkzg, bsid.xzahl, bsid.vbeln, bsid.sgtxt, bsid.zfbdt, bsid.zterm,
    bsid.zbd1t, bsid.zbd2t, bsid.zbd3t, bsid.waers, bsid.wrbtr, bsid.dmbtr, bsid.rstgr, bsid.saknr,
    'OPEN' AS item_status
    FROM finance_conf.sapdl2008__bsid bsid
    INNER JOIN master_data_conf.sapdl2008__t001 t001
      ON t001.bukrs = bsid.bukrs
    INNER JOIN finance_tbl.v_fx_rates_changed fx
      ON fx.from_currency  = t001.waers
      AND fx.reference_date = finance_tbl.convert_sap_datestring_to_date(
            COALESCE(NULLIF(bsid.augdt, '00000000'), bsid.budat))
    WHERE NOT (
        bsid.budat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
        OR bsid.cpudt >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
         -- OR bsid.aedat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd')  -- aedat not available
    )

  UNION ALL

  SELECT
    bsad.bukrs, bsad.kunnr, bsad.belnr, bsad.gjahr, bsad.buzei, bsad.blart, bsad.bldat, bsad.budat,
    bsad.augbl, bsad.augdt, bsad.shkzg, bsad.xzahl, bsad.vbeln, bsad.sgtxt, bsad.zfbdt, bsad.zterm,
    bsad.zbd1t, bsad.zbd2t, bsad.zbd3t, bsad.waers, bsad.wrbtr, bsad.dmbtr, bsad.rstgr, bsad.saknr,
    'CLEARED' AS item_status
    FROM finance_conf.sapdl2008__bsad bsad
    INNER JOIN master_data_conf.sapdl2008__t001 t001
      ON t001.bukrs = bsad.bukrs
    INNER JOIN finance_tbl.v_fx_rates_changed fx
      ON fx.from_currency  = t001.waers
      AND fx.reference_date = finance_tbl.convert_sap_datestring_to_date(
            COALESCE(NULLIF(bsad.augdt, '00000000'), bsad.budat))
    WHERE NOT (
        bsad.budat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
        OR bsad.cpudt >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
        OR bsad.augdt >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
        -- OR bsad.aedat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd')  -- aedat not available
    )
),
d_t052_distinct_cte AS (
  SELECT
    zterm,
    MAX(TRY_CAST(ztag1 AS INTEGER)) AS ztag1
  FROM
    master_data_conf.sapdl2008__t052
  GROUP BY
    ALL
),
d_material_cte AS (
  SELECT
    vbeln,
    CONCAT_WS(', ', COLLECT_LIST(DISTINCT matnr)) AS material_number
  FROM
    direct_sales_conf.sapdl2008__vbrp
  GROUP BY
    ALL
),
d_tariff_percentages_cte AS (
  SELECT
    bseg.belnr  AS belnr,
    bseg.bukrs  AS bukrs,
    bseg.gjahr  AS gjahr,
    MAX(vbrp.matnr) AS matnr
  FROM
    finance_conf.sapdl2008__bseg bseg
      LEFT JOIN direct_sales_conf.sapdl2008__vbrp vbrp
        ON vbrp.vbeln = bseg.kidno
        AND vbrp.matnr IN ('1-120037-2')
        AND vbrp.AUBEL = bseg.VBEL2
        AND vbrp.AUPOS = bseg.POSN2
  GROUP BY
    ALL
),
d_tariff_konv_cte AS (
  SELECT
    knumv,
    MAX(kbetr) AS kbetr
  FROM
    pricing_l0_raw.sapdl2008__konv
  WHERE
    kschl IN ('ZTRF', 'ZTRM')
  GROUP BY
    ALL
),
ref_gbl_product AS (
  SELECT
    PROD_CODE,
    PROD_BUSLN_FNCTN_ID
  FROM
    master_data_conf.gbl_current__gbl_product
  WHERE
    COALESCE(PROD_BUSLN_FNCTN_ID, '') != ''
),
ref_cbc_by_billing_doc AS (
  SELECT
    vbrp.vbeln,
    gp.PROD_BUSLN_FNCTN_ID AS competency_business_cde,
    ROW_NUMBER() OVER (PARTITION BY vbrp.vbeln ORDER BY ABS(vbrp.netwr) DESC) AS rn
  FROM
    direct_sales_conf.sapdl2008__vbrp vbrp
      INNER JOIN master_data_l0_raw.sapdl2008__mara mara
        ON vbrp.matnr = mara.matnr
      INNER JOIN ref_gbl_product gp
        ON mara.prdha = gp.PROD_CODE
),
ar_base AS (
  SELECT
    f_act_rec_inv_cte.bukrs                          AS company_code,
    f_act_rec_inv_cte.kunnr                          AS customer_account_number,
    f_act_rec_inv_cte.belnr                          AS accounting_document_id,
    f_act_rec_inv_cte.gjahr                          AS accounting_document_posting_fiscal_year_id,
    f_act_rec_inv_cte.buzei                          AS accounting_document_item_id,
    f_act_rec_inv_cte.blart                          AS accounting_document_type_code,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.bldat
    )                                                AS accounting_document_create_date,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.budat
    )                                                AS accounting_document_posting_date,
    f_act_rec_inv_cte.augbl                          AS accounting_document_item_clearing_document_id,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.augdt
    )                                                AS accounting_document_item_clearing_date,
    f_act_rec_inv_cte.shkzg                          AS accounting_document_item_credit_or_debit_code,
    f_act_rec_inv_cte.xzahl                          AS accounting_document_item_posting_key_used_in_payment_indicator,
    f_act_rec_inv_cte.vbeln                          AS accounting_document_item_invoice_document_id,
    f_act_rec_inv_cte.sgtxt                          AS accounting_document_item_text,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.zfbdt
    )                                                AS accounting_document_item_baseline_date,
    DATE_ADD(
      finance_tbl.convert_sap_datestring_to_date(
        IF(
          COALESCE(f_act_rec_inv_cte.zfbdt, '') IN ('', '00000000'),
          f_act_rec_inv_cte.bldat,
          f_act_rec_inv_cte.zfbdt
        )
      ),
      CASE
        WHEN
          f_act_rec_inv_cte.shkzg = 'H'
          AND COALESCE(f_bseg.rebzg, '') = ''
        THEN 0
        ELSE
          COALESCE(
            NULLIF(TRY_CAST(f_act_rec_inv_cte.zbd3t AS INTEGER), 0),
            NULLIF(TRY_CAST(f_act_rec_inv_cte.zbd2t AS INTEGER), 0),
            NULLIF(TRY_CAST(f_act_rec_inv_cte.zbd1t AS INTEGER), 0),
            d_t052_cte.ztag1,
            0
          )
      END
    )                                                AS accounting_document_item_due_date,
    f_act_rec_inv_cte.zterm                          AS accounting_document_item_payment_terms_code,
    f_act_rec_inv_cte.zbd1t                          AS accounting_document_item_cash_discount_days_quantity_1,
    f_act_rec_inv_cte.zbd2t                          AS accounting_document_item_cash_discount_days_quantity_2,
    f_bkpf.usnam                                     AS accounting_document_create_network_user_id,
    f_act_rec_inv_cte.waers                          AS accounting_document_item_document_currency_code,
    f_t001.waers                                     AS accounting_document_item_functional_currency_code,
    f_act_rec_inv_cte.rstgr                          AS accounting_document_item_payment_reason_code,
    f_act_rec_inv_cte.item_status                    AS accounting_document_item_clearing_status_name,
    COALESCE(f_bfod.prctr, f_bseg.prctr)            AS profit_center_id,
    d_ted_profit_center.ztedprc                      AS ted_profit_center_id,
    finance_tbl.convert_sap_datestring_to_date(
      d_ted_profit_center.eff_date
    )                                                AS ted_profit_center_effective_date,
    f_bfod.auzei                                     AS accounting_document_breakdown_item_id,
    IF(
      COALESCE(f_bfod.auzei, 1) = 1,
      finance_tbl.round_and_set_signal_amount(f_act_rec_inv_cte.wrbtr, f_act_rec_inv_cte.shkzg),
      0
    )                                                AS accounting_document_item_document_currency_amount,
    f_act_rec_inv_cte.saknr                          AS accouting_document_item_general_ledger_reconciliation_account_number,
    ROUND(
      COALESCE(
        finance_tbl.round_and_set_signal_amount(f_bfod.dmbtr, f_bfod.shkzg),
        finance_tbl.round_and_set_signal_amount(f_act_rec_inv_cte.dmbtr, f_act_rec_inv_cte.shkzg)
      )
        * POWER(10, 2 - COALESCE(f_tcurx_functional.currdec, 2)),
      3
    )                                                AS accounting_document_item_functional_currency_amount,
    CASE
      WHEN d_tk_cte.kbetr IS NOT NULL
      THEN COALESCE(d_tk_cte.kbetr / 10, IF(d_tp_cte.matnr IS NOT NULL, 100, 0))
      WHEN d_tp_cte.matnr IS NOT NULL THEN 100
      ELSE 0
    END                                              AS accounting_document_item_tariff_percentage,
    d_cs.customer_owning_business_unit_dsg_id,
    d_cs.sales_territory_owning_business_unit_dsg_id,
    d_cs.gam_global_business_unit_owning_business_unit_dsg_id,
    d_dc.company_dsg_id,
    d_cs.industry_business_code,
    f_t001.waers                                     AS company_currency_code,
    d_cer.currency_exchange_rate_to_USD_multiplier_factor,
    d_curr.exchange_rate,
    d_curr.from_currency_ratio,
    d_curr.target_currency_ratio,
    d_cbc.competency_business_cde                    AS resolved_cbc,
    d_pc_rels.MGE_PROFIT_CENTER_ABBREV_ID            AS override_bu_abbrev,
    d_pc_rels.SAP_PROFIT_CENTER_CDE                  AS override_sap_profit_center,
    d_bu.business_unit_id                            AS initial_business_unit_id,
    d_bu.business_unit_dsg_id,
    d_bu.business_unit_group_id,
    cust_lookup.customer_key_id,
    d_csa.sales_organization_id,
    d_csa.distribution_channel_code,
    d_csa.worldwide_customer_level_1_number,
    d_csa.worldwide_customer_level_1_label           AS worldwide_customer_level_1_name,
    d_csa.worldwide_customer_level_2_number,
    d_csa.worldwide_customer_level_2_label           AS worldwide_customer_level_2_name,
    d_csa.country_region_level_1_label               AS country_region_level_1_name,
    d_csa.country_region_level_2_label               AS country_region_level_2_name,
    d_csa.gam_level_3_assoc_full_name,
    d_csa.account_manager_level_6_assoc_full_name,
    d_csa.account_manager_level_7_assoc_full_name,
    d_csa.sales_territory_level_6_assoc_full_name,
    d_csa.sales_territory_level_7_assoc_full_name,
    d_csa.sales_office_code,
    d_csa.sales_office_label                         AS sales_office_name,
    d_csa.account_manager_level_1_assoc_full_name    AS account_manager_assoc_full_name,
    d_csa.sales_territory_level_1_assoc_full_name    AS sales_territory_assoc_full_name,
    d_cs.sensors_alternate_sales_territory_code,
    d_cs.sensors_alternate_sales_territory_name
  FROM
    f_accounts_receivable_invoice_cte f_act_rec_inv_cte
      LEFT JOIN finance_conf.sapdl2008__bseg f_bseg
        ON f_act_rec_inv_cte.belnr = f_bseg.belnr
        AND LTRIM('0', f_act_rec_inv_cte.buzei) = LTRIM('0', f_bseg.buzei)
        AND f_act_rec_inv_cte.bukrs = f_bseg.bukrs
        AND f_act_rec_inv_cte.gjahr = f_bseg.gjahr
      LEFT JOIN finance_conf.sapdl2008__bkpf f_bkpf
        ON f_act_rec_inv_cte.bukrs = f_bkpf.bukrs
        AND f_act_rec_inv_cte.belnr = f_bkpf.belnr
        AND f_act_rec_inv_cte.gjahr = f_bkpf.gjahr
      LEFT JOIN master_data_conf.sapdl2008__t001 f_t001
        ON f_act_rec_inv_cte.bukrs = f_t001.bukrs
      LEFT JOIN finance_l0_raw.sapdl2008__tcurx f_tcurx_functional
        ON f_tcurx_functional.currkey = f_t001.waers
      LEFT JOIN finance_l0_raw.sapdl2008__tcurx f_tcurx_document
        ON f_tcurx_document.currkey = f_act_rec_inv_cte.waers
      LEFT JOIN finance_tbl.dim_currencies_dates_rates d_curr
        ON d_curr.reference_date
          = finance_tbl.convert_sap_datestring_to_date(
            COALESCE(NULLIF(f_act_rec_inv_cte.augdt, 0), f_act_rec_inv_cte.budat)
          )
        AND d_curr.from_currency = f_t001.waers
      LEFT JOIN d_t052_distinct_cte d_t052_cte
        ON f_act_rec_inv_cte.zterm = d_t052_cte.zterm
      LEFT JOIN direct_sales_raw.sapdl2008__vbrk vbrk
        ON vbrk.vbeln = f_bseg.kidno
      LEFT JOIN d_tariff_konv_cte d_tk_cte
        ON d_tk_cte.knumv = vbrk.knumv
      LEFT JOIN d_tariff_percentages_cte d_tp_cte
        ON f_bkpf.belnr = d_tp_cte.belnr
        AND f_bkpf.bukrs = d_tp_cte.bukrs
        AND f_bkpf.gjahr = d_tp_cte.gjahr
      LEFT JOIN master_data_tbl.dim_customer d_cs
        ON d_cs.customer_account_number = REGEXP_REPLACE(f_act_rec_inv_cte.kunnr, '^0+', '')
        AND d_cs.customer_company_code = f_act_rec_inv_cte.bukrs
        AND d_cs.source_system_id = 1
        AND d_cs.customer_company_legacy_format_account_number_rank = 1
        AND d_cs.record_active_indicator = 'Y'
      LEFT JOIN master_data_tbl.dim_company d_dc
        ON d_dc.company_code = f_act_rec_inv_cte.bukrs
        AND d_dc.record_active_indicator = 'Y'
      LEFT JOIN finance_l0_raw.sapdl2008__bfod_a f_bfod
        ON f_bfod.belnr = f_act_rec_inv_cte.belnr
        AND f_bfod.buzei = f_act_rec_inv_cte.buzei
        AND f_bfod.gjahr = f_act_rec_inv_cte.gjahr
        AND f_bfod.bukrs = f_act_rec_inv_cte.bukrs
      LEFT JOIN master_data_tbl.dim_profit_center d_pc
        ON d_pc.profit_center_id = COALESCE(f_bfod.prctr, f_bseg.prctr)
        AND d_pc.record_active_indicator = 'Y'
      LEFT JOIN master_data_tbl.dim_business_unit d_bu
        ON d_pc.business_unit_id = d_bu.business_unit_id
        AND d_bu.record_active_indicator = 'Y'
      LEFT JOIN master_data_tbl.dim_currency_exchange_rate d_cer
        ON d_cer.currency_code = f_t001.waers
        AND d_cer.currency_exchange_rate_type_code = 1
        AND TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())) BETWEEN
          TO_DATE(d_cer.effective_from_date)
        AND
          TO_DATE(d_cer.effective_to_date)
      LEFT JOIN finance_l0_raw.sapdl2008__zpcafence d_ted_profit_center
        ON d_ted_profit_center.prctr = COALESCE(f_bfod.prctr, f_bseg.prctr)
        AND d_ted_profit_center.eff_date <= f_act_rec_inv_cte.budat
      LEFT JOIN ref_cbc_by_billing_doc d_cbc
        ON f_bseg.kidno = d_cbc.vbeln
        AND d_cbc.rn = 1
        AND COALESCE(f_bseg.kidno, '') != ''
      LEFT JOIN master_data_conf.gbl_current__gbl_mge_profit_center_rels d_pc_rels
        ON f_act_rec_inv_cte.bukrs = d_pc_rels.ORGANIZATION_ID
        AND d_cs.industry_business_code = d_pc_rels.INDUSTRY_BUSINESS_CDE
        AND d_cbc.competency_business_cde = d_pc_rels.COMPETENCY_BUSINESS_CDE
      LEFT JOIN LATERAL (
        SELECT
          customer_key_id
        FROM
          master_data_l1_curated.dim_customer_current
        WHERE
          submitted_customer_account_number = f_act_rec_inv_cte.kunnr
          AND customer_company_code = f_act_rec_inv_cte.bukrs
          AND source_system_id = 1
        ORDER BY
          distribution_channel_code ASC
        LIMIT 1
      ) cust_lookup
      LEFT JOIN master_data_l1_curated.dim_customer_current d_csa
        ON d_csa.customer_key_id = cust_lookup.customer_key_id
  QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY
          f_act_rec_inv_cte.bukrs,
          f_act_rec_inv_cte.kunnr,
          f_act_rec_inv_cte.belnr,
          f_act_rec_inv_cte.gjahr,
          f_act_rec_inv_cte.buzei,
          f_bfod.auzei
        ORDER BY
          d_ted_profit_center.eff_date DESC,
          IF(f_act_rec_inv_cte.item_status = 'CLEARED', 1, 2) ASC
      ) = 1
),
ar_final AS (
  SELECT
    b.company_code,
    b.customer_account_number,
    b.accounting_document_id,
    b.accounting_document_posting_fiscal_year_id,
    b.accounting_document_item_id,
    b.accounting_document_type_code,
    b.accounting_document_create_date,
    b.accounting_document_posting_date,
    b.accounting_document_item_clearing_document_id,
    b.accounting_document_item_clearing_date,
    b.accounting_document_item_credit_or_debit_code,
    b.accounting_document_item_posting_key_used_in_payment_indicator,
    b.accounting_document_item_invoice_document_id,
    b.accounting_document_item_text,
    b.accounting_document_item_baseline_date,
    b.accounting_document_item_due_date,
    b.accounting_document_item_payment_terms_code,
    b.accounting_document_item_cash_discount_days_quantity_1,
    b.accounting_document_item_cash_discount_days_quantity_2,
    b.accounting_document_create_network_user_id,
    b.accounting_document_item_document_currency_code,
    b.accounting_document_item_functional_currency_code,
    b.accounting_document_item_payment_reason_code,
    b.accounting_document_item_clearing_status_name,
    CASE
      WHEN
        b.resolved_cbc IS NOT NULL
        AND (
          COALESCE(b.initial_business_unit_id, '') IN ('', 'LGL', 'TMP', 'CRP')
          OR b.profit_center_id IS NULL
        )
        AND b.override_sap_profit_center IS NOT NULL
      THEN b.override_sap_profit_center
      WHEN
        b.resolved_cbc IS NULL
        AND COALESCE(b.initial_business_unit_id, '') IN ('LGL', 'TMP', 'CRP')
      THEN NULL
      ELSE b.profit_center_id
    END                                              AS ted_overriding_profit_center_id,
    b.profit_center_id,
    b.ted_profit_center_id,
    b.ted_profit_center_effective_date,
    b.accounting_document_breakdown_item_id,
    b.accounting_document_item_document_currency_amount,
    b.accouting_document_item_general_ledger_reconciliation_account_number,
    b.accounting_document_item_functional_currency_amount,
    COALESCE(
      finance_tbl.convert_amount_to_usd_currency(
        b.accounting_document_item_functional_currency_amount,
        b.exchange_rate,
        b.from_currency_ratio,
        b.target_currency_ratio
      ),
      b.accounting_document_item_functional_currency_amount
    )                                                AS accounting_document_item_actual_rate_functional_amount,
    b.accounting_document_item_tariff_percentage,
    admin.get_data_security_tag(
      ARRAY(
        COALESCE(b.customer_owning_business_unit_dsg_id, 0),
        COALESCE(b.sales_territory_owning_business_unit_dsg_id, 0),
        COALESCE(b.gam_global_business_unit_owning_business_unit_dsg_id, 0),
        COALESCE(b.company_dsg_id, 0),
        COALESCE(d_bu_final.business_unit_dsg_id, b.business_unit_dsg_id, 0)
      )
    )                                                AS data_security_tag_id,
    COALESCE(d_bu_final.business_unit_group_id, b.business_unit_group_id)
                                                     AS business_unit_group_id,
    b.accounting_document_item_functional_currency_amount
      * b.currency_exchange_rate_to_USD_multiplier_factor
                                                     AS accounting_document_item_budget_rate_amount,
    b.sales_organization_id,
    b.distribution_channel_code,
    b.worldwide_customer_level_1_number,
    b.worldwide_customer_level_1_name,
    b.worldwide_customer_level_2_number,
    b.worldwide_customer_level_2_name,
    b.country_region_level_1_name,
    b.country_region_level_2_name,
    b.gam_level_3_assoc_full_name,
    b.account_manager_level_6_assoc_full_name,
    b.account_manager_level_7_assoc_full_name,
    b.sales_territory_level_6_assoc_full_name,
    b.sales_territory_level_7_assoc_full_name,
    b.sales_office_code,
    b.sales_office_name,
    b.account_manager_assoc_full_name,
    b.sales_territory_assoc_full_name,
    b.sensors_alternate_sales_territory_code,
    b.sensors_alternate_sales_territory_name
  FROM
    ar_base b
      LEFT JOIN master_data_tbl.dim_profit_center d_pc_final
        ON d_pc_final.profit_center_id
          = CASE
            WHEN
              b.resolved_cbc IS NOT NULL
              AND (
                COALESCE(b.initial_business_unit_id, '') IN ('', 'LGL', 'TMP', 'CRP')
                OR b.profit_center_id IS NULL
              )
              AND b.override_sap_profit_center IS NOT NULL
            THEN b.override_sap_profit_center
            WHEN
              b.resolved_cbc IS NULL
              AND COALESCE(b.initial_business_unit_id, '') IN ('LGL', 'TMP', 'CRP')
            THEN NULL
            ELSE b.profit_center_id
          END
        AND d_pc_final.record_active_indicator = 'Y'
      LEFT JOIN master_data_tbl.dim_business_unit d_bu_final
        ON d_pc_final.business_unit_id = d_bu_final.business_unit_id
        AND d_bu_final.record_active_indicator = 'Y'
)

SELECT * FROM ar_final
" -- TRANSFORM_QUERY
,CUSTOM_SCRIPT_PARAMS = map('MULTIPLE_SOURCES', 'N', 'SOFT_DELETE_FLAG', 'N', 'HARD_DELETE_FLAG', 'N','PRIMARY_KEYS','company_code,customer_account_number,accounting_document_id,accounting_document_posting_fiscal_year_id,accounting_document_item_id,accounting_document_breakdown_item_id')
,SOURCE_PK = 'company_code,customer_account_number,accounting_document_id,accounting_document_posting_fiscal_year_id,accounting_document_item_id,accounting_document_breakdown_item_id'
where DATA_FLOW_GROUP_ID = "FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1"
and TARGET_OBJ_NAME = "fact_accounts_receivable_invoice_snapshot"


        --     ,null -- GENERIC_SCRIPTS
        --     ,'company_code,customer_account_number,accounting_document_id,accounting_document_posting_fiscal_year_id,accounting_document_item_id,accounting_document_breakdown_item_id' -- SOURCE_PK
        --     ,'company_code,customer_account_number,accounting_document_id,accounting_document_posting_fiscal_year_id,accounting_document_item_id,accounting_document_breakdown_item_id'
        --     -- TARGET_PK (6-col grain)
        --     ,'DELTA' -- LOAD_TYPE
        --     ,'Y' -- IS_ACTIVE
        --     ,'N' -- LS_FLAG
        --     ,null -- LS_DETAIL
        --     ,null -- PARTITION_OR_INDEX
        --     ,current_user() -- INSERTED_BY
        --     ,current_user() -- UPDATED_BY
        --     ,current_timestamp() -- INSERTED_TS
        --     ,current_timestamp() -- UPDATED_TS
        --     ,map('MULTIPLE_SOURCES', 'N', 'SOFT_DELETE_FLAG', 'N', 'HARD_DELETE_FLAG', 'N','PRIMARY_KEYS','company_code,customer_account_number,accounting_document_id,accounting_document_posting_fiscal_year_id,accounting_document_item_id,accounting_document_breakdown_item_id')
        --     ,null -- PARTITION_METHOD
        --     ,null -- RETENTION_DETAILS
        --     ,null -- DEPLOYMENT_SOURCE_DFG
        -- );

-- COMMAND ----------

-- DBTITLE 1,DETAILS FX RATES BASELINE
-- -------------------------------------------------------------------------
-- 2d. staging.fx_rates_baseline (TABLE, FULL) — Priority 3
--     MUST run AFTER ar_fact merge succeeds
-- -------------------------------------------------------------------------
--insert into admin.data_flow_pb_detail (
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
            'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1'
            ,'finance'
            ,null
            ,'finance_tbl'
            ,'fx_rates_baseline'
            ,3 -- PRIORITY (after MERGE at priority 2)
            ,'TABLE'
            ,"
SELECT
  reference_date,
  from_currency,
  target_currency,
  date_exchange_rate,
  exchange_rate,
  from_currency_ratio,
  target_currency_ratio
FROM
  finance_tbl.dim_currencies_dates_rates
"
            ,null
            ,null
            ,null
            ,'FULL' -- LOAD_TYPE (recreate each run)
            ,'Y'
            ,'N'
            ,null
            ,null
            ,current_user()
            ,current_user()
            ,current_timestamp()
            ,current_timestamp()
            ,null
            ,null
            ,null
            ,null
        );

-- COMMAND ----------

-- DBTITLE 1,AR VIEW _ NO DELETED RECORDS
-- -------------------------------------------------------------------------
-- 2e. l2.v_ar_fact (VIEW) — Priority 3
-- -------------------------------------------------------------------------
--insert into admin.data_flow_pb_detail (
--     DATA_FLOW_GROUP_ID
--     ,LOB
--     ,SOURCE
--     ,TARGET_OBJ_SCHEMA
--     ,TARGET_OBJ_NAME
--     ,PRIORITY
--     ,TARGET_OBJ_TYPE
--     ,TRANSFORM_QUERY
--     ,GENERIC_SCRIPTS
--     ,SOURCE_PK
--     ,TARGET_PK
--     ,LOAD_TYPE
--     ,IS_ACTIVE
--     ,LS_FLAG
--     ,LS_DETAIL
--     ,PARTITION_OR_INDEX
--     ,INSERTED_BY
--     ,UPDATED_BY
--     ,INSERTED_TS
--     ,UPDATED_TS
--     ,CUSTOM_SCRIPT_PARAMS
--     ,PARTITION_METHOD
--     ,RETENTION_DETAILS
--     ,DEPLOYMENT_SOURCE_DFG
-- )
--     values
--         (
--             'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1'
--             ,'finance'
--             ,null
--             ,'finance'
--             ,'v_fact_accounts_receivable_invoice_snapshot'
--             ,3 -- PRIORITY (parallel with FX baseline)
--             ,'VIEW'
update admin.data_flow_pb_detail
set
  transform_query =
"
SELECT
  company_code,
  customer_account_number,
  accounting_document_id,
  accounting_document_posting_fiscal_year_id,
  accounting_document_item_id,
  accounting_document_breakdown_item_id,
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
  accounting_document_item_document_currency_amount,
  accouting_document_item_general_ledger_reconciliation_account_number,
  accounting_document_item_functional_currency_amount,
  accounting_document_item_actual_rate_functional_amount,
  accounting_document_item_tariff_percentage,
  data_security_tag_id,
  business_unit_group_id,
  accounting_document_item_budget_rate_amount,
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
FROM
  finance.fact_accounts_receivable_invoice_snapshot
WHERE 1=1
  --- AND is_deleted = 'N'
"
where DATA_FLOW_GROUP_ID = "FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1"
and TARGET_OBJ_NAME = "v_fact_accounts_receivable_invoice_snapshot"
        --     ,null
        --     ,null
        --     ,null
        --     ,'FULL'
        --     ,'Y'
        --     ,'N'
        --     ,null
        --     ,null
        --     ,current_user()
        --     ,current_user()
        --     ,current_timestamp()
        --     ,current_timestamp()
        --     ,null
        --     ,null
        --     ,null
        --     ,null
        -- );

-- COMMAND ----------

-- DBTITLE 1,L2 - accounts_receivable_invoice_snapshot_open
-- -------------------------------------------------------------------------
-- 2f. l2.v_ar_fact_l2 (TABLE) — Priority 4
-- -------------------------------------------------------------------------
-- insert into admin.data_flow_pb_detail (
--     DATA_FLOW_GROUP_ID
--     ,LOB
--     ,SOURCE
--     ,TARGET_OBJ_SCHEMA
--     ,TARGET_OBJ_NAME
--     ,PRIORITY
--     ,TARGET_OBJ_TYPE
--     ,TRANSFORM_QUERY
--     ,GENERIC_SCRIPTS
--     ,SOURCE_PK
--     ,TARGET_PK
--     ,LOAD_TYPE
--     ,IS_ACTIVE
--     ,LS_FLAG
--     ,LS_DETAIL
--     ,PARTITION_OR_INDEX
--     ,INSERTED_BY
--     ,UPDATED_BY
--     ,INSERTED_TS
--     ,UPDATED_TS
--     ,CUSTOM_SCRIPT_PARAMS
--     ,PARTITION_METHOD
--     ,RETENTION_DETAILS
--     ,DEPLOYMENT_SOURCE_DFG
-- )
--     values
--         (
--             'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1'
--             ,'finance'
--             ,null
--             ,'finance'
--             ,'accounts_receivable_invoice_snapshot_open'
--             ,4 -- PRIORITY
--             ,'TABLE'
 update admin.data_flow_pb_detail set TRANSFORM_QUERY =
"
select
    accounting_document_id
    ,accounting_document_item_id
    ,customer_account_number
    ,company_code
    ,accounting_document_create_date invoice_create_date
    ,accounting_document_posting_date invoice_posting_date
    ,accounting_document_item_due_date invoice_due_date
    ,accounting_document_type_code invoice_document_type_code
    ,accounting_document_item_invoice_document_id invoice_document_id
    ,accounting_document_item_document_currency_amount invoice_document_currency_amount
    ,accounting_document_item_actual_rate_functional_amount invoice_actual_rate_functional_amount
    ,accounting_document_item_payment_terms_code invoice_payment_terms_code
    ,ted_overriding_profit_center_id profit_center_id
    ,accounting_document_item_clearing_status_name invoice_clearing_status_name
    ,accounting_document_item_document_currency_code invoice_document_currency_code
    ,accounting_document_item_functional_currency_code invoice_document_functional_currency_code
    ,if(
        coalesce(accounting_document_item_tariff_percentage) > 0
        ,'Tariff'
        ,'All Other'
    ) invoice_tariff_indicator
    ,round(
        case
            when
                coalesce(accounting_document_item_tariff_percentage, 0) > 0
                and coalesce(accounting_document_item_tariff_percentage, 0) < 100
            then
                (
                    accounting_document_item_actual_rate_functional_amount
                        - (
                            accounting_document_item_actual_rate_functional_amount
                                / (100 + accounting_document_item_tariff_percentage)
                        )
                            * 100
                )
            when
                accounting_document_item_tariff_percentage = 100
            then
                accounting_document_item_actual_rate_functional_amount
            else 0
        end
        ,3
    ) invoice_actual_tariff_rate_functional_amount
    ,case
        when
            datediff(current_date(), accounting_document_item_due_date) between
                0
            and
                30
        then
            '30'
        when
            datediff(current_date(), accounting_document_item_due_date) between
                31
            and
                60
        then
            '60'
        when
            datediff(current_date(), accounting_document_item_due_date) between
                61
            and
                90
        then
            '90'
        when
            datediff(current_date(), accounting_document_item_due_date) between
                91
            and
                180
        then
            '180'
        when
            datediff(current_date(), accounting_document_item_due_date) between
                181
            and
                210
        then
            '210'
        when
            datediff(current_date(), accounting_document_item_due_date) between
                211
            and
                240
        then
            '240'
        when
            datediff(current_date(), accounting_document_item_due_date) between
                241
            and
                280
        then
            '280'
        when
            datediff(current_date(), accounting_document_item_due_date) between
                281
            and
                320
        then
            '320'
        when
            datediff(current_date(), accounting_document_item_due_date) between
                321
            and
                365
        then
            '365'
        when datediff(current_date(), accounting_document_item_due_date) >= 366 then 'More than 365'
    end invoice_due_date_sorted_list
    ,accounting_document_posting_fiscal_year_id
    ,accounting_document_item_payment_reason_code invoice_payment_reason_code
    ,if(coalesce(invoice_payment_reason_code, '') = '', 0, 1) invoice_dispute_indicator
    ,datediff(current_date(), invoice_due_date) invoice_days_since_due_date
    ,if(
        isnull(accounting_document_item_clearing_date)
        and length(accounting_document_item_payment_reason_code) = 2
        ,accounting_document_item_actual_rate_functional_amount
        ,0
    ) receivable_disputed_document_currency_amount
    ,if(
        isnull(accounting_document_item_clearing_date)
        ,accounting_document_item_actual_rate_functional_amount
        ,0
    ) receivable_open_document_currency_amount
    ,if(
        isnull(accounting_document_item_clearing_date)
        and accounting_document_item_due_date > current_date()
        ,accounting_document_item_actual_rate_functional_amount
        ,0
    ) receivable_current_due_document_currency_amount
    ,if(
        isnull(accounting_document_item_clearing_date)
        and accounting_document_item_due_date <= current_date()
        ,accounting_document_item_actual_rate_functional_amount
        ,0
    ) receivable_past_due_document_currency_amount
    ,if(
        not isnull(accounting_document_item_clearing_date)
        ,accounting_document_item_actual_rate_functional_amount
        ,0
    ) invoice_cleared_document_currency_amount
    ,if(
        isnull(accounting_document_item_clearing_date)
        and length(accounting_document_item_payment_reason_code) = 2
        ,accounting_document_item_actual_rate_functional_amount
        ,0
    ) receivable_disputed_actual_rate_functional_amount
    ,if(
        isnull(accounting_document_item_clearing_date)
        ,accounting_document_item_actual_rate_functional_amount
        ,0
    ) receivable_open_actual_rate_functional_amount
    ,if(
        isnull(accounting_document_item_clearing_date)
        and accounting_document_item_due_date > current_date()
        ,accounting_document_item_actual_rate_functional_amount
        ,0
    ) receivable_current_due_actual_rate_functional_amount
    ,if(
        isnull(accounting_document_item_clearing_date)
        and accounting_document_item_due_date <= current_date()
        ,accounting_document_item_actual_rate_functional_amount
        ,0
    ) receivable_past_due_actual_rate_functional_amount
    ,if(
        not isnull(accounting_document_item_clearing_date)
        ,accounting_document_item_actual_rate_functional_amount
        ,0
    ) invoice_cleared_actual_rate_functional_amount
    ,accounting_document_item_clearing_date invoice_clearing_date
    ,data_security_tag_id data_security_tag_id
    ,business_unit_group_id business_unit_group_id
    ,accounting_document_item_budget_rate_amount
  ,sales_organization_id
  ,distribution_channel_code
  ,worldwide_customer_level_1_number
  ,worldwide_customer_level_1_name
  ,worldwide_customer_level_2_number
  ,worldwide_customer_level_2_name
  ,country_region_level_1_name
  ,country_region_level_2_name
  ,gam_level_3_assoc_full_name
  ,account_manager_level_6_assoc_full_name
  ,account_manager_level_7_assoc_full_name
  ,sales_territory_level_6_assoc_full_name
  ,sales_territory_level_7_assoc_full_name
  ,sales_office_code
  ,sales_office_name
  ,account_manager_assoc_full_name
  ,sales_territory_assoc_full_name
  ,sensors_alternate_sales_territory_code
  ,sensors_alternate_sales_territory_name
from finance.v_fact_accounts_receivable_invoice_snapshot
where
  accounting_document_item_clearing_status_name = 'OPEN'
"
where DATA_FLOW_GROUP_ID = "FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1"
and TARGET_OBJ_NAME = "accounts_receivable_invoice_snapshot_open"
        --     ,null
        --     ,null
        --     ,null
        --     ,'FULL'
        --     ,'Y'
        --     ,'N'
        --     ,null
        --     ,null
        --     ,current_user()
        --     ,current_user()
        --     ,current_timestamp()
        --     ,current_timestamp()
        --     ,null
        --     ,null
        --     ,null
        --     ,null
        -- );

-- COMMAND ----------

select count(*) from finance_tbl.fact_accounts_receivable_invoice

-- COMMAND ----------

select count(*) from finance.fact_accounts_receivable_invoice_snapshot

-- COMMAND ----------

--truncate table finance.fact_accounts_receivable_invoice_snapshot

-- COMMAND ----------

WITH delta_config AS (
  SELECT
    CASE
      -- First run: table is empty → full load
      WHEN (SELECT COUNT(*) FROM finance.fact_accounts_receivable_invoice_snapshot) = 0
      THEN DATE '1900-01-01'
      -- Last Sunday of the month → full load
      WHEN DAYOFWEEK(CURRENT_DATE()) = 1
        AND MONTH(DATE_ADD(CURRENT_DATE(), 7)) != MONTH(CURRENT_DATE())
      THEN DATE '1900-01-01'
      -- Any other Sunday → 7-day window
      WHEN DAYOFWEEK(CURRENT_DATE()) = 1
      THEN DATE_SUB(CURRENT_DATE(), 7)
      -- Monday–Saturday → 1-day increment
      ELSE DATE_SUB(CURRENT_DATE(), 1)
    END AS delta_from_date
),

f_accounts_receivable_invoice_cte AS (

  SELECT
    bukrs, kunnr, belnr, gjahr, buzei, blart, bldat, budat,
    augbl, augdt, shkzg, xzahl, vbeln, sgtxt, zfbdt, zterm,
    zbd1t, zbd2t, zbd3t, waers, wrbtr, dmbtr, rstgr, saknr,
    'OPEN' AS item_status
  FROM
    finance_conf.sapdl2008__bsid
  WHERE
    budat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
    OR cpudt >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
    -- OR aedat >= ELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)    -- aedat not available in BSID/BSAD

  UNION ALL

  SELECT
    bukrs, kunnr, belnr, gjahr, buzei, blart, bldat, budat,
    augbl, augdt, shkzg, xzahl, vbeln, sgtxt, zfbdt, zterm,
    zbd1t, zbd2t, zbd3t, waers, wrbtr, dmbtr, rstgr, saknr,
    'CLEARED' AS item_status
  FROM
    finance_conf.sapdl2008__bsad
  WHERE
    budat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
    OR cpudt >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
    OR augdt >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
    -- OR aedat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)    -- aedat not available in BSID/BSAD

  UNION ALL

  SELECT
    bsid.bukrs, bsid.kunnr, bsid.belnr, bsid.gjahr, bsid.buzei, bsid.blart, bsid.bldat, bsid.budat,
    bsid.augbl, bsid.augdt, bsid.shkzg, bsid.xzahl, bsid.vbeln, bsid.sgtxt, bsid.zfbdt, bsid.zterm,
    bsid.zbd1t, bsid.zbd2t, bsid.zbd3t, bsid.waers, bsid.wrbtr, bsid.dmbtr, bsid.rstgr, bsid.saknr,
    'OPEN' AS item_status
    FROM finance_conf.sapdl2008__bsid bsid
    INNER JOIN master_data_conf.sapdl2008__t001 t001
      ON t001.bukrs = bsid.bukrs
    INNER JOIN finance_tbl.v_fx_rates_changed fx
      ON fx.from_currency  = t001.waers
      AND fx.reference_date = finance_tbl.convert_sap_datestring_to_date(
            COALESCE(NULLIF(bsid.augdt, '00000000'), bsid.budat))
    WHERE NOT (
        bsid.budat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
        OR bsid.cpudt >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
        -- OR bsid.aedat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd')  -- aedat not available
    )

  UNION ALL

  SELECT
    bsad.bukrs, bsad.kunnr, bsad.belnr, bsad.gjahr, bsad.buzei, bsad.blart, bsad.bldat, bsad.budat,
    bsad.augbl, bsad.augdt, bsad.shkzg, bsad.xzahl, bsad.vbeln, bsad.sgtxt, bsad.zfbdt, bsad.zterm,
    bsad.zbd1t, bsad.zbd2t, bsad.zbd3t, bsad.waers, bsad.wrbtr, bsad.dmbtr, bsad.rstgr, bsad.saknr,
    'CLEARED' AS item_status
    FROM finance_conf.sapdl2008__bsad bsad
    INNER JOIN master_data_conf.sapdl2008__t001 t001
      ON t001.bukrs = bsad.bukrs
    INNER JOIN finance_tbl.v_fx_rates_changed fx
      ON fx.from_currency  = t001.waers
      AND fx.reference_date = finance_tbl.convert_sap_datestring_to_date(
            COALESCE(NULLIF(bsad.augdt, '00000000'), bsad.budat))
    WHERE NOT (
        bsad.budat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
        OR bsad.cpudt >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
        OR bsad.augdt >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd') FROM delta_config)
        -- OR bsad.aedat >= (SELECT DATE_FORMAT(delta_from_date, 'yyyyMMdd')  -- aedat not available
    )
),
d_t052_distinct_cte AS (
  SELECT
    zterm,
    MAX(TRY_CAST(ztag1 AS INTEGER)) AS ztag1
  FROM
    master_data_conf.sapdl2008__t052
  GROUP BY
    ALL
),
d_material_cte AS (
  SELECT
    vbeln,
    CONCAT_WS(', ', COLLECT_LIST(DISTINCT matnr)) AS material_number
  FROM
    direct_sales_conf.sapdl2008__vbrp
  GROUP BY
    ALL
),
d_tariff_percentages_cte AS (
  SELECT
    bseg.belnr  AS belnr,
    bseg.bukrs  AS bukrs,
    bseg.gjahr  AS gjahr,
    MAX(vbrp.matnr) AS matnr
  FROM
    finance_conf.sapdl2008__bseg bseg
      LEFT JOIN direct_sales_conf.sapdl2008__vbrp vbrp
        ON vbrp.vbeln = bseg.kidno
        AND vbrp.matnr IN ('1-120037-2')
        AND vbrp.AUBEL = bseg.VBEL2
        AND vbrp.AUPOS = bseg.POSN2
  GROUP BY
    ALL
),
d_tariff_konv_cte AS (
  SELECT
    knumv,
    MAX(kbetr) AS kbetr
  FROM
    pricing_l0_raw.sapdl2008__konv
  WHERE
    kschl IN ('ZTRF', 'ZTRM')
  GROUP BY
    ALL
),
ref_gbl_product AS (
  SELECT
    PROD_CODE,
    PROD_BUSLN_FNCTN_ID
  FROM
    master_data_conf.gbl_current__gbl_product
  WHERE
    COALESCE(PROD_BUSLN_FNCTN_ID, '') != ''
),
ref_cbc_by_billing_doc AS (
  SELECT
    vbrp.vbeln,
    gp.PROD_BUSLN_FNCTN_ID AS competency_business_cde,
    ROW_NUMBER() OVER (PARTITION BY vbrp.vbeln ORDER BY ABS(vbrp.netwr) DESC) AS rn
  FROM
    direct_sales_conf.sapdl2008__vbrp vbrp
      INNER JOIN master_data_l0_raw.sapdl2008__mara mara
        ON vbrp.matnr = mara.matnr
      INNER JOIN ref_gbl_product gp
        ON mara.prdha = gp.PROD_CODE
),
ar_base AS (
  SELECT
    f_act_rec_inv_cte.bukrs                          AS company_code,
    f_act_rec_inv_cte.kunnr                          AS customer_account_number,
    f_act_rec_inv_cte.belnr                          AS accounting_document_id,
    f_act_rec_inv_cte.gjahr                          AS accounting_document_posting_fiscal_year_id,
    f_act_rec_inv_cte.buzei                          AS accounting_document_item_id,
    f_act_rec_inv_cte.blart                          AS accounting_document_type_code,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.bldat
    )                                                AS accounting_document_create_date,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.budat
    )                                                AS accounting_document_posting_date,
    f_act_rec_inv_cte.augbl                          AS accounting_document_item_clearing_document_id,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.augdt
    )                                                AS accounting_document_item_clearing_date,
    f_act_rec_inv_cte.shkzg                          AS accounting_document_item_credit_or_debit_code,
    f_act_rec_inv_cte.xzahl                          AS accounting_document_item_posting_key_used_in_payment_indicator,
    f_act_rec_inv_cte.vbeln                          AS accounting_document_item_invoice_document_id,
    f_act_rec_inv_cte.sgtxt                          AS accounting_document_item_text,
    finance_tbl.convert_sap_datestring_to_date(
      f_act_rec_inv_cte.zfbdt
    )                                                AS accounting_document_item_baseline_date,
    DATE_ADD(
      finance_tbl.convert_sap_datestring_to_date(
        IF(
          COALESCE(f_act_rec_inv_cte.zfbdt, '') IN ('', '00000000'),
          f_act_rec_inv_cte.bldat,
          f_act_rec_inv_cte.zfbdt
        )
      ),
      CASE
        WHEN
          f_act_rec_inv_cte.shkzg = 'H'
          AND COALESCE(f_bseg.rebzg, '') = ''
        THEN 0
        ELSE
          COALESCE(
            NULLIF(TRY_CAST(f_act_rec_inv_cte.zbd3t AS INTEGER), 0),
            NULLIF(TRY_CAST(f_act_rec_inv_cte.zbd2t AS INTEGER), 0),
            NULLIF(TRY_CAST(f_act_rec_inv_cte.zbd1t AS INTEGER), 0),
            d_t052_cte.ztag1,
            0
          )
      END
    )                                                AS accounting_document_item_due_date,
    f_act_rec_inv_cte.zterm                          AS accounting_document_item_payment_terms_code,
    f_act_rec_inv_cte.zbd1t                          AS accounting_document_item_cash_discount_days_quantity_1,
    f_act_rec_inv_cte.zbd2t                          AS accounting_document_item_cash_discount_days_quantity_2,
    f_bkpf.usnam                                     AS accounting_document_create_network_user_id,
    f_act_rec_inv_cte.waers                          AS accounting_document_item_document_currency_code,
    f_t001.waers                                     AS accounting_document_item_functional_currency_code,
    f_act_rec_inv_cte.rstgr                          AS accounting_document_item_payment_reason_code,
    f_act_rec_inv_cte.item_status                    AS accounting_document_item_clearing_status_name,
    COALESCE(f_bfod.prctr, f_bseg.prctr)            AS profit_center_id,
    d_ted_profit_center.ztedprc                      AS ted_profit_center_id,
    finance_tbl.convert_sap_datestring_to_date(
      d_ted_profit_center.eff_date
    )                                                AS ted_profit_center_effective_date,
    f_bfod.auzei                                     AS accounting_document_breakdown_item_id,
    IF(
      COALESCE(f_bfod.auzei, 1) = 1,
      finance_tbl.round_and_set_signal_amount(f_act_rec_inv_cte.wrbtr, f_act_rec_inv_cte.shkzg),
      0
    )                                                AS accounting_document_item_document_currency_amount,
    f_act_rec_inv_cte.saknr                          AS accouting_document_item_general_ledger_reconciliation_account_number,
    ROUND(
      COALESCE(
        finance_tbl.round_and_set_signal_amount(f_bfod.dmbtr, f_bfod.shkzg),
        finance_tbl.round_and_set_signal_amount(f_act_rec_inv_cte.dmbtr, f_act_rec_inv_cte.shkzg)
      )
        * POWER(10, 2 - COALESCE(f_tcurx_functional.currdec, 2)),
      3
    )                                                AS accounting_document_item_functional_currency_amount,
    CASE
      WHEN d_tk_cte.kbetr IS NOT NULL
      THEN COALESCE(d_tk_cte.kbetr / 10, IF(d_tp_cte.matnr IS NOT NULL, 100, 0))
      WHEN d_tp_cte.matnr IS NOT NULL THEN 100
      ELSE 0
    END                                              AS accounting_document_item_tariff_percentage,
    d_cs.customer_owning_business_unit_dsg_id,
    d_cs.sales_territory_owning_business_unit_dsg_id,
    d_cs.gam_global_business_unit_owning_business_unit_dsg_id,
    d_dc.company_dsg_id,
    d_cs.industry_business_code,
    f_t001.waers                                     AS company_currency_code,
    d_cer.currency_exchange_rate_to_USD_multiplier_factor,
    d_curr.exchange_rate,
    d_curr.from_currency_ratio,
    d_curr.target_currency_ratio,
    d_cbc.competency_business_cde                    AS resolved_cbc,
    d_pc_rels.MGE_PROFIT_CENTER_ABBREV_ID            AS override_bu_abbrev,
    d_pc_rels.SAP_PROFIT_CENTER_CDE                  AS override_sap_profit_center,
    d_bu.business_unit_id                            AS initial_business_unit_id,
    d_bu.business_unit_dsg_id,
    d_bu.business_unit_group_id,
    cust_lookup.customer_key_id,
    d_csa.sales_organization_id,
    d_csa.distribution_channel_code,
    d_csa.worldwide_customer_level_1_number,
    d_csa.worldwide_customer_level_1_label           AS worldwide_customer_level_1_name,
    d_csa.worldwide_customer_level_2_number,
    d_csa.worldwide_customer_level_2_label           AS worldwide_customer_level_2_name,
    d_csa.country_region_level_1_label               AS country_region_level_1_name,
    d_csa.country_region_level_2_label               AS country_region_level_2_name,
    d_csa.gam_level_3_assoc_full_name,
    d_csa.account_manager_level_6_assoc_full_name,
    d_csa.account_manager_level_7_assoc_full_name,
    d_csa.sales_territory_level_6_assoc_full_name,
    d_csa.sales_territory_level_7_assoc_full_name,
    d_csa.sales_office_code,
    d_csa.sales_office_label                         AS sales_office_name,
    d_csa.account_manager_level_1_assoc_full_name    AS account_manager_assoc_full_name,
    d_csa.sales_territory_level_1_assoc_full_name    AS sales_territory_assoc_full_name,
    d_cs.sensors_alternate_sales_territory_code,
    d_cs.sensors_alternate_sales_territory_name
  FROM
    f_accounts_receivable_invoice_cte f_act_rec_inv_cte
      LEFT JOIN finance_conf.sapdl2008__bseg f_bseg
        ON f_act_rec_inv_cte.belnr = f_bseg.belnr
        AND LTRIM('0', f_act_rec_inv_cte.buzei) = LTRIM('0', f_bseg.buzei)
        AND f_act_rec_inv_cte.bukrs = f_bseg.bukrs
        AND f_act_rec_inv_cte.gjahr = f_bseg.gjahr
      LEFT JOIN finance_conf.sapdl2008__bkpf f_bkpf
        ON f_act_rec_inv_cte.bukrs = f_bkpf.bukrs
        AND f_act_rec_inv_cte.belnr = f_bkpf.belnr
        AND f_act_rec_inv_cte.gjahr = f_bkpf.gjahr
      LEFT JOIN master_data_conf.sapdl2008__t001 f_t001
        ON f_act_rec_inv_cte.bukrs = f_t001.bukrs
      LEFT JOIN finance_l0_raw.sapdl2008__tcurx f_tcurx_functional
        ON f_tcurx_functional.currkey = f_t001.waers
      LEFT JOIN finance_l0_raw.sapdl2008__tcurx f_tcurx_document
        ON f_tcurx_document.currkey = f_act_rec_inv_cte.waers
      LEFT JOIN finance_tbl.dim_currencies_dates_rates d_curr
        ON d_curr.reference_date
          = finance_tbl.convert_sap_datestring_to_date(
            COALESCE(NULLIF(f_act_rec_inv_cte.augdt, 0), f_act_rec_inv_cte.budat)
          )
        AND d_curr.from_currency = f_t001.waers
      LEFT JOIN d_t052_distinct_cte d_t052_cte
        ON f_act_rec_inv_cte.zterm = d_t052_cte.zterm
      LEFT JOIN direct_sales_raw.sapdl2008__vbrk vbrk
        ON vbrk.vbeln = f_bseg.kidno
      LEFT JOIN d_tariff_konv_cte d_tk_cte
        ON d_tk_cte.knumv = vbrk.knumv
      LEFT JOIN d_tariff_percentages_cte d_tp_cte
        ON f_bkpf.belnr = d_tp_cte.belnr
        AND f_bkpf.bukrs = d_tp_cte.bukrs
        AND f_bkpf.gjahr = d_tp_cte.gjahr
      LEFT JOIN master_data_tbl.dim_customer d_cs
        ON d_cs.customer_account_number = REGEXP_REPLACE(f_act_rec_inv_cte.kunnr, '^0+', '')
        AND d_cs.customer_company_code = f_act_rec_inv_cte.bukrs
        AND d_cs.source_system_id = 1
        AND d_cs.customer_company_legacy_format_account_number_rank = 1
        AND d_cs.record_active_indicator = 'Y'
      LEFT JOIN master_data_tbl.dim_company d_dc
        ON d_dc.company_code = f_act_rec_inv_cte.bukrs
        AND d_dc.record_active_indicator = 'Y'
      LEFT JOIN finance_l0_raw.sapdl2008__bfod_a f_bfod
        ON f_bfod.belnr = f_act_rec_inv_cte.belnr
        AND f_bfod.buzei = f_act_rec_inv_cte.buzei
        AND f_bfod.gjahr = f_act_rec_inv_cte.gjahr
        AND f_bfod.bukrs = f_act_rec_inv_cte.bukrs
      LEFT JOIN master_data_tbl.dim_profit_center d_pc
        ON d_pc.profit_center_id = COALESCE(f_bfod.prctr, f_bseg.prctr)
        AND d_pc.record_active_indicator = 'Y'
      LEFT JOIN master_data_tbl.dim_business_unit d_bu
        ON d_pc.business_unit_id = d_bu.business_unit_id
        AND d_bu.record_active_indicator = 'Y'
      LEFT JOIN master_data_tbl.dim_currency_exchange_rate d_cer
        ON d_cer.currency_code = f_t001.waers
        AND d_cer.currency_exchange_rate_type_code = 1
        AND TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())) BETWEEN
          TO_DATE(d_cer.effective_from_date)
        AND
          TO_DATE(d_cer.effective_to_date)
      LEFT JOIN finance_l0_raw.sapdl2008__zpcafence d_ted_profit_center
        ON d_ted_profit_center.prctr = COALESCE(f_bfod.prctr, f_bseg.prctr)
        AND d_ted_profit_center.eff_date <= f_act_rec_inv_cte.budat
      LEFT JOIN ref_cbc_by_billing_doc d_cbc
        ON f_bseg.kidno = d_cbc.vbeln
        AND d_cbc.rn = 1
        AND COALESCE(f_bseg.kidno, '') != ''
      LEFT JOIN master_data_conf.gbl_current__gbl_mge_profit_center_rels d_pc_rels
        ON f_act_rec_inv_cte.bukrs = d_pc_rels.ORGANIZATION_ID
        AND d_cs.industry_business_code = d_pc_rels.INDUSTRY_BUSINESS_CDE
        AND d_cbc.competency_business_cde = d_pc_rels.COMPETENCY_BUSINESS_CDE
      LEFT JOIN LATERAL (
        SELECT
          customer_key_id
        FROM
          master_data_l1_curated.dim_customer_current
        WHERE
          submitted_customer_account_number = f_act_rec_inv_cte.kunnr
          AND customer_company_code = f_act_rec_inv_cte.bukrs
          AND source_system_id = 1
        ORDER BY
          distribution_channel_code ASC
        LIMIT 1
      ) cust_lookup
      LEFT JOIN master_data_l1_curated.dim_customer_current d_csa
        ON d_csa.customer_key_id = cust_lookup.customer_key_id
  QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY
          f_act_rec_inv_cte.bukrs,
          f_act_rec_inv_cte.kunnr,
          f_act_rec_inv_cte.belnr,
          f_act_rec_inv_cte.gjahr,
          f_act_rec_inv_cte.buzei,
          f_bfod.auzei
        ORDER BY
          d_ted_profit_center.eff_date DESC,
          IF(f_act_rec_inv_cte.item_status = 'CLEARED', 1, 2) ASC
      ) = 1
),
ar_final AS (
  SELECT
    b.company_code,
    b.customer_account_number,
    b.accounting_document_id,
    b.accounting_document_posting_fiscal_year_id,
    b.accounting_document_item_id,
    b.accounting_document_type_code,
    b.accounting_document_create_date,
    b.accounting_document_posting_date,
    b.accounting_document_item_clearing_document_id,
    b.accounting_document_item_clearing_date,
    b.accounting_document_item_credit_or_debit_code,
    b.accounting_document_item_posting_key_used_in_payment_indicator,
    b.accounting_document_item_invoice_document_id,
    b.accounting_document_item_text,
    b.accounting_document_item_baseline_date,
    b.accounting_document_item_due_date,
    b.accounting_document_item_payment_terms_code,
    b.accounting_document_item_cash_discount_days_quantity_1,
    b.accounting_document_item_cash_discount_days_quantity_2,
    b.accounting_document_create_network_user_id,
    b.accounting_document_item_document_currency_code,
    b.accounting_document_item_functional_currency_code,
    b.accounting_document_item_payment_reason_code,
    b.accounting_document_item_clearing_status_name,
    CASE
      WHEN
        b.resolved_cbc IS NOT NULL
        AND (
          COALESCE(b.initial_business_unit_id, '') IN ('', 'LGL', 'TMP', 'CRP')
          OR b.profit_center_id IS NULL
        )
        AND b.override_sap_profit_center IS NOT NULL
      THEN b.override_sap_profit_center
      WHEN
        b.resolved_cbc IS NULL
        AND COALESCE(b.initial_business_unit_id, '') IN ('LGL', 'TMP', 'CRP')
      THEN NULL
      ELSE b.profit_center_id
    END                                              AS ted_overriding_profit_center_id,
    b.profit_center_id,
    b.ted_profit_center_id,
    b.ted_profit_center_effective_date,
    b.accounting_document_breakdown_item_id,
    b.accounting_document_item_document_currency_amount,
    b.accouting_document_item_general_ledger_reconciliation_account_number,
    b.accounting_document_item_functional_currency_amount,
    COALESCE(
      finance_tbl.convert_amount_to_usd_currency(
        b.accounting_document_item_functional_currency_amount,
        b.exchange_rate,
        b.from_currency_ratio,
        b.target_currency_ratio
      ),
      b.accounting_document_item_functional_currency_amount
    )                                                AS accounting_document_item_actual_rate_functional_amount,
    b.accounting_document_item_tariff_percentage,
    admin.get_data_security_tag(
      ARRAY(
        COALESCE(b.customer_owning_business_unit_dsg_id, 0),
        COALESCE(b.sales_territory_owning_business_unit_dsg_id, 0),
        COALESCE(b.gam_global_business_unit_owning_business_unit_dsg_id, 0),
        COALESCE(b.company_dsg_id, 0),
        COALESCE(d_bu_final.business_unit_dsg_id, b.business_unit_dsg_id, 0)
      )
    )                                                AS data_security_tag_id,
    COALESCE(d_bu_final.business_unit_group_id, b.business_unit_group_id)
                                                     AS business_unit_group_id,
    b.accounting_document_item_functional_currency_amount
      * b.currency_exchange_rate_to_USD_multiplier_factor
                                                     AS accounting_document_item_budget_rate_amount,
    b.sales_organization_id,
    b.distribution_channel_code,
    b.worldwide_customer_level_1_number,
    b.worldwide_customer_level_1_name,
    b.worldwide_customer_level_2_number,
    b.worldwide_customer_level_2_name,
    b.country_region_level_1_name,
    b.country_region_level_2_name,
    b.gam_level_3_assoc_full_name,
    b.account_manager_level_6_assoc_full_name,
    b.account_manager_level_7_assoc_full_name,
    b.sales_territory_level_6_assoc_full_name,
    b.sales_territory_level_7_assoc_full_name,
    b.sales_office_code,
    b.sales_office_name,
    b.account_manager_assoc_full_name,
    b.sales_territory_assoc_full_name,
    b.sensors_alternate_sales_territory_code,
    b.sensors_alternate_sales_territory_name
  FROM
    f_accounts_receivable_invoice_cte f_act_rec_inv_cte
      LEFT JOIN finance_conf.sapdl2008__bseg f_bseg
        ON f_act_rec_inv_cte.belnr = f_bseg.belnr
        AND LTRIM('0', f_act_rec_inv_cte.buzei) = LTRIM('0', f_bseg.buzei)
        AND f_act_rec_inv_cte.bukrs = f_bseg.bukrs
        AND f_act_rec_inv_cte.gjahr = f_bseg.gjahr
      LEFT JOIN finance_conf.sapdl2008__bkpf f_bkpf
        ON f_act_rec_inv_cte.bukrs = f_bkpf.bukrs
        AND f_act_rec_inv_cte.belnr = f_bkpf.belnr
        AND f_act_rec_inv_cte.gjahr = f_bkpf.gjahr
      LEFT JOIN master_data_conf.sapdl2008__t001 f_t001
        ON f_act_rec_inv_cte.bukrs = f_t001.bukrs
      LEFT JOIN finance_l0_raw.sapdl2008__tcurx f_tcurx_functional
        ON f_tcurx_functional.currkey = f_t001.waers
      LEFT JOIN finance_l0_raw.sapdl2008__tcurx f_tcurx_document
        ON f_tcurx_document.currkey = f_act_rec_inv_cte.waers
      LEFT JOIN finance_tbl.dim_currencies_dates_rates d_curr
        ON d_curr.reference_date
          = finance_tbl.convert_sap_datestring_to_date(
            COALESCE(NULLIF(f_act_rec_inv_cte.augdt, 0), f_act_rec_inv_cte.budat)
          )
        AND d_curr.from_currency = f_t001.waers
      LEFT JOIN d_t052_distinct_cte d_t052_cte
        ON f_act_rec_inv_cte.zterm = d_t052_cte.zterm
      LEFT JOIN direct_sales_raw.sapdl2008__vbrk vbrk
        ON vbrk.vbeln = f_bseg.kidno
      LEFT JOIN d_tariff_konv_cte d_tk_cte
        ON d_tk_cte.knumv = vbrk.knumv
      LEFT JOIN d_tariff_percentages_cte d_tp_cte
        ON f_bkpf.belnr = d_tp_cte.belnr
        AND f_bkpf.bukrs = d_tp_cte.bukrs
        AND f_bkpf.gjahr = d_tp_cte.gjahr
      LEFT JOIN master_data_tbl.dim_customer d_cs
        ON d_cs.customer_account_number = REGEXP_REPLACE(f_act_rec_inv_cte.kunnr, '^0+', '')
        AND d_cs.customer_company_code = f_act_rec_inv_cte.bukrs
        AND d_cs.source_system_id = 1
        AND d_cs.customer_company_legacy_format_account_number_rank = 1
        AND d_cs.record_active_indicator = 'Y'
      LEFT JOIN master_data_tbl.dim_company d_dc
        ON d_dc.company_code = f_act_rec_inv_cte.bukrs
        AND d_dc.record_active_indicator = 'Y'
      LEFT JOIN finance_l0_raw.sapdl2008__bfod_a f_bfod
        ON f_bfod.belnr = f_act_rec_inv_cte.belnr
        AND f_bfod.buzei = f_act_rec_inv_cte.buzei
        AND f_bfod.gjahr = f_act_rec_inv_cte.gjahr
        AND f_bfod.bukrs = f_act_rec_inv_cte.bukrs
      LEFT JOIN master_data_tbl.dim_profit_center d_pc
        ON d_pc.profit_center_id = COALESCE(f_bfod.prctr, f_bseg.prctr)
        AND d_pc.record_active_indicator = 'Y'
      LEFT JOIN master_data_tbl.dim_business_unit d_bu
        ON d_pc.business_unit_id = d_bu.business_unit_id
        AND d_bu.record_active_indicator = 'Y'
      LEFT JOIN master_data_tbl.dim_currency_exchange_rate d_cer
        ON d_cer.currency_code = f_t001.waers
        AND d_cer.currency_exchange_rate_type_code = 1
        AND TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())) BETWEEN
          TO_DATE(d_cer.effective_from_date)
        AND
          TO_DATE(d_cer.effective_to_date)
      LEFT JOIN finance_l0_raw.sapdl2008__zpcafence d_ted_profit_center
        ON d_ted_profit_center.prctr = COALESCE(f_bfod.prctr, f_bseg.prctr)
        AND d_ted_profit_center.eff_date <= f_act_rec_inv_cte.budat
      LEFT JOIN ref_cbc_by_billing_doc d_cbc
        ON f_bseg.kidno = d_cbc.vbeln
        AND d_cbc.rn = 1
        AND COALESCE(f_bseg.kidno, '') != ''
      LEFT JOIN master_data_conf.gbl_current__gbl_mge_profit_center_rels d_pc_rels
        ON f_act_rec_inv_cte.bukrs = d_pc_rels.ORGANIZATION_ID
        AND d_cs.industry_business_code = d_pc_rels.INDUSTRY_BUSINESS_CDE
        AND d_cbc.competency_business_cde = d_pc_rels.COMPETENCY_BUSINESS_CDE
      LEFT JOIN LATERAL (
        SELECT
          customer_key_id
        FROM
          master_data_l1_curated.dim_customer_current
        WHERE
          submitted_customer_account_number = f_act_rec_inv_cte.kunnr
          AND customer_company_code = f_act_rec_inv_cte.bukrs
          AND source_system_id = 1
        ORDER BY
          distribution_channel_code ASC
        LIMIT 1
      ) cust_lookup
      LEFT JOIN master_data_l1_curated.dim_customer_current d_csa
        ON d_csa.customer_key_id = cust_lookup.customer_key_id
  QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY
          f_act_rec_inv_cte.bukrs,
          f_act_rec_inv_cte.kunnr,
          f_act_rec_inv_cte.belnr,
          f_act_rec_inv_cte.gjahr,
          f_act_rec_inv_cte.buzei,
          f_bfod.auzei
        ORDER BY
          d_ted_profit_center.eff_date DESC,
          IF(f_act_rec_inv_cte.item_status = 'CLEARED', 1, 2) ASC
      ) = 1
),
ar_final AS (
  SELECT
    b.company_code,
    b.customer_account_number,
    b.accounting_document_id,
    b.accounting_document_posting_fiscal_year_id,
    b.accounting_document_item_id,
    b.accounting_document_type_code,
    b.accounting_document_create_date,
    b.accounting_document_posting_date,
    b.accounting_document_item_clearing_document_id,
    b.accounting_document_item_clearing_date,
    b.accounting_document_item_credit_or_debit_code,
    b.accounting_document_item_posting_key_used_in_payment_indicator,
    b.accounting_document_item_invoice_document_id,
    b.accounting_document_item_text,
    b.accounting_document_item_baseline_date,
    b.accounting_document_item_due_date,
    b.accounting_document_item_payment_terms_code,
    b.accounting_document_item_cash_discount_days_quantity_1,
    b.accounting_document_item_cash_discount_days_quantity_2,
    b.accounting_document_create_network_user_id,
    b.accounting_document_item_document_currency_code,
    b.accounting_document_item_functional_currency_code,
    b.accounting_document_item_payment_reason_code,
    b.accounting_document_item_clearing_status_name,
    CASE
      WHEN
        b.resolved_cbc IS NOT NULL
        AND (
          COALESCE(b.initial_business_unit_id, '') IN ('', 'LGL', 'TMP', 'CRP')
          OR b.profit_center_id IS NULL
        )
        AND b.override_sap_profit_center IS NOT NULL
      THEN b.override_sap_profit_center
      WHEN
        b.resolved_cbc IS NULL
        AND COALESCE(b.initial_business_unit_id, '') IN ('LGL', 'TMP', 'CRP')
      THEN NULL
      ELSE b.profit_center_id
    END                                              AS ted_overriding_profit_center_id,
    b.profit_center_id,
    b.ted_profit_center_id,
    b.ted_profit_center_effective_date,
    b.accounting_document_breakdown_item_id,
    b.accounting_document_item_document_currency_amount,
    b.accouting_document_item_general_ledger_reconciliation_account_number,
    b.accounting_document_item_functional_currency_amount,
    COALESCE(
      finance_tbl.convert_amount_to_usd_currency(
        b.accounting_document_item_functional_currency_amount,
        b.exchange_rate,
        b.from_currency_ratio,
        b.target_currency_ratio
      ),
      b.accounting_document_item_functional_currency_amount
    )                                                AS accounting_document_item_actual_rate_functional_amount,
    b.accounting_document_item_tariff_percentage,
    admin.get_data_security_tag(
      ARRAY(
        COALESCE(b.customer_owning_business_unit_dsg_id, 0),
        COALESCE(b.sales_territory_owning_business_unit_dsg_id, 0),
        COALESCE(b.gam_global_business_unit_owning_business_unit_dsg_id, 0),
        COALESCE(b.company_dsg_id, 0),
        COALESCE(d_bu_final.business_unit_dsg_id, b.business_unit_dsg_id, 0)
      )
    )                                                AS data_security_tag_id,
    COALESCE(d_bu_final.business_unit_group_id, b.business_unit_group_id)
                                                     AS business_unit_group_id,
    b.accounting_document_item_functional_currency_amount
      * b.currency_exchange_rate_to_USD_multiplier_factor
                                                     AS accounting_document_item_budget_rate_amount,
    b.sales_organization_id,
    b.distribution_channel_code,
    b.worldwide_customer_level_1_number,
    b.worldwide_customer_level_1_name,
    b.worldwide_customer_level_2_number,
    b.worldwide_customer_level_2_name,
    b.country_region_level_1_name,
    b.country_region_level_2_name,
    b.gam_level_3_assoc_full_name,
    b.account_manager_level_6_assoc_full_name,
    b.account_manager_level_7_assoc_full_name,
    b.sales_territory_level_6_assoc_full_name,
    b.sales_territory_level_7_assoc_full_name,
    b.sales_office_code,
    b.sales_office_name,
    b.account_manager_assoc_full_name,
    b.sales_territory_assoc_full_name,
    b.sensors_alternate_sales_territory_code,
    b.sensors_alternate_sales_territory_name
  FROM
    ar_base b
      LEFT JOIN master_data_tbl.dim_profit_center d_pc_final
        ON d_pc_final.profit_center_id
          = CASE
            WHEN
              b.resolved_cbc IS NOT NULL
              AND (
                COALESCE(b.initial_business_unit_id, '') IN ('', 'LGL', 'TMP', 'CRP')
                OR b.profit_center_id IS NULL
              )
              AND b.override_sap_profit_center IS NOT NULL
            THEN b.override_sap_profit_center
            WHEN
              b.resolved_cbc IS NULL
              AND COALESCE(b.initial_business_unit_id, '') IN ('LGL', 'TMP', 'CRP')
            THEN NULL
            ELSE b.profit_center_id
          END
        AND d_pc_final.record_active_indicator = 'Y'
      LEFT JOIN master_data_tbl.dim_business_unit d_bu_final
        ON d_pc_final.business_unit_id = d_bu_final.business_unit_id
        AND d_bu_final.record_active_indicator = 'Y'
)

SELECT count(*) FROM ar_final

-- COMMAND ----------

select
DATA_FLOW_GROUP_ID,
TARGET_OBJ_SCHEMA,
TARGET_OBJ_NAME,
SECURITY_GROUP,
ROW_SECURITY_FLAG,
BU_SECURITY_FLAG,
INSERTED_BY,
UPDATED_BY,
INSERTED_TS,
UPDATED_TS,
PII_COLUMN_SECURITY_LIST,
COST_COLUMN_SECURITY_LIST
 from admin.data_flow_object_security_lookup where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_L1'

-- COMMAND ----------

insert into admin.data_flow_object_security_lookup (DATA_FLOW_GROUP_ID,
TARGET_OBJ_SCHEMA,
TARGET_OBJ_NAME,
SECURITY_GROUP,
ROW_SECURITY_FLAG,
BU_SECURITY_FLAG,
INSERTED_BY,
UPDATED_BY,
INSERTED_TS,
UPDATED_TS,
PII_COLUMN_SECURITY_LIST,
COST_COLUMN_SECURITY_LIST)
select "FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1" as DATA_FLOW_GROUP_ID,
"finance" as TARGET_OBJ_SCHEMA,
"fact_accounts_receivable_invoice_snapshot" as TARGET_OBJ_NAME,
SECURITY_GROUP,
ROW_SECURITY_FLAG,
BU_SECURITY_FLAG,
current_user() as INSERTED_BY,
current_user() as UPDATED_BY,
current_timestamp() as INSERTED_TS,
current_timestamp() as UPDATED_TS,
PII_COLUMN_SECURITY_LIST,
COST_COLUMN_SECURITY_LIST
 from admin.data_flow_object_security_lookup where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_L1'
