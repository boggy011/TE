-- Databricks notebook source
-- DBTITLE 1,Task 5 — Data quality checks (NOT a framework DETAIL)
-- ============================================================================
-- 9 DQ checks (DQ-01..DQ-09) combined via UNION ALL. Each returns 1+ rows on
-- failure; healthy load returns zero rows total.
--
-- NOT REGISTERED in admin.data_flow_pb_detail — materialization = `test` does not
-- map to the framework's FULL / DELTA / SCD LOAD_TYPE matrix (those all WRITE to a
-- target table; a `test` writes nothing). Deployment options (see 00_register_pipeline.sql):
--   A) Run this notebook as a separate workflow task after the framework job;
--      fail the workflow when the result is non-empty.
--   B) Wrap in a CTAS to a dq_runs audit table → fits LOAD_TYPE = FULL, gives history.
-- ============================================================================

-- COMMAND ----------

-- DBTITLE 1,Parameter
create widget text as_of_date default '2026-05-14';

-- COMMAND ----------

-- DBTITLE 1,Run all 9 DQ checks
-- DQ-01: L1 row count today equals (currently-open in SKU) + (cleared-today in SKU)
select
    'DQ-01'                                                                 as check_id,
    'l1_row_count_mismatch'                                                 as check_name,
    'BLOCKING'                                                              as severity,
    cast(:as_of_date as date)                                               as run_date,
    cast(abs(l1_count - expected_count) as bigint)                          as violation_count,
    concat('l1_today_count=', l1_count, ', expected=', expected_count)      as sample_detail
from (
    select
        (select count(*)
           from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
          where snapshot_date = cast(:as_of_date as date))                  as l1_count,
        (
            (select count(*)
               from finance_tbl.fact_accounts_receivable_invoice_sku
              where accounting_document_item_clearing_status_name = 'OPEN')
          + (select count(*)
               from finance_tbl.fact_accounts_receivable_invoice_sku
              where accounting_document_item_clearing_status_name = 'CLEARED'
                and accounting_document_item_clearing_date = cast(:as_of_date as date))
        )                                                                   as expected_count
) counts
where l1_count <> expected_count

union all

-- DQ-02: L1 sum reconciles to upstream SKU (tolerance 0.01)
select
    'DQ-02', 'l1_open_sum_mismatch', 'BLOCKING', cast(:as_of_date as date),
    cast(1 as bigint),
    concat('l1_sum=', l1_sum, ', sku_sum=', sku_sum, ', diff=', l1_sum - sku_sum)
from (
    select
        (select coalesce(sum(material_open_functional_amount), 0)
           from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
          where snapshot_date  = cast(:as_of_date as date)
            and snapshot_state = 'OPEN')                                    as l1_sum,
        (select coalesce(sum(material_open_functional_amount), 0)
           from finance_tbl.fact_accounts_receivable_invoice_sku
          where accounting_document_item_clearing_status_name = 'OPEN')     as sku_sum
) sums
where abs(l1_sum - sku_sum) > 0.01

union all

-- DQ-03: per-row disjoint partition (within 0.01)
select
    'DQ-03', 'bucket_partition_violation', 'BLOCKING', cast(:as_of_date as date),
    cast(count(*) as bigint),
    concat(
        'sample_ad_id=',     cast(min(accounting_document_id) as string),
        ', sample_mat=',     cast(min(material_number)        as string),
        ', max_abs_diff=',   cast(max(abs(material_open_functional_amount
                                          - (material_current_due_functional_amount
                                             + material_past_due_functional_amount
                                             + material_disputed_functional_amount))) as string)
    )
from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
where snapshot_date  = cast(:as_of_date as date)
  and snapshot_state = 'OPEN'
  and abs(material_open_functional_amount
          - (material_current_due_functional_amount
             + material_past_due_functional_amount
             + material_disputed_functional_amount)) > 0.01
having count(*) > 0

union all

-- DQ-04: closed-once rule
select
    'DQ-04', 'closed_once_violation', 'BLOCKING', cast(:as_of_date as date),
    cast(count(*) as bigint),
    concat(
        'sample_ad_id=', cast(min(accounting_document_id) as string),
        ', sample_mat=', cast(min(material_number)        as string),
        ', max_dups=',   cast(max(cleared_count)          as string)
    )
from (
    select
        company_code, accounting_document_id,
        accounting_document_posting_fiscal_year_id, accounting_document_item_id,
        accounting_document_breakdown_item_id, billing_document_id,
        billing_document_item_id, material_number,
        count(*) as cleared_count
    from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
    where snapshot_state = 'CLEARED'
    group by
        company_code, accounting_document_id,
        accounting_document_posting_fiscal_year_id, accounting_document_item_id,
        accounting_document_breakdown_item_id, billing_document_id,
        billing_document_item_id, material_number
    having count(*) > 1
) duplicates
having count(*) > 0

union all

-- DQ-05: grain uniqueness
select
    'DQ-05', 'grain_uniqueness_violation', 'BLOCKING', cast(:as_of_date as date),
    cast(count(*) as bigint),
    concat(
        'sample_snap_date=', cast(min(snapshot_date) as string),
        ', sample_ad_id=',   cast(min(accounting_document_id) as string),
        ', max_dups=',       cast(max(dup_count)     as string)
    )
from (
    select
        snapshot_date, snapshot_state, company_code, accounting_document_id,
        accounting_document_posting_fiscal_year_id, accounting_document_item_id,
        accounting_document_breakdown_item_id, billing_document_id,
        billing_document_item_id, material_number,
        count(*) as dup_count
    from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
    group by
        snapshot_date, snapshot_state, company_code, accounting_document_id,
        accounting_document_posting_fiscal_year_id, accounting_document_item_id,
        accounting_document_breakdown_item_id, billing_document_id,
        billing_document_item_id, material_number
    having count(*) > 1
) dups
having count(*) > 0

union all

-- DQ-06: L2 row count = L1 OPEN row count
select
    'DQ-06', 'l2_l1_open_mismatch', 'BLOCKING', cast(:as_of_date as date),
    cast(abs(l1_open_count - l2_count) as bigint),
    concat('l1_open=', l1_open_count, ', l2=', l2_count)
from (
    select
        (select count(*) from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
           where snapshot_state = 'OPEN')                                    as l1_open_count,
        (select count(*) from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot_open) as l2_count
) counts
where l1_open_count <> l2_count

union all

-- DQ-07: retention
select
    'DQ-07', 'retention_violation', 'BLOCKING', cast(:as_of_date as date),
    cast(count(*) as bigint),
    concat(
        'oldest_snapshot_date=', cast(min(snapshot_date) as string),
        ', retention_boundary=', cast(date_sub(cast(:as_of_date as date), 60) as string)
    )
from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
where snapshot_date < date_sub(cast(:as_of_date as date), 60)
having count(*) > 0

union all

-- DQ-08: archive coverage (coarse — only fires at rollover boundary)
select
    'DQ-08', 'archive_no_inflow_today', 'BLOCKING', cast(:as_of_date as date),
    cast(1 as bigint),
    concat(
        'archive_rows_archived_today=', archive_inflow,
        ', l1_min_snapshot_date=',      cast(l1_min as string)
    )
from (
    select
        (select count(*)
           from iceberg_cold.fact_accounts_receivable_invoice_sku_snapshot_archive
          where archived_on_date = cast(:as_of_date as date))               as archive_inflow,
        (select min(snapshot_date)
           from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot)  as l1_min
) archive_status
where l1_min = date_sub(cast(:as_of_date as date), 60)
  and archive_inflow = 0

union all

-- DQ-09: freshness — today's load is present
select
    'DQ-09', 'todays_load_missing', 'BLOCKING', cast(:as_of_date as date),
    cast(1 as bigint),
    concat('today_count=', today_count)
from (
    select count(*) as today_count
      from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
     where snapshot_date = cast(:as_of_date as date)
) freshness
where today_count = 0;
