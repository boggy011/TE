-- ============================================================================
-- Model:           05_dq_ar_sku_snapshot
-- Task:            Data quality checks for the snapshot pipeline
-- ----------------------------------------------------------------------------
-- materialization: test
-- runs_on:         every_day  (after Tasks 1-4 complete)
-- parameters:      :as_of_date  (DATE)
-- depends_on:      [finance_tbl.fact_accounts_receivable_invoice_sku,
--                   finance_tbl.fact_accounts_receivable_invoice_sku_snapshot,
--                   finance_tbl.fact_accounts_receivable_invoice_sku_snapshot_open,
--                   iceberg_cold.fact_accounts_receivable_invoice_sku_snapshot_archive]
-- description:     Nine union-all'd checks. Each returns 0 rows when healthy or 1+ rows on
--                  failure. The orchestrator inspects the result directly and fails the pipeline
--                  on any non-empty row. No `dq_runs` audit table in this iteration.
-- ----------------------------------------------------------------------------
-- Output schema (consistent across all 9 checks):
--     check_id        string    e.g. 'DQ-03'
--     check_name      string    human-readable identifier
--     severity        string    'BLOCKING' or 'WARNING'
--     run_date        date      :as_of_date
--     violation_count bigint    number of violating rows
--     sample_detail   string    representative key for diagnosis
-- TODO[header-keys]:      align key names with framework spec.
-- TODO[iceberg-location]: confirm archive table reference.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- DQ-01: L1 row count today equals (currently-open in SKU) + (cleared-today in SKU)
-- ----------------------------------------------------------------------------
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

-- ----------------------------------------------------------------------------
-- DQ-02: L1 sum(material_open_functional_amount) reconciles to upstream SKU.
-- Tolerance 0.01 absorbs rounding differences from re-derivation.
-- ----------------------------------------------------------------------------
select
    'DQ-02'                                                                 as check_id,
    'l1_open_sum_mismatch'                                                  as check_name,
    'BLOCKING'                                                              as severity,
    cast(:as_of_date as date)                                               as run_date,
    cast(1 as bigint)                                                       as violation_count,
    concat('l1_sum=', l1_sum, ', sku_sum=', sku_sum, ', diff=', l1_sum - sku_sum) as sample_detail
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

-- ----------------------------------------------------------------------------
-- DQ-03: per-row disjoint partition: open = current_due + past_due + disputed (within 0.01)
-- ----------------------------------------------------------------------------
select
    'DQ-03'                                                                 as check_id,
    'bucket_partition_violation'                                            as check_name,
    'BLOCKING'                                                              as severity,
    cast(:as_of_date as date)                                               as run_date,
    cast(count(*) as bigint)                                                as violation_count,
    concat(
        'sample_ad_id=',     cast(min(accounting_document_id) as string),
        ', sample_mat=',     cast(min(material_number)        as string),
        ', max_abs_diff=',   cast(max(abs(material_open_functional_amount
                                          - (material_current_due_functional_amount
                                             + material_past_due_functional_amount
                                             + material_disputed_functional_amount))) as string)
    )                                                                       as sample_detail
from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
where snapshot_date  = cast(:as_of_date as date)
  and snapshot_state = 'OPEN'
  and abs(material_open_functional_amount
          - (material_current_due_functional_amount
             + material_past_due_functional_amount
             + material_disputed_functional_amount)) > 0.01
having count(*) > 0

union all

-- ----------------------------------------------------------------------------
-- DQ-04: closed-once rule. Each (AR PK + material) must appear at most once with state CLEARED.
-- Violations indicate a clearing date was re-snapshotted, which double-counts revenue rollups.
-- ----------------------------------------------------------------------------
select
    'DQ-04'                                                                 as check_id,
    'closed_once_violation'                                                 as check_name,
    'BLOCKING'                                                              as severity,
    cast(:as_of_date as date)                                               as run_date,
    cast(count(*) as bigint)                                                as violation_count,
    concat(
        'sample_ad_id=', cast(min(accounting_document_id) as string),
        ', sample_mat=', cast(min(material_number)        as string),
        ', max_dups=',   cast(max(cleared_count)          as string)
    )                                                                       as sample_detail
from (
    select
        company_code,
        accounting_document_id,
        accounting_document_posting_fiscal_year_id,
        accounting_document_item_id,
        accounting_document_breakdown_item_id,
        billing_document_id,
        billing_document_item_id,
        material_number,
        count(*) as cleared_count
    from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
    where snapshot_state = 'CLEARED'
    group by
        company_code,
        accounting_document_id,
        accounting_document_posting_fiscal_year_id,
        accounting_document_item_id,
        accounting_document_breakdown_item_id,
        billing_document_id,
        billing_document_item_id,
        material_number
    having count(*) > 1
) duplicates
having count(*) > 0

union all

-- ----------------------------------------------------------------------------
-- DQ-05: grain uniqueness. No duplicate (snapshot_date, snapshot_state, AR PK, material).
-- Defends against a buggy MERGE that inserts instead of updating.
-- ----------------------------------------------------------------------------
select
    'DQ-05'                                                                 as check_id,
    'grain_uniqueness_violation'                                            as check_name,
    'BLOCKING'                                                              as severity,
    cast(:as_of_date as date)                                               as run_date,
    cast(count(*) as bigint)                                                as violation_count,
    concat(
        'sample_snap_date=', cast(min(snapshot_date) as string),
        ', sample_ad_id=',   cast(min(accounting_document_id) as string),
        ', max_dups=',       cast(max(dup_count)     as string)
    )                                                                       as sample_detail
from (
    select
        snapshot_date,
        snapshot_state,
        company_code,
        accounting_document_id,
        accounting_document_posting_fiscal_year_id,
        accounting_document_item_id,
        accounting_document_breakdown_item_id,
        billing_document_id,
        billing_document_item_id,
        material_number,
        count(*) as dup_count
    from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
    group by
        snapshot_date,
        snapshot_state,
        company_code,
        accounting_document_id,
        accounting_document_posting_fiscal_year_id,
        accounting_document_item_id,
        accounting_document_breakdown_item_id,
        billing_document_id,
        billing_document_item_id,
        material_number
    having count(*) > 1
) dups
having count(*) > 0

union all

-- ----------------------------------------------------------------------------
-- DQ-06: L2 row count = L1 OPEN row count
-- ----------------------------------------------------------------------------
select
    'DQ-06'                                                                 as check_id,
    'l2_l1_open_mismatch'                                                   as check_name,
    'BLOCKING'                                                              as severity,
    cast(:as_of_date as date)                                               as run_date,
    cast(abs(l1_open_count - l2_count) as bigint)                           as violation_count,
    concat('l1_open=', l1_open_count, ', l2=', l2_count)                    as sample_detail
from (
    select
        (select count(*)
           from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
          where snapshot_state = 'OPEN')                                    as l1_open_count,
        (select count(*)
           from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot_open) as l2_count
) counts
where l1_open_count <> l2_count

union all

-- ----------------------------------------------------------------------------
-- DQ-07: retention. No L1 rows older than 60 days.
-- Aggregated to one row with the count of offending rows.
-- ----------------------------------------------------------------------------
select
    'DQ-07'                                                                 as check_id,
    'retention_violation'                                                   as check_name,
    'BLOCKING'                                                              as severity,
    cast(:as_of_date as date)                                               as run_date,
    cast(count(*) as bigint)                                                as violation_count,
    concat(
        'oldest_snapshot_date=', cast(min(snapshot_date) as string),
        ', retention_boundary=', cast(date_sub(cast(:as_of_date as date), 60) as string)
    )                                                                       as sample_detail
from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
where snapshot_date < date_sub(cast(:as_of_date as date), 60)
having count(*) > 0

union all

-- ----------------------------------------------------------------------------
-- DQ-08: archive coverage (coarse). Fires when retention IS in effect (L1's oldest snapshot_date
-- is at the boundary) but today's archive batch is empty — likely Task 1 silently failed.
-- ----------------------------------------------------------------------------
select
    'DQ-08'                                                                 as check_id,
    'archive_no_inflow_today'                                               as check_name,
    'BLOCKING'                                                              as severity,
    cast(:as_of_date as date)                                               as run_date,
    cast(1 as bigint)                                                       as violation_count,
    concat(
        'archive_rows_archived_today=', archive_inflow,
        ', l1_min_snapshot_date=',      cast(l1_min as string)
    )                                                                       as sample_detail
from (
    select
        (select count(*)
           from iceberg_cold.fact_accounts_receivable_invoice_sku_snapshot_archive
          where archived_on_date = cast(:as_of_date as date))               as archive_inflow,
        (select min(snapshot_date)
           from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot)  as l1_min
) archive_status
-- Only fire when L1's oldest day is exactly at the retention boundary (= rollover is happening).
-- During the first 60 days of operations, there's nothing to archive yet, so suppress the check.
where l1_min = date_sub(cast(:as_of_date as date), 60)
  and archive_inflow = 0

union all

-- ----------------------------------------------------------------------------
-- DQ-09: freshness. Today's load lands in L1.
-- ----------------------------------------------------------------------------
select
    'DQ-09'                                                                 as check_id,
    'todays_load_missing'                                                   as check_name,
    'BLOCKING'                                                              as severity,
    cast(:as_of_date as date)                                               as run_date,
    cast(1 as bigint)                                                       as violation_count,
    concat('today_count=', today_count)                                     as sample_detail
from (
    select count(*) as today_count
      from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
     where snapshot_date = cast(:as_of_date as date)
) freshness
where today_count = 0
