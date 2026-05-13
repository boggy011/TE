-- ============================================================================
-- Model:           04_ctas_fact_ar_sku_snapshot_open
-- Task:            Task 4 of 4 — rebuild L2 (open-only) from L1
-- ----------------------------------------------------------------------------
-- materialization: create_or_replace_table
-- target:          finance_tbl.fact_accounts_receivable_invoice_sku_snapshot_open
-- partition_by:    snapshot_date
-- cluster_by:      [snapshot_date, company_code]
-- runs_on:         every_day
-- parameters:      :as_of_date  (DATE)
-- depends_on:      [finance_tbl.fact_accounts_receivable_invoice_sku_snapshot]
-- description:     Derived view materialized as a Delta table for BI/reporting that only cares
--                  about open receivables. Schema identical to L1 (no extra columns), but ~85%
--                  smaller (~1.5B vs ~9.8B rows). Daily full rebuild from L1 keeps the
--                  framework SELECT-only constraint.
-- TODO[header-keys]: align key names with framework spec.
-- ============================================================================

select /*+ REPARTITION(200, snapshot_date) */
    *
from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
where snapshot_state = 'OPEN'
