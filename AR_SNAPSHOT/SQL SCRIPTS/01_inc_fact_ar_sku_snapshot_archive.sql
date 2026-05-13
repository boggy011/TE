-- ============================================================================
-- Model:           01_inc_fact_ar_sku_snapshot_archive
-- Task:            Task 1 of 4 — archive rows aging out of the 60-day retention window
-- ----------------------------------------------------------------------------
-- materialization: incremental_merge
-- target:          iceberg_cold.fact_accounts_receivable_invoice_sku_snapshot_archive
-- table_format:    uniform_iceberg
-- partition_by:    snapshot_date
-- unique_key:      [snapshot_date,
--                   snapshot_state,
--                   company_code,
--                   accounting_document_id,
--                   accounting_document_posting_fiscal_year_id,
--                   accounting_document_item_id,
--                   accounting_document_breakdown_item_id,
--                   billing_document_id,
--                   billing_document_item_id,
--                   material_number]
-- runs_on:         every_day
-- parameters:      :as_of_date  (DATE)
-- depends_on:      [finance_tbl.fact_accounts_receivable_invoice_sku_snapshot]
-- description:     Move stale L1 rows (older than 60 days) to the Iceberg cold archive BEFORE
--                  Task 2 rewrites L1 without them. Idempotent re-run safe via unique_key.
--                  Adds an `archived_on_date` audit column so the archive carries when each
--                  row left the hot table.
-- TODO[header-keys]:     align key names with framework spec.
-- TODO[iceberg-location]: confirm catalog/schema/storage URL for the cold archive.
-- ============================================================================

select /*+ REPARTITION(50) */
    s.*,
    cast(:as_of_date as date) as archived_on_date
from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot s
where s.snapshot_date < date_sub(cast(:as_of_date as date), 60)
