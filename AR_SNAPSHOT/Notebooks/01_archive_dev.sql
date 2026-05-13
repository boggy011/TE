-- Databricks notebook source
-- DBTITLE 1,Task 1 dev — Archive stale rows (DELTA merge to Iceberg)
-- ============================================================================
-- Mirrors framework DETAIL row at PRIORITY=1 in 00_register_pipeline.sql.
-- Reads:   finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
-- Writes:  iceberg_cold.fact_accounts_receivable_invoice_sku_snapshot_archive  (TODO[iceberg-location])
-- Volume:  ~165M rows/day
-- MUST run before Task 2. If Task 2 rewrites first, stale rows are lost without archiving.
-- ============================================================================

-- COMMAND ----------

-- DBTITLE 1,Parameter
create widget text as_of_date default '2026-05-14';

-- COMMAND ----------

-- DBTITLE 1,Preview the archive batch
create or replace temp view task1_archive_preview as
select /*+ REPARTITION(50) */
    s.*,
    cast(:as_of_date as date) as archived_on_date
from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot s
where s.snapshot_date < date_sub(cast(:as_of_date as date), 60);

select count(*) as rows_to_archive,
       min(snapshot_date) as oldest,
       max(snapshot_date) as newest
from task1_archive_preview;

-- COMMAND ----------

-- DBTITLE 1,Spot check — first 100 rows
select * from task1_archive_preview limit 100;
