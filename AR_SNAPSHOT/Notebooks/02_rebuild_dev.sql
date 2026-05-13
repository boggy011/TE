-- Databricks notebook source
-- DBTITLE 1,Task 2 dev — Rebuild L1 within 60-day window (FULL CTAS, framework tax)
-- ============================================================================
-- Mirrors framework DETAIL row at PRIORITY=2 in 00_register_pipeline.sql.
-- Reads / writes: finance_tbl.fact_accounts_receivable_invoice_sku_snapshot  (self)
-- Volume:  ~9.8B rows. Expensive. ~30-60 min wall time on the recommended cluster.
-- MUST run AFTER Task 1 (Task 1 archives the rows that are about to be excluded here)
-- and BEFORE Task 3 (Task 3 then appends today's slice).
-- Requires: spark.sql.adaptive.coalescePartitions.enabled = false  (set at cluster level)
-- ============================================================================

-- COMMAND ----------

-- DBTITLE 1,Parameter
create widget text as_of_date default '2026-05-14';

-- COMMAND ----------

-- DBTITLE 1,Preview the rebuild set
create or replace temp view task2_rebuild_preview as
select /*+ REPARTITION(400, snapshot_date) */
    *
from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
where snapshot_date >= date_sub(cast(:as_of_date as date), 60)
  and snapshot_date <  cast(:as_of_date as date);

select count(*) as rows_to_keep,
       min(snapshot_date) as kept_oldest,
       max(snapshot_date) as kept_newest
from task2_rebuild_preview;

-- COMMAND ----------

-- DBTITLE 1,Row counts by snapshot_date (sanity)
select snapshot_date, count(*) as row_count
from task2_rebuild_preview
group by snapshot_date
order by snapshot_date;
