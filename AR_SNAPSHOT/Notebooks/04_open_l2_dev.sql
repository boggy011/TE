-- Databricks notebook source
-- DBTITLE 1,Task 4 dev — Rebuild L2 open-only (FULL CTAS)
-- ============================================================================
-- Mirrors framework DETAIL row at PRIORITY=4 in 00_register_pipeline.sql.
-- Reads:   finance_tbl.fact_accounts_receivable_invoice_sku_snapshot  (the just-completed L1)
-- Writes:  finance_tbl.fact_accounts_receivable_invoice_sku_snapshot_open
-- Volume:  ~1.5B rows (~15% of L1)
-- Schema identical to L1; pure filter on snapshot_state = 'OPEN'.
-- Runs AFTER Tasks 1-3 finish (depends on L1 being fresh).
-- ============================================================================

-- COMMAND ----------

-- DBTITLE 1,Parameter
create widget text as_of_date default '2026-05-14';

-- COMMAND ----------

-- DBTITLE 1,Preview L2 open
create or replace temp view task4_open_preview as
select /*+ REPARTITION(200, snapshot_date) */
    *
from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
where snapshot_state = 'OPEN';

select count(*) as l2_open_count
from task4_open_preview;

-- COMMAND ----------

-- DBTITLE 1,Compare to L1 OPEN count (matches DQ-06)
select
  (select count(*) from task4_open_preview)                                                                 as l2_count,
  (select count(*) from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot where snapshot_state='OPEN') as l1_open_count;
