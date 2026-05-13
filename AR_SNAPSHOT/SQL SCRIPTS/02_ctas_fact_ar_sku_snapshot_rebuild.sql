-- ============================================================================
-- Model:           02_ctas_fact_ar_sku_snapshot_rebuild
-- Task:            Task 2 of 4 — daily rebuild of L1 retaining only the 60-day window
-- ----------------------------------------------------------------------------
-- materialization: create_or_replace_table
-- target:          finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
-- partition_by:    snapshot_date
-- cluster_by:      [snapshot_date, company_code]
-- runs_on:         every_day
-- parameters:      :as_of_date  (DATE)
-- depends_on:      [finance_tbl.fact_accounts_receivable_invoice_sku_snapshot]
-- description:     This is the "framework tax": because the orchestrator allows SELECT only,
--                  removing stale rows requires producing the entire table again without them.
--                  Filter keeps snapshot_date within [as_of_date - 60, as_of_date), i.e. the
--                  retention window but EXCLUDING today (Task 3 merges today's slice afterwards).
--                  ~9.8B rows daily. The expensive task; expect ~30-60 min wall clock on the
--                  recommended cluster.
-- ----------------------------------------------------------------------------
-- Run order: Task 1 must succeed first (otherwise stale rows are lost without being archived).
--            Task 3 must follow (otherwise today's slice never lands in L1).
-- TODO[header-keys]: align key names with framework spec.
-- TODO[aqe-coalesce]: requires `spark.sql.adaptive.coalescePartitions.enabled=false` at the
--                     cluster level so REPARTITION(400) is honored, not collapsed by AQE.
-- ============================================================================

select /*+ REPARTITION(400, snapshot_date) */
    *
from finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
where snapshot_date >= date_sub(cast(:as_of_date as date), 60)
  and snapshot_date <  cast(:as_of_date as date)
