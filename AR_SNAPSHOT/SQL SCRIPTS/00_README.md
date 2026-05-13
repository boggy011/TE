# AR SKU Snapshot Framework — Implementation

Daily snapshot of `fact_accounts_receivable_invoice_sku` at AR × material × snapshot_date grain,
with 60-day retention enforced by SELECT-only pattern, plus Iceberg cold archive and an open-only L2.

## File index and run order

| Order | File | Materialization | Reads | Writes |
|---|---|---|---|---|
| (foundation) | `fact_accounts_receivable_invoice_sku.sql` | `create_or_replace_table` | AR fact, VBRP, dim_material | `finance_tbl.fact_accounts_receivable_invoice_sku` |
| 1 | `01_inc_fact_ar_sku_snapshot_archive.sql` | `incremental_merge` (UniForm) | snapshot L1 | `iceberg_cold.fact_accounts_receivable_invoice_sku_snapshot_archive` |
| 2 | `02_ctas_fact_ar_sku_snapshot_rebuild.sql` | `create_or_replace_table` | snapshot L1 (self) | snapshot L1 |
| 3 | `03_inc_fact_ar_sku_snapshot_append_today.sql` | `incremental_merge` | SKU | snapshot L1 |
| 4 | `04_ctas_fact_ar_sku_snapshot_open.sql` | `create_or_replace_table` | snapshot L1 | snapshot_open L2 |
| 5 | `05_dq_ar_sku_snapshot.sql` | `test` | all of the above | (rows-on-failure stream) |

**Strict ordering required.** Task 1 must complete before Task 2 (otherwise stale rows are lost without
being archived). Task 3 must run after Task 2 (otherwise today's rows would be wiped by the rebuild).
Task 4 runs after Tasks 1–3. Task 5 runs last, after L1 and L2 are both fresh.

## Parameter conventions

Every snapshot model takes one runtime parameter, `:as_of_date` (a `DATE`). The orchestrator binds
this to the fiscal run date — typically today, but the model SQLs are written so that any
`:as_of_date` would work consistently (even though backfill is out of scope for this iteration).

`current_date()` is **not** used anywhere in the snapshot pipeline. Only the upstream SKU view uses
it (for the live aging bucket classification). The snapshot re-derives those classifications against
`:as_of_date` to preserve as-of-snapshot semantics.

## Retention

Hardcoded to 60 days. Boundary is `date_sub(cast(:as_of_date as date), 60)` everywhere.
If the retention window changes, search-and-replace that expression across files 01, 02, and 05.

## The framework tax (read this first)

The orchestration framework accepts only SELECT statements. Removing stale rows requires producing
the table without them via CTAS. We rewrite ~9.8B rows daily to drop ~165M stale ones. This is
unavoidable under the current framework. Cost numbers from production runs should be tracked as the
business case for a future `retention_clause` framework enhancement (which would eliminate Task 2
and reduce the daily pipeline from ~90 min to ~15 min).

## Closed-once rule

- **OPEN** rows are snapshotted every fiscal day until they clear.
- **CLEARED** rows are snapshotted exactly once, on the day they cleared.

Implementation lives in Task 3 (`03_inc_fact_ar_sku_snapshot_append_today.sql`) as a `UNION ALL` of
two branches: open as-of `:as_of_date`, and cleared where `clearing_date = :as_of_date`. The
unique_key includes `snapshot_state`, so re-running Task 3 for the same day is idempotent.

## Disjoint dispute partition (Option A)

For OPEN rows: `material_open_functional_amount = material_current_due + material_past_due + material_disputed`.
Disputed wins over past_due (matches SAP FSCM convention). DQ-03 validates this arithmetic.

Dispute is identified by `length(accounting_document_item_payment_reason_code) = 2` (BSEG-RSTGR
convention; 2-character reason codes are the org's dispute markers).

## Snapshot grain (unique_key)

```
[snapshot_date,
 snapshot_state,
 company_code,
 accounting_document_id,
 accounting_document_posting_fiscal_year_id,
 accounting_document_item_id,
 accounting_document_breakdown_item_id,
 billing_document_id,
 billing_document_item_id,
 material_number]
```

The same key is used by Task 1 (archive merge), Task 3 (append merge), DQ-04, and DQ-05.

## Header metadata convention

Every SQL model begins with a fenced metadata block:

```sql
-- ============================================================================
-- Model:           <file basename without extension>
-- Task:            <human description>
-- ----------------------------------------------------------------------------
-- materialization: <create_or_replace_table | incremental_merge | test>
-- target:          <fully.qualified.target_table>
-- table_format:    <delta | uniform_iceberg>          -- where applicable
-- partition_by:    <column>                            -- where applicable
-- cluster_by:      [<col1>, <col2>]                    -- where applicable
-- unique_key:      [<col1>, ...]                       -- merge only
-- runs_on:         every_day
-- parameters:      :as_of_date
-- depends_on:      [<table1>, <table2>]
-- description:     <one-line summary>
-- ============================================================================
```

**`TODO[header-keys]`** is grep-able. Once the framework's exact key spelling is confirmed, do a
project-wide search-and-replace and remove these markers.

## Materialization names used

| Name | Effect | Used by |
|---|---|---|
| `create_or_replace_table` | Full CTAS of the target | sku, 02, 04 |
| `incremental_merge` | `MERGE INTO target USING <select> ON unique_key` | 01, 03 |
| `test` | SELECT that must return zero rows for a healthy load | 05 |

**`TODO[header-keys]`**: confirm these names against framework spec; align if different.

## Optimization decisions

- `REPARTITION(N, snapshot_date)` hint applied to every snapshot SELECT to align writer
  parallelism with partition boundaries. `N` is tuned per file based on volume:
  - Task 1 (archive, ~165M rows): `REPARTITION(50, snapshot_date)`
  - Task 2 (rebuild, ~9.8B rows): `REPARTITION(400, snapshot_date)`
  - Task 3 (append, ~165M rows): `REPARTITION(50, snapshot_date)`
  - Task 4 (open L2, ~1.5B rows): `REPARTITION(200, snapshot_date)`
- No `BROADCAST` hints because the snapshot pipeline does not join to dim_date or any other small
  reference table. (If we add such joins later, broadcast them explicitly.)
- `spark.sql.adaptive.coalescePartitions.enabled = false` must be set at the **cluster** level
  because the framework does not allow `SET` statements at the top of model SELECTs. Ask the
  platform team to apply this as a job-cluster Spark config.

## Target table properties (one-time setup)

Apply to `finance_tbl.fact_accounts_receivable_invoice_sku_snapshot` before first run:

```
delta.dataSkippingNumIndexedCols = 8
delta.tuneFileSizesForRewrites   = true
delta.autoOptimize.optimizeWrite = true
delta.autoOptimize.autoCompact   = false
delta.checkpointInterval         = 100
```

Apply to `finance_tbl.fact_accounts_receivable_invoice_sku_snapshot_open`:

```
delta.dataSkippingNumIndexedCols = 8
delta.autoOptimize.optimizeWrite = true
```

## Cluster recommendation

- Storage-optimized instances with local NVMe (AWS i3/i4i, Azure Lsv3 family)
- ~20–30 workers (one worker per 50–100M rows on the Task 2 rewrite)
- Photon enabled
- DBR 14.3 LTS or above

## Iceberg archive

**`TODO[iceberg-location]`**: confirm catalog, schema, and external storage URL for the cold
archive. The placeholder used in this iteration is `iceberg_cold.fact_accounts_receivable_invoice_sku_snapshot_archive`.
Prerequisites for first run:

- Unity Catalog external location exists and is bound to the target storage
- Storage credential exists with WRITE permission on that location
- UniForm Delta is supported on the target DBR (write as Delta, expose Iceberg metadata)

## Data quality

All 9 checks live in `05_dq_ar_sku_snapshot.sql`, combined via `UNION ALL`. Each check returns one
row per failure; a healthy load returns zero rows total. There is **no persistent `dq_runs` audit
table** in this iteration — the orchestrator is expected to inspect the result of the `test`
materialization directly and fail the pipeline on any non-empty row.

| ID | What it checks | Type |
|---|---|---|
| DQ-01 | L1 row count today equals expected (open + cleared-today) | row-count |
| DQ-02 | L1 sum of `material_open_functional_amount` reconciles to upstream SKU | sum-reconcile |
| DQ-03 | `material_open = material_current_due + material_past_due + material_disputed` per row | per-row arithmetic |
| DQ-04 | Each (AR PK + material) appears at most once with `snapshot_state = 'CLEARED'` | closed-once |
| DQ-05 | No duplicate (snapshot_date, snapshot_state, AR PK, material) rows | grain uniqueness |
| DQ-06 | L2 row count = L1 OPEN row count | L2 consistency |
| DQ-07 | No L1 rows older than 60 days | retention |
| DQ-08 | Archive has rows for the most recent boundary day | archive coverage (coarse) |
| DQ-09 | Today's load is present in L1 | freshness |

DQ-08 is intentionally coarse without an audit table. It catches gross failures (Task 1 archived
zero rows when it should have archived ~165M) but not subtle row-level loss. Strengthening it
requires a `dq_runs`-style history table.

## Prerequisites before first run

1. AR fact `finance_tbl.fact_accounts_receivable_invoice` exists and is fresh for `:as_of_date`
2. SKU view `finance_tbl.fact_accounts_receivable_invoice_sku` is built (this repo's first file)
3. Iceberg archive location is created and accessible
4. Cluster is provisioned per the recommendation above
5. Job-cluster Spark config has AQE coalesce disabled
6. Target tables `..._snapshot` and `..._snapshot_open` either don't exist yet (framework will
   create on first run) or exist with the table properties listed above
7. First-run bootstrap note: on day 1, Task 1 and Task 2 read an empty L1 (or a non-existent one,
   depending on framework first-run semantics) and produce empty outputs; only Task 3 writes real
   data. By day 61, all four tasks contribute non-trivially.

## TODO markers in the code

Grep these in the SQL files; each is a known unknown to revisit before deployment.

| Marker | Meaning |
|---|---|
| `TODO[header-keys]` | Confirm framework's exact header key spelling |
| `TODO[iceberg-location]` | Confirm catalog/schema/storage for the cold archive |
| `TODO[aqe-coalesce]` | Confirm cluster-level disable of AQE coalesce |
| `TODO[unknown-sku-fallback]` | Decide whether to implement `__UNKNOWN_SKU__` synthetic-material fallback for AR rows without VBRP match (currently dropped via INNER JOIN) |
