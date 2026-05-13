# AR SKU Snapshot Framework — Session Handoff

**Status**: Design finalized, ready to code.
**Word doc**: `AR_SKU_Snapshot_Framework.docx` (21 pages, contains the full technical design)
**Next session goal**: Update the upstream SKU base query with dispute logic, then write the 6 SQL deliverable files.

---

## 1. Project context (one-paragraph version)

We're building a snapshot framework on top of an existing AR-with-material fact table in Databricks. Two upstream Databricks jobs (out of scope) maintain the source tables daily: an AR invoice fact, and a SKU explosion of that fact with pro-rata distribution. Our framework consumes the SKU table, adds 60-day historical snapshots with closed-once / open-each-day semantics, archives stale rows to Iceberg storage, and produces an open-only derived table. The whole pipeline must operate under a SELECT-only framework constraint that cannot DELETE rows, which forces a daily full-rewrite pattern (the "framework tax").

---

## 2. The framework constraint (read this first)

The orchestration framework accepts **only SELECT statements**. Capabilities:

| Can do | Cannot do |
|---|---|
| SELECT with CTEs, JOINs, hints | DELETE FROM ... WHERE ... |
| CREATE OR REPLACE TABLE (full CTAS) | INSERT INTO ... REPLACE WHERE ... |
| MERGE INTO target USING <select> ON unique_key | Direct partition drops |
| View registration | DDL (CREATE TABLE, ALTER TABLE) |
| | Procedural code, multi-statement transactions |
| | Anything that isn't a SELECT |

**Consequence (the framework tax)**: Removing rows requires producing the table without them via CTAS. We rewrite ~9.8B rows daily to remove ~165M stale rows. This is ~60x I/O amplification per net change. Unavoidable under current framework; documented as the business case for a future framework enhancement.

**Path A decision**: Accept the cost and ship. Apply optimizations within the constraint. Track daily cost numbers as evidence for framework enhancement request.

---

## 3. The architecture (final)

### Tables (in our scope)

| Table | Purpose | How it's built |
|---|---|---|
| `finance_tbl.fact_accounts_receivable_invoice_sku_snapshot` | L1 snapshot, AR x material grain, open + closed, 60-day retention, partitioned by snapshot_date, liquid clustered | Daily three-task pipeline (archive / rebuild / append) |
| `finance_tbl.fact_accounts_receivable_invoice_sku_snapshot_open` | L2, open rows only, retention inherited from L1 | Single CTAS daily with `WHERE snapshot_state = 'OPEN'` |
| `iceberg_cold.fact_accounts_receivable_invoice_sku_snapshot_archive` | Cold archive for stale rows past 60 days | Daily incremental MERGE (UniForm Delta + Iceberg metadata) |

### Upstream (NOT in our scope)

- `finance_tbl.fact_accounts_receivable_invoice` — AR invoice fact (built from BSID/BSAD + enrichment, by another job)
- `finance_tbl.fact_accounts_receivable_invoice_sku` — AR + material grain explosion with pro-rata (our consumption point; built by another job)
- `master_data_tbl.dim_date` — fiscal calendar

### Why one snapshot table, not two

Earlier iterations had separate L1 (invoice grain) and L2 (SKU grain) snapshot tables. Final design uses one physical snapshot table (sourced from SKU) plus one derived open-only table. Reasons:
- Single source of truth (L1 and L2 cannot disagree)
- Halves the framework tax (one full rewrite per day, not two)
- Storage efficiency (open is a subset of full; no duplication)
- Simpler dependency graph

---

## 4. The closed-once rule

Open rows snapshotted every fiscal day until they clear. Cleared rows snapshotted ONCE on the day they cleared. This produces ~10B rows steady state in L1 instead of ~66B with naive every-row-every-day. The rule is implemented in Task 3 via:

```sql
-- OPEN branch: all currently-open rows for today
SELECT ... WHERE clearing_status = 'OPEN' AS-OF :as_of_date

UNION ALL

-- CLEARED branch: only items that cleared today
SELECT ... WHERE clearing_status = 'CLEARED' AND clearing_date = :as_of_date
```

The unique_key includes `snapshot_state` so the framework's MERGE upsert is idempotent — re-running for the same day produces the same result.

---

## 5. Volume sizing

| Component | Value | Notes |
|---|---|---|
| AR fact rows today | 364,278,372 | confirmed |
| SKU fan-out factor | ~3x assumed | **PENDING confirmation**: run `SELECT COUNT(*) FROM fact_accounts_receivable_invoice_sku` |
| SKU table rows today | ~1.1B | derived |
| % open at any moment | ~15% | typical AR ledger |
| Open rows per snapshot day | ~165M | 15% × 1.1B |
| Closed-once steady state | ~10-11B | 60-day window |
| Naive every-row-every-day | ~66B | rejected; closed-once is 6x cheaper |

**Daily wall-clock estimate** (20-30 worker storage-optimized cluster):

| Task | Time |
|---|---|
| Task 1: Archive stale | 5-10 min |
| Task 2: Rebuild without stale (framework tax) | 30-60 min |
| Task 3: Append today | 3-5 min |
| Task 4: Rebuild open-only L2 | 10-15 min |
| **Total** | **~50-90 min** |

---

## 6. The four daily tasks

### Task 1 — Archive stale rows
- **SELECT source**: `fact_accounts_receivable_invoice_sku_snapshot`
- **Filter**: `snapshot_date < date_sub(:run_date, 60)`
- **Target**: `iceberg_cold.fact_accounts_receivable_invoice_sku_snapshot_archive`
- **Materialization**: `incremental_merge` (UniForm Delta with Iceberg metadata)
- **Volume**: ~165M rows daily
- **MUST RUN FIRST** — once Task 2 rebuilds without stale rows, the stale data is gone

### Task 2 — Rebuild without stale (the framework tax)
- **SELECT source**: `fact_accounts_receivable_invoice_sku_snapshot` (reads itself)
- **Filter**: `snapshot_date >= date_sub(:run_date, 60) AND snapshot_date < :run_date`
- **Target**: same table (full replacement)
- **Materialization**: `create_or_replace_table` (full CTAS)
- **Volume**: ~9.8B rows read and written
- **This is the expensive task**. Apply all Path A optimizations.
- The filter excludes today's rows because Task 3 will add them.

### Task 3 — Append today's snapshot
- **SELECT source**: `fact_accounts_receivable_invoice_sku` (upstream SKU table)
- **Two branches via UNION ALL**:
  - OPEN: `clearing_status = OPEN as-of :as_of_date`
  - CLEARED: `clearing_status = CLEARED AND clearing_date = :as_of_date`
- **Target**: `fact_accounts_receivable_invoice_sku_snapshot`
- **Materialization**: `incremental_merge` (framework handles the upsert)
- **Unique key**: `[snapshot_date, snapshot_state, company_code, accounting_document_id, accounting_document_posting_fiscal_year_id, accounting_document_item_id, accounting_document_breakdown_item_id, billing_document_id, billing_document_item_id, material_number]`
- **Volume**: ~165M rows daily

### Task 4 — Rebuild open-only L2
- **SELECT source**: `fact_accounts_receivable_invoice_sku_snapshot` (the just-updated L1)
- **Filter**: `WHERE snapshot_state = 'OPEN'`
- **Target**: `fact_accounts_receivable_invoice_sku_snapshot_open`
- **Materialization**: `create_or_replace_table`
- **Volume**: ~1.5B rows (15% of L1)
- **Runs after L1 chain completes**

---

## 7. Path A optimizations (apply to every model)

### REPARTITION hint on the SELECT

```sql
select /*+ REPARTITION(400, snapshot_date) */
    ...
```

For Task 2 (9.8B rows across 60 partitions): target 400. Each writer handles ~7 partitions, ~25M rows.

### BROADCAST hint for dim_date

```sql
select /*+ BROADCAST(d), REPARTITION(400, snapshot_date) */
    ...
from snapshot_table s
join master_data_tbl.dim_date d on d.calendar_date = s.snapshot_date
```

Dim_date is tiny; explicit broadcast survives stale stats.

### Disable AQE coalesce (if framework allows SET)

```sql
set spark.sql.adaptive.coalescePartitions.enabled = false;
```

If SET isn't allowed, request platform team to set cluster-wide.

### Liquid clustering on the snapshot table

```yaml
# in model header
cluster_by: [snapshot_date, company_code]
```

Applied at write time; no separate OPTIMIZE pass needed.

### Target table properties (one-time setup)

```
delta.dataSkippingNumIndexedCols = 8
delta.tuneFileSizesForRewrites = true
delta.autoOptimize.optimizeWrite = true
delta.autoOptimize.autoCompact = false
delta.checkpointInterval = 100
```

### Cluster sizing
- Storage-optimized instances with local NVMe (AWS i3/i4i, Azure Lsv3)
- ~1 worker per 50-100M source rows → 20-30 workers for our 9.8B rewrite
- Photon enabled for SELECT phase

### Project narrowly
Don't `SELECT *` if downstream uses fewer columns. Less shuffle, less written, fewer columns for stats.

---

## 8. Dispute logic update (PENDING — apply this FIRST)

The base SKU query currently has dispute as a placeholder of 0. Update to use the proven logic from the existing `accounts_receivable_invoice_open` L2 table:

### Current placeholder in `fact_accounts_receivable_invoice_sku` (in the `ar_with_buckets` CTE):

```sql
-- Disputed: placeholder = 0; wire to dispute source when available.
cast(0 as decimal(36, 3))
    as receivable_disputed_actual_rate_functional_amount,
```

### Replace with this (the new `ar_with_buckets` CTE body):

```sql
ar_with_buckets as (
    select
        ar.*,

        -- OPEN: any uncleared AR amount
        case
            when ar.accounting_document_item_clearing_status_name = 'OPEN'
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as receivable_open_actual_rate_functional_amount,

        -- DISPUTED: OPEN + 2-char payment_reason_code (BSEG-RSTGR).
        -- Matches the formula used in finance_tbl.accounts_receivable_invoice_open.
        case
            when ar.accounting_document_item_clearing_date is null
             and length(ar.accounting_document_item_payment_reason_code) = 2
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as receivable_disputed_actual_rate_functional_amount,

        -- CURRENT_DUE: OPEN, NOT disputed, due_date >= as_of_date.
        case
            when ar.accounting_document_item_clearing_status_name = 'OPEN'
             and ar.accounting_document_item_due_date >= current_date()
             and not (length(ar.accounting_document_item_payment_reason_code) = 2)
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as receivable_current_due_actual_rate_functional_amount,

        -- PAST_DUE: OPEN, NOT disputed, due_date < as_of_date.
        case
            when ar.accounting_document_item_clearing_status_name = 'OPEN'
             and ar.accounting_document_item_due_date <  current_date()
             and not (length(ar.accounting_document_item_payment_reason_code) = 2)
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as receivable_past_due_actual_rate_functional_amount,

        -- CLEARED: any cleared amount
        case
            when ar.accounting_document_item_clearing_status_name = 'CLEARED'
                then ar.accounting_document_item_actual_rate_functional_amount
            else 0
        end as invoice_cleared_actual_rate_functional_amount
    from finance_tbl.fact_accounts_receivable_invoice ar
)
```

### Why this logic

Existing L2 `accounts_receivable_invoice_open` uses:
```sql
if(isnull(accounting_document_item_clearing_date)
   and length(accounting_document_item_payment_reason_code) = 2,
   accounting_document_item_actual_rate_functional_amount, 0)
   as receivable_disputed_actual_rate_functional_amount
```

The rule: item is disputed when it's still open AND payment_reason_code (SAP BSEG-RSTGR) is exactly 2 characters. The 2-char convention is the org's standard for dispute reason codes (e.g., 01=quality, 02=quantity, etc.). We inherit this proven logic exactly.

### Why current_due / past_due exclude disputed

Disjoint partition: `open = current_due + past_due + disputed`. A disputed item that's also past its due date is classified as disputed only, not past_due. This matches the SAP FSCM convention (disputed items leave the collections workflow). Disputed wins over the due-date classification.

### IMPORTANT: Verify against existing L2 before deploying

Before final deployment, check the existing `accounts_receivable_invoice_open` SQL to confirm whether it uses:
- **Option A (what we have above)**: disjoint partition, disputed items removed from past_due
- **Option B**: overlapping flag, disputed items still counted in past_due

If existing L2 uses Option B, remove `and not (length(payment_reason_code) = 2)` from current_due and past_due. The DQ check then changes from `open = current_due + past_due + disputed` to `open = current_due + past_due` (disputed becomes a separate flag column).

---

## 9. Files to be created (the 6 deliverables)

All files use the framework's model-per-file pattern: one SELECT statement plus a header comment block declaring materialization, unique_key, retention, etc.

### File index

| # | File | Materialization | Purpose |
|---|---|---|---|
| 0 | `00_README.md` | — | Framework conventions, header metadata reference, run order, prerequisites |
| 1 | `01_inc_fact_ar_sku_snapshot_archive.sql` | `incremental_merge` (UniForm) | Task 1: archive stale rows to Iceberg |
| 2 | `02_ctas_fact_ar_sku_snapshot_rebuild.sql` | `create_or_replace_table` | Task 2: rebuild L1 without stale rows |
| 3 | `03_inc_fact_ar_sku_snapshot_append_today.sql` | `incremental_merge` | Task 3: append today's slice (OPEN + cleared-today) |
| 4 | `04_ctas_fact_ar_sku_snapshot_open.sql` | `create_or_replace_table` | Task 4: rebuild open-only L2 |
| 5 | `05_dq_ar_sku_snapshot.sql` | `test` | DQ checks DQ-01 through DQ-09 |

### Plus (upstream change, owned by us)

| File | Purpose |
|---|---|
| `fact_accounts_receivable_invoice_sku.sql` (UPDATE) | Apply the dispute logic from Section 8 to the existing SKU base query |

---

## 10. Header metadata pattern (every SQL file)

```sql
-- name:             fact_accounts_receivable_invoice_sku_snapshot
-- materialization:  incremental_merge
-- target:           finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
-- unique_key:       [snapshot_date, snapshot_state, company_code,
--                    accounting_document_id,
--                    accounting_document_posting_fiscal_year_id,
--                    accounting_document_item_id,
--                    accounting_document_breakdown_item_id,
--                    billing_document_id, billing_document_item_id,
--                    material_number]
-- partition_by:     snapshot_date
-- cluster_by:       [snapshot_date, company_code]
-- retention_clause: snapshot_date >= date_sub(cast(:as_of_date as date), 60)
-- runs_on:          every_day
-- parameters:       :as_of_date
-- table_properties:
--   delta.dataSkippingNumIndexedCols: 8
--   delta.tuneFileSizesForRewrites: true
--   delta.autoOptimize.optimizeWrite: true
--   delta.autoOptimize.autoCompact: false

select /*+ BROADCAST(d), REPARTITION(400, snapshot_date) */
    ...
```

**PENDING CONFIRMATION**: The exact header key names depend on the framework's spec. The above is illustrative; verify against framework docs before deployment.

---

## 11. Data quality checks (DQ harness)

9 checks, each returns rows on failure, zero rows on healthy load. Persist results to `dq_runs` audit table.

| ID | Check | Severity |
|---|---|---|
| DQ-01 | L1 row count for :run_date = today's expected open count + cleared-today count | blocking |
| DQ-02 | L1 OPEN reconciliation: L1 sum of open amounts = upstream SKU sum of open amounts | blocking |
| DQ-03 | Bucket partition: open = current_due + past_due + disputed for every row | blocking |
| DQ-04 | Closed-once rule: every (AR PK + material) appears at most once with snapshot_state = 'CLEARED' | blocking |
| DQ-05 | No duplicate (snapshot_date, snapshot_state, AR PK, material) rows | blocking |
| DQ-06 | L2 OPEN row count = L1 row count with snapshot_state = 'OPEN' | blocking |
| DQ-07 | Retention compliance: no rows with snapshot_date older than 60 days | blocking |
| DQ-08 | Archive coverage: every row removed from L1 today exists in Iceberg archive | blocking |
| DQ-09 | Today's load is present in L1 | blocking |

**DQ-08 is critical under our framework constraint**: because Task 2 removes stale rows by absence (not by explicit DELETE), there's a theoretical risk of losing rows if Task 1's archive failed silently. DQ-08 guards against this.

---

## 12. PENDING items to resolve before/during coding

### Before coding starts

1. **Confirm SKU fan-out factor**
   - Run: `SELECT COUNT(*) FROM finance_tbl.fact_accounts_receivable_invoice_sku`
   - Compare to 364M AR rows to compute fan-out
   - Currently assuming 3x → ~1.1B SKU rows. If higher, recompute all volume estimates.

2. **Confirm dispute partition behavior matches existing L2**
   - Read `finance_tbl.accounts_receivable_invoice_open` SQL
   - Check how `past_due` is computed there
   - If Option A (disjoint): our SQL above is correct as-is
   - If Option B (overlapping): remove the `and not (length(...) = 2)` predicate from past_due and current_due

3. **Confirm framework header syntax**
   - Get framework documentation for exact key names: `materialization`, `unique_key`, `partition_by`, `cluster_by`, `retention_clause`, `runs_on`, `parameters`, `table_properties`, `table_format`
   - The patterns in Section 10 are illustrative; framework may use different names

4. **Confirm whether SET statements are allowed at top of SELECT**
   - Affects AQE coalesce disabling (Section 7)
   - If not, request platform team to set cluster-wide

5. **Iceberg archive target setup**
   - Confirm catalog and external storage location for `iceberg_cold.*` schema
   - Confirm Unity Catalog supports UniForm Delta with Iceberg metadata
   - Storage credential and external location must exist before first run

6. **Provision storage-optimized cluster**
   - AWS i3/i4i or Azure Lsv3 family
   - 20-30 workers
   - Photon enabled
   - DBR 14.3 LTS or above

### Discovered during coding

7. **Source column name verification**
   - The dispute logic references `accounting_document_item_payment_reason_code`
   - Confirm this column exists in `fact_accounts_receivable_invoice_sku` (not just in the AR fact)
   - If not in SKU projection: add it

8. **vbrp namespace**
   - SKU view references `direct_sales_raw.sapCL2028__vbrp`
   - But sergii (the AR fact source) references `direct_sales_conf.sapPR2028__vbrp`
   - Confirm which is authoritative; align both

---

## 13. Future considerations (NOT in this iteration)

- **Framework enhancement request**: A `retention_clause` metadata key that issues a DELETE after the merge. Single feature that eliminates Task 2 entirely, reducing daily pipeline from ~90 min to ~15 min. Use the cost numbers from this iteration as the business case.
- **Weekly/monthly cadences (FW, FME)**: Not in scope. If added later, same three-task pattern with different retention windows.
- **Native Iceberg migration**: When Unity Catalog managed Iceberg tables go GA, the archive can be migrated via single CTAS. UniForm continues to work meanwhile.
- **Backfill driver**: An orchestrator script that iterates `:as_of_date` over a date range to populate historical snapshots from day one.

---

## 14. Key design decisions log (for future readers)

| Decision | Reasoning |
|---|---|
| One snapshot table, not two | Single source of truth; halves framework tax |
| Source from SKU table, not AR fact | SKU is downstream; inheriting its pro-rata and bucket logic |
| L2 derived from L1, not from SKU directly | Cannot disagree with L1 by construction |
| Closed-once rule | 6x storage savings vs naive every-row-every-day |
| Liquid clustering, not partition + ZORDER | Single write pass; handles partition skew |
| UniForm Delta for archive | GA-stable; archive works with same incremental_merge pattern as hot tables |
| Path A (accept framework tax) | Framework changes are slower than just shipping; build cost numbers as evidence |
| Disjoint dispute partition (Option A) | Matches SAP FSCM convention; clean DQ arithmetic |

---

## 15. Quick reference: where to find things

- **Full technical design**: `AR_SKU_Snapshot_Framework.docx` (21 pages, in `/mnt/user-data/outputs/`)
- **Conversation transcript**: `/mnt/transcripts/2026-05-12-14-36-52-ar-sku-snapshot-framework.txt`
- **Previous SQL drafts**: in transcript, lines starting around 871 (SKU view), 1485 (DDL), 1591 (L1 loader), 1652 (L2 loader)
- **Sergii's full AR query**: in transcript, lines 420+ (in `sergii_ar_integrated.txt`)

---

## 16. Suggested first-message for the next session

> "Picking up from the AR SKU Snapshot Framework handoff. I've read the MD doc. Let's start by:
> 1. Updating the dispute logic in `fact_accounts_receivable_invoice_sku.sql` per section 8
> 2. Then writing the 6 deliverable files
>
> Before we write anything, I have these answers to the pending items:
> - SKU fan-out factor: [run the COUNT query, paste result]
> - Existing L2 partition behavior: [Option A or B]
> - Framework header syntax: [paste relevant framework docs or confirm illustrative pattern is fine]
> - SET statements allowed: [yes / no]
> - Iceberg archive location: [confirmed / setting up]
> - Cluster: [provisioned / pending]"

This gives the next session everything needed to start coding immediately.
