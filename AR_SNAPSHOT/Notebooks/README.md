# AR SKU Snapshot — Notebooks

Databricks notebooks for the AR SKU snapshot pipeline. SQL source-of-truth lives in
`../SQL SCRIPTS/`; these notebooks are framework-ready (notebook-source format) for
import into Databricks.

## File map

| Notebook | Purpose | Framework? |
|----------|---------|------------|
| `00_register_pipeline.sql` | HEADER + 4 DETAIL INSERTs + security lookup INSERTs for the whole pipeline | yes — run once to register |
| `01_archive_dev.sql` | Task 1 dev: archive stale rows (DELTA merge → Iceberg) | mirrors PRIORITY 1 DETAIL |
| `02_rebuild_dev.sql` | Task 2 dev: rebuild L1 within 60-day window (FULL CTAS) | mirrors PRIORITY 2 DETAIL |
| `03_append_today_dev.sql` | Task 3 dev: append today's slice (DELTA merge, closed-once) | mirrors PRIORITY 3 DETAIL |
| `04_open_l2_dev.sql` | Task 4 dev: rebuild L2 open-only (FULL CTAS) | mirrors PRIORITY 4 DETAIL |
| `05_dq_dev.sql` | Task 5: 9 DQ checks (UNION ALL) | NOT a framework DETAIL — see note below |

## DFG and run order

Single DFG: **`FINANCE_FIN360_AR_SKU_SNAPSHOT_L1`**

```
PRIORITY 1  Task 1  archive stale → iceberg_cold.<...>_snapshot_archive   (DELTA merge)
                  │
                  ▼  must complete before Task 2
PRIORITY 2  Task 2  rebuild L1     → finance_tbl.<...>_snapshot            (FULL CTAS)
                  │
                  ▼  must complete before Task 3 (else today's rows get wiped)
PRIORITY 3  Task 3  append today   → finance_tbl.<...>_snapshot            (DELTA merge)
                  │
                  ▼
PRIORITY 4  Task 4  open L2        → finance_tbl.<...>_snapshot_open       (FULL CTAS)
                  │
                  ▼
TASK 5     DQ tests (separate workflow task — not a framework DETAIL)
```

Tasks 2 and 3 share the same `TARGET_OBJ_NAME`. Verify the framework allows two
DETAIL rows with the same target within one DFG before deploying; if not, split into
two DFG_IDs.

## Why DQ (Task 5) is not a framework DETAIL

Materialization = `test` (per the 00_README in `../SQL SCRIPTS/`). A `test` is a
SELECT that must return zero rows for a healthy load — it doesn't write a target.
The framework's `LOAD_TYPE` values (`FULL` / `DELTA` / `SCD`) all describe writes.

Two deployment options for DQ:

- **A (recommended):** Run `05_dq_dev.sql` as a separate workflow task after the
  framework job. Fail the workflow if the notebook's last cell returns a non-empty
  result set.
- **B:** Wrap the DQ SELECT in a CTAS to a `dq_runs` audit table → fits
  `LOAD_TYPE = FULL`, gives history. The 00_README explicitly notes there is no
  `dq_runs` table in this iteration; adding one is a small extension.

## Pre-flight before running `00_register_pipeline.sql`

1. `:as_of_date` binding — confirm the framework substitutes this placeholder
   at runtime in `TRANSFORM_QUERY`. If not, queries must be rewritten.
2. `iceberg_cold` catalog/schema/external location exists.
3. Framework supports two DETAIL rows with the same target (Tasks 2 and 3) within
   one DFG.
4. `uniform_iceberg` table format support for Task 1's archive target.
5. Replace `DATA_SME` / `PRODUCT_OWNER` placeholders.
6. `COMPUTE_CLASS` sized for the 9.8B-row Task 2 rewrite.
7. Cluster Spark config: `spark.sql.adaptive.coalescePartitions.enabled = false`.

## References

- `../SQL SCRIPTS/00_README.md` — framework conventions, header metadata, retention, DQ map
- `../SQL SCRIPTS/AR_SKU_Snapshot_Framework_Handoff (1).md` — full design rationale
- `.claude/databricks/notebook-format.md` — Databricks notebook source format rule
- `.claude/databricks/example-notebook.sql` — canonical OneData framework example
- `.claude/framework/onedata-runbook-gen2.md` — runbook for framework metadata tables
