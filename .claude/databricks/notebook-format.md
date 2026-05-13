# Rule: Databricks notebook source format

**Scope:** Any source file in this repo intended to be imported into Databricks as a **notebook** — typically `.py` (Python), `.sql` (SQL), `.scala`, or `.r`. The file must be recognized as a notebook on import, not as a plain script.

**Canonical example:** [`example-notebook.sql`](example-notebook.sql) — real working SQL notebook from the OneData framework. Use it as the structural reference for new notebooks.

## Mandatory first line

The very first line tells Databricks this is a notebook. The comment style must match the file's primary language:

| File type | First line |
|-----------|------------|
| `.py` (Python) | `# Databricks notebook source` |
| `.sql` (SQL)   | `-- Databricks notebook source` |
| `.scala`       | `// Databricks notebook source` |
| `.r`           | `# Databricks notebook source` |

Without this marker, Databricks imports the file as a plain Workspace file. **Never omit it.**

## Cell separator

Between every cell, on its own line, using the same comment style as the file:

| File type | Separator |
|-----------|-----------|
| `.py`     | `# COMMAND ----------` |
| `.sql`    | `-- COMMAND ----------` |
| `.scala`  | `// COMMAND ----------` |

Surround with one blank line above and below.

## Cell title (preferred)

Optional but strongly preferred — gives every cell a visible heading in the UI:

| File type | Title syntax |
|-----------|--------------|
| `.py`     | `# DBTITLE 1,<title>` |
| `.sql`    | `-- DBTITLE 1,<title>` |
| `.scala`  | `// DBTITLE 1,<title>` |

Placed immediately after `# COMMAND ----------`. The `1` is required (cell-title version). Title text is everything after the comma.

## Magic commands (other-language cells)

Every line of a magic cell must be prefixed with the file's `MAGIC` marker:

| File type | Magic prefix |
|-----------|--------------|
| `.py`     | `# MAGIC `   |
| `.sql`    | `-- MAGIC `  |
| `.scala`  | `// MAGIC `  |

Supported magics: `%md`, `%sql`, `%python`, `%scala`, `%r`, `%sh`, `%fs`, `%run`, `%pip`.

**SQL notebook example — Markdown cell:**
```sql
-- MAGIC %md
-- MAGIC ## Section heading
-- MAGIC Plain markdown lines, each prefixed.
```

**Python notebook example — SQL cell:**
```python
# MAGIC %sql
# MAGIC SELECT count(*) FROM admin.data_flow_control_header
```

## Minimal SQL notebook template

```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Notebook title
-- MAGIC Short description.

-- COMMAND ----------

-- DBTITLE 1,Inspect header
select *
from admin.data_flow_control_header
where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1';

-- COMMAND ----------

-- DBTITLE 1,Inspect detail
select *
from admin.data_flow_pb_detail
where DATA_FLOW_GROUP_ID = 'FINANCE_FIN360_ACCOUNTS_RECEIVABLE_SNAPSHOT_L1';
```

## Minimal Python notebook template

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook title

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("environment", "DEV")
env: str = dbutils.widgets.get("environment")

# COMMAND ----------

# DBTITLE 1,Load
df = spark.read.table(f"tedatacatalog_{env.lower()}.admin.data_flow_control_header")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM admin.data_flow_control_header

# COMMAND ----------

dbutils.notebook.exit("OK")
```

## Hard rules

- **Never** omit the first-line marker — that single line is what makes the file a notebook on import.
- **Never** mix Jupyter `# %%` separators with Databricks `-- COMMAND ----------` / `# COMMAND ----------` in the same file.
- Comment style of all framework markers (`Databricks notebook source`, `COMMAND ----------`, `DBTITLE`, `MAGIC`) **must match the file's primary language**. A `.sql` file with `# COMMAND ----------` will not be recognized.
- Magic-command cells must use the `MAGIC ` prefix on **every** line, including blank lines (just `-- MAGIC` or `# MAGIC` alone).
- In Python notebooks: assume `spark`, `dbutils`, and `display` are pre-bound at runtime — never import or define them.

## When this rule applies

- **Applies:** any file in this repo that will be deployed to Databricks (e.g., paths referenced in `DATA_FLOW_PB_DETAIL.GENERIC_SCRIPTS`, DLT pipeline notebooks, job task notebooks, exploratory notebooks committed to the repo).
- **Does not apply:** plain helper modules, tests, build scripts, or any source file that is not meant to be imported as a Databricks notebook. Those remain regular Python / SQL files without the markers.

If unsure whether a file should be a notebook, ask before writing — once it's committed without the marker, fixing it requires re-import in Databricks.
