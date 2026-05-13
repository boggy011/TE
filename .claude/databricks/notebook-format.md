# Rule: Python files must be valid Databricks notebook source

**Scope:** Any `.py` file intended to run in Databricks (notebook, job task, DLT pipeline). The file must be importable into a Databricks workspace and recognized as a notebook — not as a plain script.

## Mandatory file structure

1. **First line must be exactly:**
   ```python
   # Databricks notebook source
   ```
   This single marker is what Databricks uses to detect a Python file as a notebook on import. Without it, the file imports as a plain `.py` file.

2. **Cell separator:**
   ```python
   # COMMAND ----------
   ```
   Place between every cell. Surround with one blank line above and below for readability.

3. **Cell title (optional but preferred for clarity):**
   ```python
   # DBTITLE 1,Load source data
   ```
   Goes at the top of a cell, immediately after `# COMMAND ----------`.

4. **Magic commands (non-Python cells):** prefix every line of the cell with `# MAGIC `.
   - Markdown:
     ```python
     # MAGIC %md
     # MAGIC ## Section heading
     # MAGIC Plain markdown lines, each prefixed with `# MAGIC `.
     ```
   - SQL:
     ```python
     # MAGIC %sql
     # MAGIC SELECT *
     # MAGIC FROM tedatacatalog_dev.admin.data_flow_control_header
     # MAGIC WHERE is_active = 'Y'
     ```
   - Other supported magics: `%scala`, `%r`, `%sh`, `%fs`, `%run`, `%pip`.

## Minimal valid template

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook title
# MAGIC Short description of what this notebook does.

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
# MAGIC SELECT count(*) FROM tedatacatalog_dev.admin.data_flow_control_header

# COMMAND ----------

# DBTITLE 1,Exit
dbutils.notebook.exit("OK")
```

## Hard rules

- **Never** omit the `# Databricks notebook source` first line. If a Python file in this repo is intended to be imported as a notebook and it doesn't start with that line, treat it as a bug.
- **Never** mix `# COMMAND ----------` with `# %%` (Jupyter / VS Code interactive) separators in the same file — Databricks only understands the former.
- Magic-command cells must use `# MAGIC ` on **every** line including blank lines (`# MAGIC` alone, no trailing content).
- Imports and module-level code go inside a cell (after the first `# COMMAND ----------`), not above it.
- Cell-title comments must use the exact form `# DBTITLE 1,<title>` (no space after the comma is fine; the `1` is required).
- Assume `spark`, `dbutils`, and `display` are pre-bound at runtime — do not import or define them.

## Import workflow (how Databricks recognizes the file)

1. Workspace → **Import** → upload the `.py` file or pull from Repos / Git folder.
2. With the first-line marker present, Databricks renders it as a notebook (cells, magic commands, titles all visible).
3. Without the marker, the file imports as a plain Python source under Workspace files — runnable via `%run` but not as a notebook UI.

## When this rule applies

- **Applies:** any `.py` file in this repo that will be deployed to Databricks (e.g., paths referenced in `DATA_FLOW_PB_DETAIL.GENERIC_SCRIPTS`, DLT pipeline notebooks, job task notebooks).
- **Does not apply:** local helper modules, test files, build scripts, or any `.py` file that is not meant to be a Databricks notebook. These remain plain Python.
- If in doubt, ask the user before writing — once a file is committed without the marker, fixing it requires re-import in Databricks.
