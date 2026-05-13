-- Databricks notebook source
-- DBTITLE 1,Task 3 dev — Append today's slice (DELTA merge, closed-once rule)
-- ============================================================================
-- Mirrors framework DETAIL row at PRIORITY=3 in 00_register_pipeline.sql.
-- Reads:   finance_tbl.fact_accounts_receivable_invoice_sku
-- Writes:  finance_tbl.fact_accounts_receivable_invoice_sku_snapshot  (same table as Task 2)
-- Volume:  ~165M rows/day
-- Closed-once rule:
--   OPEN  rows are snapshotted every fiscal day until they clear
--   CLEARED rows are snapshotted exactly once, on the day they cleared
-- Unique key (10 cols) includes snapshot_state so re-runs for the same :as_of_date are idempotent.
-- MUST run after Task 2 — otherwise Task 2's rebuild wipes today's rows.
-- ============================================================================

-- COMMAND ----------

-- DBTITLE 1,Parameter
create widget text as_of_date default '2026-05-14';

-- COMMAND ----------

-- DBTITLE 1,Preview today's slice
create or replace temp view task3_append_preview as
with snapshot_today_rows as (
    select
        cast(:as_of_date as date)                                  as snapshot_date,
        sku.accounting_document_item_clearing_status_name          as snapshot_state,
        sku.*,
        case
            when sku.accounting_document_item_clearing_date is null
             and length(sku.accounting_document_item_payment_reason_code) = 2
                then true
            else false
        end                                                        as is_disputed_flag,
        case
            when sku.accounting_document_item_clearing_status_name = 'OPEN'
             and sku.accounting_document_item_due_date >= cast(:as_of_date as date)
                then true
            else false
        end                                                        as is_current_due_flag
    from finance_tbl.fact_accounts_receivable_invoice_sku sku
    where
        sku.accounting_document_item_clearing_status_name = 'OPEN'
        or
        (sku.accounting_document_item_clearing_status_name = 'CLEARED'
         and sku.accounting_document_item_clearing_date = cast(:as_of_date as date))
)
select /*+ REPARTITION(50) */
    snapshot_date,
    snapshot_state,
    company_code,
    customer_account_number,
    accounting_document_id,
    accounting_document_posting_fiscal_year_id,
    accounting_document_item_id,
    accounting_document_type_code,
    accounting_document_create_date,
    accounting_document_posting_date,
    accounting_document_item_clearing_document_id,
    accounting_document_item_clearing_date,
    accounting_document_item_credit_or_debit_code,
    accounting_document_item_posting_key_used_in_payment_indicator,
    accounting_document_item_invoice_document_id,
    accounting_document_item_text,
    accounting_document_item_baseline_date,
    accounting_document_item_due_date,
    accounting_document_item_payment_terms_code,
    accounting_document_item_cash_discount_days_quantity_1,
    accounting_document_item_cash_discount_days_quantity_2,
    accounting_document_create_network_user_id,
    accounting_document_item_document_currency_code,
    accounting_document_item_functional_currency_code,
    accounting_document_item_payment_reason_code,
    accounting_document_item_clearing_status_name,
    ted_overriding_profit_center_id,
    profit_center_id,
    ted_profit_center_id,
    ted_profit_center_effective_date,
    accounting_document_breakdown_item_id,
    accounting_document_item_document_currency_amount,
    accouting_document_item_general_ledger_reconciliation_account_number,
    accounting_document_item_functional_currency_amount,
    accounting_document_item_actual_rate_functional_amount,
    accounting_document_item_tariff_percentage,
    data_security_tag_id,
    business_unit_group_id,
    accounting_document_item_budget_rate_amount,
    sold_to_customer_number,
    sales_organization_id,
    distribution_channel_code,
    worldwide_customer_level_1_number,
    worldwide_customer_level_1_name,
    worldwide_customer_level_2_number,
    worldwide_customer_level_2_name,
    country_region_level_1_name,
    country_region_level_2_name,
    gam_level_3_assoc_full_name,
    account_manager_level_6_assoc_full_name,
    account_manager_level_7_assoc_full_name,
    sales_territory_level_6_assoc_full_name,
    sales_territory_level_7_assoc_full_name,
    sales_office_code,
    sales_office_name,
    account_manager_assoc_full_name,
    sales_territory_assoc_full_name,
    sensors_alternate_sales_territory_code,
    sensors_alternate_sales_territory_name,
    cast(case
        when snapshot_state = 'OPEN'
            then accounting_document_item_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as receivable_open_actual_rate_functional_amount,
    cast(case
        when snapshot_state = 'OPEN' and is_disputed_flag
            then accounting_document_item_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as receivable_disputed_actual_rate_functional_amount,
    cast(case
        when snapshot_state = 'OPEN' and is_current_due_flag and not is_disputed_flag
            then accounting_document_item_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as receivable_current_due_actual_rate_functional_amount,
    cast((
        case when snapshot_state = 'OPEN'
             then accounting_document_item_actual_rate_functional_amount else 0 end
        - case when snapshot_state = 'OPEN' and is_disputed_flag
               then accounting_document_item_actual_rate_functional_amount else 0 end
        - case when snapshot_state = 'OPEN' and is_current_due_flag and not is_disputed_flag
               then accounting_document_item_actual_rate_functional_amount else 0 end
    ) as decimal(36, 3)) as receivable_past_due_actual_rate_functional_amount,
    cast(case
        when snapshot_state = 'CLEARED'
            then accounting_document_item_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as invoice_cleared_actual_rate_functional_amount,
    billing_document_id,
    billing_document_item_id,
    material_number,
    raw_material_number,
    material_description,
    material_quantity,
    material_unit_of_measure_code,
    material_billing_line_net_value,
    material_billing_line_gross_value,
    product_hierarchy_l3_code,
    product_hierarchy_l3_name,
    product_hierarchy_l4_code,
    product_hierarchy_l4_name,
    material_cbc_code,
    material_cbc_name,
    material_prorate_factor,
    billing_doc_total_net_value,
    billing_doc_line_item_count,
    material_document_currency_amount,
    material_actual_rate_functional_amount,
    cast(case
        when snapshot_state = 'OPEN'
            then material_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as material_open_functional_amount,
    cast(case
        when snapshot_state = 'OPEN' and is_disputed_flag
            then material_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as material_disputed_functional_amount,
    cast(case
        when snapshot_state = 'OPEN' and is_current_due_flag and not is_disputed_flag
            then material_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as material_current_due_functional_amount,
    cast((
        case when snapshot_state = 'OPEN'
             then material_actual_rate_functional_amount else 0 end
        - case when snapshot_state = 'OPEN' and is_disputed_flag
               then material_actual_rate_functional_amount else 0 end
        - case when snapshot_state = 'OPEN' and is_current_due_flag and not is_disputed_flag
               then material_actual_rate_functional_amount else 0 end
    ) as decimal(36, 3)) as material_past_due_functional_amount,
    cast(case
        when snapshot_state = 'CLEARED'
            then material_actual_rate_functional_amount
        else 0
    end as decimal(36, 3)) as material_cleared_functional_amount
from snapshot_today_rows;

select snapshot_state, count(*) as row_count
from task3_append_preview
group by snapshot_state;

-- COMMAND ----------

-- DBTITLE 1,Bucket arithmetic check (per row)
-- Disjoint partition: material_open = current_due + past_due + disputed
-- This is the same check that DQ-03 does after the load.
select count(*) as bucket_violations
from task3_append_preview
where snapshot_state = 'OPEN'
  and abs(material_open_functional_amount
          - (material_current_due_functional_amount
             + material_past_due_functional_amount
             + material_disputed_functional_amount)) > 0.01;
