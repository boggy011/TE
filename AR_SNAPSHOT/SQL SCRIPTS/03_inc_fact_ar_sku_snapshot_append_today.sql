-- ============================================================================
-- Model:           03_inc_fact_ar_sku_snapshot_append_today
-- Task:            Task 3 of 4 — append today's slice (OPEN + cleared-today) to L1
-- ----------------------------------------------------------------------------
-- materialization: incremental_merge
-- target:          finance_tbl.fact_accounts_receivable_invoice_sku_snapshot
-- partition_by:    snapshot_date
-- cluster_by:      [snapshot_date, company_code]
-- unique_key:      [snapshot_date,
--                   snapshot_state,
--                   company_code,
--                   accounting_document_id,
--                   accounting_document_posting_fiscal_year_id,
--                   accounting_document_item_id,
--                   accounting_document_breakdown_item_id,
--                   billing_document_id,
--                   billing_document_item_id,
--                   material_number]
-- runs_on:         every_day
-- parameters:      :as_of_date  (DATE)
-- depends_on:      [finance_tbl.fact_accounts_receivable_invoice_sku]
-- description:     Closed-once rule: every OPEN row gets a snapshot every day until clearing;
--                  CLEARED rows get exactly one snapshot, on the day they cleared. AR aging
--                  buckets are re-derived against :as_of_date so the snapshot is correct
--                  as-of that date (not vs current_date()).
-- TODO[header-keys]: align key names with framework spec.
-- ============================================================================

with snapshot_today_rows as (
    -- Closed-once filter: keep all currently-OPEN rows + only the rows that cleared on :as_of_date.
    -- snapshot_state is derived from the AR clearing_status so it travels into the unique_key.
    -- Two boolean flags pre-compute the dispute / current-due classification once; the bucket
    -- case-when expressions in the final SELECT reuse them, which keeps the logic in one place.
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
        -- OPEN branch: every currently-open AR x material row
        sku.accounting_document_item_clearing_status_name = 'OPEN'
        or
        -- CLEARED branch: only items whose clearing date is :as_of_date (one-shot)
        (sku.accounting_document_item_clearing_status_name = 'CLEARED'
         and sku.accounting_document_item_clearing_date = cast(:as_of_date as date))
)

select /*+ REPARTITION(50) */
    -- ----------------------------------------------------------------
    -- Snapshot dimensions (added by this loader)
    -- ----------------------------------------------------------------
    snapshot_date,
    snapshot_state,

    -- ----------------------------------------------------------------
    -- AR identity / dimensions (pass-through from SKU)
    -- ----------------------------------------------------------------
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

    -- ----------------------------------------------------------------
    -- AR-grain aging buckets (RE-DERIVED against :as_of_date)
    -- Disjoint partition: receivable_open = current_due + past_due + disputed.
    -- past_due is computed as the complement of (disputed + current_due) so the partition
    -- holds by construction even for rows with NULL due_date.
    -- ----------------------------------------------------------------
    cast(case
        when snapshot_state = 'OPEN'
            then accounting_document_item_actual_rate_functional_amount
        else 0
    end as decimal(36, 3))                                         as receivable_open_actual_rate_functional_amount,

    cast(case
        when snapshot_state = 'OPEN' and is_disputed_flag
            then accounting_document_item_actual_rate_functional_amount
        else 0
    end as decimal(36, 3))                                         as receivable_disputed_actual_rate_functional_amount,

    cast(case
        when snapshot_state = 'OPEN' and is_current_due_flag and not is_disputed_flag
            then accounting_document_item_actual_rate_functional_amount
        else 0
    end as decimal(36, 3))                                         as receivable_current_due_actual_rate_functional_amount,

    cast((
        case when snapshot_state = 'OPEN'
             then accounting_document_item_actual_rate_functional_amount else 0 end
        - case when snapshot_state = 'OPEN' and is_disputed_flag
               then accounting_document_item_actual_rate_functional_amount else 0 end
        - case when snapshot_state = 'OPEN' and is_current_due_flag and not is_disputed_flag
               then accounting_document_item_actual_rate_functional_amount else 0 end
    ) as decimal(36, 3))                                           as receivable_past_due_actual_rate_functional_amount,

    cast(case
        when snapshot_state = 'CLEARED'
            then accounting_document_item_actual_rate_functional_amount
        else 0
    end as decimal(36, 3))                                         as invoice_cleared_actual_rate_functional_amount,

    -- ----------------------------------------------------------------
    -- Material grain identity (pass-through)
    -- ----------------------------------------------------------------
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

    -- Pro-rata audit (pass-through)
    material_prorate_factor,
    billing_doc_total_net_value,
    billing_doc_line_item_count,

    -- Pro-rated full amounts (pass-through; bucket-independent)
    material_document_currency_amount,
    material_actual_rate_functional_amount,

    -- ----------------------------------------------------------------
    -- Material-grain aging buckets (RE-DERIVED against :as_of_date)
    -- Same logic as AR-grain buckets but applied to material_actual_rate_functional_amount,
    -- which is already pro-rated. Equivalent to (AR bucket) × material_prorate_factor.
    -- ----------------------------------------------------------------
    cast(case
        when snapshot_state = 'OPEN'
            then material_actual_rate_functional_amount
        else 0
    end as decimal(36, 3))                                         as material_open_functional_amount,

    cast(case
        when snapshot_state = 'OPEN' and is_disputed_flag
            then material_actual_rate_functional_amount
        else 0
    end as decimal(36, 3))                                         as material_disputed_functional_amount,

    cast(case
        when snapshot_state = 'OPEN' and is_current_due_flag and not is_disputed_flag
            then material_actual_rate_functional_amount
        else 0
    end as decimal(36, 3))                                         as material_current_due_functional_amount,

    cast((
        case when snapshot_state = 'OPEN'
             then material_actual_rate_functional_amount else 0 end
        - case when snapshot_state = 'OPEN' and is_disputed_flag
               then material_actual_rate_functional_amount else 0 end
        - case when snapshot_state = 'OPEN' and is_current_due_flag and not is_disputed_flag
               then material_actual_rate_functional_amount else 0 end
    ) as decimal(36, 3))                                           as material_past_due_functional_amount,

    cast(case
        when snapshot_state = 'CLEARED'
            then material_actual_rate_functional_amount
        else 0
    end as decimal(36, 3))                                         as material_cleared_functional_amount

from snapshot_today_rows
